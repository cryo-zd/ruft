//! In-memory logical log operations across a compacted snapshot boundary.
//!
//! [`RaftLog`] provides a unified view of the Raft log that spans the
//! compacted snapshot boundary and the in-memory suffix. Callers query terms
//! and entries without needing to know whether the target index falls in the
//! snapshot or the suffix.
//!
//! The log enforces Raft structural invariants: entries must form a continuous
//! sequence with non-decreasing terms, and committed entries may never be
//! overwritten or compacted away.

mod entry;
mod unstable;

use std::ops::RangeInclusive;

pub use entry::{Entry, EntryPayload};

use crate::{InvariantViolation, LogError, LogIndex, RecoveredState, SnapshotRecord, Term};
use unstable::Unstable;

/// A validated log suffix together with its compacted snapshot boundary.
///
/// The log is indexed from 1. Index 0 is a virtual origin with term 0.
/// The snapshot boundary (if present) represents the prefix `[1,
/// snapshot_index]` that has been compacted. The in-memory `entries` vector
/// holds the suffix `[first_index, last_index]`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftLog<C> {
    /// The compacted prefix, if any. Its `last_included_index` is the highest
    /// index covered by the snapshot.
    snapshot: Option<SnapshotRecord>,
    /// The in-memory suffix, starting immediately after the snapshot boundary
    /// (or at index 1 when no snapshot exists).
    entries: Vec<Entry<C>>,
    /// The highest index known to be committed.
    committed: LogIndex,
    /// Tracks the continuous range of entries that have been appended but not
    /// yet confirmed durable.
    unstable: Unstable,
}

impl<C> RaftLog<C> {
    /// Reconstructs a logical log from already validated durable state.
    pub fn from_recovered(recovered: &RecoveredState<C>) -> Self {
        Self {
            snapshot: recovered.snapshot().cloned(),
            entries: recovered.entries().to_vec(),
            committed: recovered.hard_state().commit_index(),
            unstable: Unstable::default(),
        }
    }

    /// Returns the first index that remains available as an entry.
    ///
    /// This is `snapshot_index + 1` when a snapshot exists, or 1 otherwise.
    /// Entries at or below `first_index - 1` have been compacted.
    pub fn first_index(&self) -> LogIndex {
        self.snapshot.as_ref().map_or(LogIndex::new(1), |record| {
            record
                .metadata()
                .index()
                .checked_next()
                .expect("snapshot index was validated during recovery")
        })
    }

    /// Returns the highest index available in the snapshot or suffix.
    ///
    /// When both the snapshot and suffix are empty (fresh log), returns 0.
    pub fn last_index(&self) -> LogIndex {
        self.entries.last().map_or_else(
            || {
                self.snapshot
                    .as_ref()
                    .map_or(LogIndex::new(0), |record| record.metadata().index())
            },
            Entry::index,
        )
    }

    /// Returns the highest index known to be committed.
    pub const fn committed_index(&self) -> LogIndex {
        self.committed
    }

    /// Advances the commit point without allowing it to exceed the local log.
    pub fn commit_to(&mut self, index: LogIndex) -> Result<(), LogError> {
        if index > self.last_index() {
            return Err(LogError::Unavailable {
                index,
                last: self.last_index(),
            });
        }
        if index > self.committed {
            self.committed = index;
        }
        Ok(())
    }

    /// Returns the last local index stored with `term`.
    ///
    /// Searches the suffix in reverse, then falls back to the snapshot
    /// boundary. Used by the leader to skip an entire conflicting term after
    /// a follower rejection — if the follower has term T at the conflict
    /// point and the leader also has entries from term T, the leader can
    /// resume right after the last entry of that term.
    pub fn last_index_of_term(&self, term: Term) -> Option<LogIndex> {
        self.entries
            .iter()
            .rev()
            .find(|entry| entry.term() == term)
            .map(Entry::index)
            .or_else(|| {
                self.snapshot.as_ref().and_then(|snapshot| {
                    (snapshot.metadata().term() == term).then(|| snapshot.metadata().index())
                })
            })
    }

    /// Returns the first local index stored with `term`.
    ///
    /// Searches the snapshot boundary first, then the suffix. Used to build
    /// a [`ConflictHint`](crate::ConflictHint) — the follower tells the leader
    /// where its conflicting term starts, letting the leader skip the whole
    /// term range.
    pub fn first_index_of_term(&self, term: Term) -> Option<LogIndex> {
        if self
            .snapshot
            .as_ref()
            .is_some_and(|snapshot| snapshot.metadata().term() == term)
        {
            return self
                .snapshot
                .as_ref()
                .map(|snapshot| snapshot.metadata().index());
        }
        self.entries
            .iter()
            .find(|entry| entry.term() == term)
            .map(Entry::index)
    }

    /// Merges a leader suffix and returns only the new entries that require
    /// durability.
    ///
    /// Uses [`Iterator::position`] to find the first index where the incoming
    /// entries differ from the local log (by term mismatch or because the
    /// index doesn't exist locally). The matching prefix is skipped — only
    /// the conflicting suffix is persisted.
    pub fn merge_from_leader(&mut self, incoming: &[Entry<C>]) -> Result<Vec<Entry<C>>, LogError> {
        // Find the first entry in the incoming batch whose term disagrees
        // with our local log (or that is beyond our log).
        let Some(first_change) = incoming.iter().position(|entry| {
            self.term(entry.index())
                .map_or(true, |term| term != entry.term())
        }) else {
            // All incoming entries match our log at the same terms.
            // Nothing new to persist.
            return Ok(Vec::new());
        };
        let new_entries = incoming[first_change..].to_vec();
        self.replace_conflict(new_entries.clone())?;
        Ok(new_entries)
    }

    /// Returns the first entry still waiting for persistence confirmation.
    pub const fn unstable_from(&self) -> Option<LogIndex> {
        self.unstable.from()
    }

    /// Looks up a term at an available index or at the snapshot boundary.
    ///
    /// Index 0 is the virtual origin and always has term 0 (unless a snapshot
    /// exists — then index 0 is invalid because the snapshot starts at ≥ 1).
    pub fn term(&self, index: LogIndex) -> Result<Term, LogError> {
        // The virtual origin at index 0 always has term 0.
        if index == LogIndex::new(0) && self.snapshot.is_none() {
            return Ok(Term::new(0));
        }

        if let Some(snapshot) = &self.snapshot {
            let snapshot_index = snapshot.metadata().index();
            if index < snapshot_index {
                return Err(LogError::Compacted {
                    index,
                    snapshot_index,
                });
            }
            if index == snapshot_index {
                return Ok(snapshot.metadata().term());
            }
        }

        self.entry_at(index)
            .map(Entry::term)
            .ok_or(LogError::Unavailable {
                index,
                last: self.last_index(),
            })
    }

    /// Returns a contiguous inclusive range of available suffix entries.
    pub fn entries(&self, range: RangeInclusive<LogIndex>) -> Result<&[Entry<C>], LogError> {
        let start = *range.start();
        let end = *range.end();
        if start > end {
            return Err(LogError::InvalidRange { start, end });
        }

        let first = self.first_index();
        if start < first {
            return Err(LogError::Compacted {
                index: start,
                snapshot_index: self
                    .snapshot
                    .as_ref()
                    .map_or(LogIndex::new(0), |record| record.metadata().index()),
            });
        }
        if end > self.last_index() {
            return Err(LogError::Unavailable {
                index: end,
                last: self.last_index(),
            });
        }

        let start_offset = usize::try_from(start.get() - first.get())
            .map_err(|_| LogError::IndexTooLarge { index: start })?;
        let end_offset = usize::try_from(end.get() - first.get())
            .map_err(|_| LogError::IndexTooLarge { index: end })?;
        Ok(&self.entries[start_offset..=end_offset])
    }

    /// Appends a new continuous suffix after the current last index.
    ///
    /// Requires that the first new entry has index `last_index + 1`. Used by
    /// the leader when appending new proposals — the leader's log is always
    /// contiguous.
    pub fn append(&mut self, entries: Vec<Entry<C>>) -> Result<(), LogError> {
        if entries.is_empty() {
            return Ok(());
        }

        self.validate_continuity(
            &entries,
            self.last_index()
                .checked_next()
                .map_err(|_| LogError::IndexOverflow {
                    at: self.last_index(),
                })?,
        )?;
        let first_new = entries[0].index();
        let last_new = entries.last().expect("entries are nonempty").index();
        self.entries.extend(entries);
        self.unstable.mark_range(first_new, last_new);
        Ok(())
    }

    /// Replaces the first conflicting uncommitted suffix, if one exists.
    ///
    /// Unlike [`RaftLog::append`], this method allows the incoming entries to start at
    /// or before `last_index` — it finds the first conflict point, truncates
    /// from there, and extends with the incoming suffix. This is the follower
    /// side of log replication.
    ///
    /// # Safety
    ///
    /// The method refuses to overwrite committed entries. If the conflict
    /// point falls at or below the commit index, it returns
    /// [`LogError::WouldTruncateCommitted`]. This protects against a leader
    /// that tries to replace already-committed data.
    pub fn replace_conflict(&mut self, incoming: Vec<Entry<C>>) -> Result<(), LogError> {
        if incoming.is_empty() {
            return Ok(());
        }
        let expected_after_last =
            self.last_index()
                .checked_next()
                .map_err(|_| LogError::IndexOverflow {
                    at: self.last_index(),
                })?;
        // The first incoming entry must not create a gap — it must be at or
        // before the next expected index.
        if incoming[0].index() > expected_after_last {
            return Err(LogError::NonContiguousEntries {
                expected: expected_after_last,
                actual: incoming[0].index(),
            });
        }
        self.validate_continuity(&incoming, incoming[0].index())?;

        // Walk through the incoming entries, comparing term by term with the
        // local log. The first deviation pinpoints where the leader's suffix
        // must replace the local suffix.
        let mut replace_at = None;
        for (offset, entry) in incoming.iter().enumerate() {
            let index = entry.index();
            if let Some(snapshot) = &self.snapshot {
                let snapshot_index = snapshot.metadata().index();
                if index < snapshot_index {
                    return Err(LogError::Compacted {
                        index,
                        snapshot_index,
                    });
                }
                // An entry at the snapshot boundary must match the snapshot's
                // term. If it doesn't, the leader and follower disagree about
                // the snapshot itself — a serious inconsistency.
                if index == snapshot_index {
                    if entry.term() != snapshot.metadata().term() {
                        return Err(LogError::SnapshotBoundaryMismatch {
                            index,
                            expected: snapshot.metadata().term(),
                            actual: entry.term(),
                        });
                    }
                    continue;
                }
            }

            match self.entry_at(index) {
                // Term matches — this prefix is consistent across both logs.
                Some(existing) if existing.term() == entry.term() => continue,
                // Term conflict at an existing entry. Truncate here and
                // replace with the leader's suffix.
                Some(_) => {
                    replace_at = Some(offset);
                    break;
                }
                // Entry beyond our log. Must be the exact next index —
                // continuity is enforced above.
                None => {
                    let expected =
                        self.last_index()
                            .checked_next()
                            .map_err(|_| LogError::IndexOverflow {
                                at: self.last_index(),
                            })?;
                    if index != expected {
                        return Err(LogError::NonContiguousEntries {
                            expected,
                            actual: index,
                        });
                    }
                    replace_at = Some(offset);
                    break;
                }
            }
        }

        // No conflict found — all entries match (or are already covered by
        // the snapshot). Nothing to replace.
        let Some(offset) = replace_at else {
            return Ok(());
        };
        let from = incoming[offset].index();
        // Safety guard: never overwrite committed entries. The leader must
        // not propose a suffix that would replace committed data.
        if from <= self.committed {
            return Err(LogError::WouldTruncateCommitted {
                from,
                committed: self.committed,
            });
        }

        // Truncate at the conflict point and extend with the leader's suffix.
        let first = self.first_index();
        let truncate_offset = usize::try_from(from.get() - first.get())
            .map_err(|_| LogError::IndexTooLarge { index: from })?;
        let last_new = incoming
            .last()
            .expect("incoming entries are nonempty")
            .index();
        self.entries.truncate(truncate_offset);
        self.unstable.truncate_from(from);
        self.entries.extend(incoming.into_iter().skip(offset));
        self.unstable.mark_range(from, last_new);
        Ok(())
    }

    /// Marks all unstable entries through `index` as durable.
    pub fn mark_stable(&mut self, index: LogIndex) -> Result<(), LogError> {
        if index > self.last_index() {
            return Err(LogError::StablePastLog {
                stable: index,
                last: self.last_index(),
            });
        }
        self.unstable.mark_stable_through(index);
        Ok(())
    }

    /// Returns the installed snapshot record, if any.
    pub const fn snapshot(&self) -> Option<&SnapshotRecord> {
        self.snapshot.as_ref()
    }

    /// Validates the continuity and term ordering of the current logical suffix.
    pub(crate) fn validate(&self) -> Result<(), InvariantViolation> {
        self.validate_continuity(&self.entries, self.first_index())
            .map_err(InvariantViolation::Log)
    }

    /// Installs a received snapshot and retains the suffix only when its
    /// boundary matches.
    ///
    /// If the local log has the same term at the snapshot boundary, entries
    /// after the boundary are consistent and can be kept. Otherwise the
    /// entire suffix is discarded — the snapshot represents a divergent
    /// history.
    ///
    /// This is the follower side of snapshot installation (receiving a
    /// snapshot from the leader). Compare with [`RaftLog::compact`], which is
    /// the local side (compacting the log after building a local snapshot).
    pub fn install_snapshot(&mut self, snapshot: SnapshotRecord) -> Result<(), LogError> {
        let through = snapshot.metadata().index();
        // If our term at the snapshot boundary matches, the suffix entries
        // beyond the boundary are still valid and can be retained.
        let retains_suffix = self
            .term(through)
            .is_ok_and(|term| term == snapshot.metadata().term());
        if retains_suffix {
            // Drain entries that precede or equal the snapshot boundary.
            let first = self.first_index();
            let drain =
                usize::try_from(through.get().saturating_add(1).saturating_sub(first.get()))
                    .map_err(|_| LogError::IndexTooLarge { index: through })?;
            self.entries.drain(..drain.min(self.entries.len()));
        } else {
            // Term mismatch at the boundary — the suffix is inconsistent
            // with the snapshot. Discard it entirely.
            self.entries.clear();
        }
        // Unstable tracking below the snapshot boundary is no longer relevant.
        self.unstable.discard_through(through);
        self.snapshot = Some(snapshot);
        // The snapshot implies its boundary index is committed, at minimum.
        if through > self.committed {
            self.committed = through;
        }
        Ok(())
    }

    /// Installs a snapshot boundary and discards only the prefix it represents.
    ///
    /// Unlike [`RaftLog::install_snapshot`], this requires that `through <= committed`
    /// (we can only compact entries that are committed) and that the term at
    /// the boundary matches (the snapshot was built from this log).
    ///
    /// This is the local side of compaction. Compare with
    /// [`RaftLog::install_snapshot`], which is the follower side.
    pub fn compact(&mut self, snapshot: SnapshotRecord) -> Result<(), LogError> {
        let through = snapshot.metadata().index();
        // Guard: cannot compact uncommitted entries. The snapshot must not
        // extend past the commit index.
        if through > self.committed {
            return Err(LogError::CompactPastCommit {
                through,
                committed: self.committed,
            });
        }
        // Guard: the snapshot boundary term must match the log. This ensures
        // the snapshot was built from this log and not from a divergent one.
        if self.term(through)? != snapshot.metadata().term() {
            return Err(LogError::SnapshotBoundaryMismatch {
                index: through,
                expected: self.term(through)?,
                actual: snapshot.metadata().term(),
            });
        }

        // Remove the log prefix that is now covered by the snapshot,
        // retaining entries above the boundary.
        let retained_from = through
            .checked_next()
            .map_err(|_| LogError::IndexOverflow { at: through })?;
        if retained_from > self.first_index() {
            let drain_to = usize::try_from(retained_from.get() - self.first_index().get())
                .map_err(|_| LogError::IndexTooLarge {
                    index: retained_from,
                })?;
            self.entries.drain(..drain_to.min(self.entries.len()));
        }
        self.snapshot = Some(snapshot);
        self.unstable.discard_through(through);
        Ok(())
    }

    /// Returns the entry at `index`, or `None` if it is before the snapshot
    /// boundary or beyond the last suffix entry.
    fn entry_at(&self, index: LogIndex) -> Option<&Entry<C>> {
        let first = self.first_index();
        if index < first {
            return None;
        }
        let offset = usize::try_from(index.get() - first.get()).ok()?;
        self.entries.get(offset)
    }

    /// Validates that entries form a continuous sequence with non-decreasing
    /// terms. The first entry must have index `expected_first`, and each
    /// subsequent entry must have the next consecutive index.
    ///
    /// Raft requires entries to be a contiguous, term-monotonic sequence.
    /// Any gap or term regression indicates a bug in the caller (or a
    /// corrupted durable state that should have been caught at recovery).
    fn validate_continuity(
        &self,
        entries: &[Entry<C>],
        expected_first: LogIndex,
    ) -> Result<(), LogError> {
        let mut expected = expected_first;
        let mut previous_term = self.term_before(expected_first)?;
        for entry in entries {
            if entry.index() != expected {
                return Err(LogError::NonContiguousEntries {
                    expected,
                    actual: entry.index(),
                });
            }
            if entry.term() < previous_term {
                return Err(LogError::TermRegression {
                    index: entry.index(),
                    previous: previous_term,
                    actual: entry.term(),
                });
            }
            previous_term = entry.term();
            expected = expected
                .checked_next()
                .map_err(|_| LogError::IndexOverflow { at: entry.index() })?;
        }
        Ok(())
    }

    /// Returns the term immediately before `first`, or term 0 if `first` is
    /// the virtual origin at index 1 and no snapshot exists.
    fn term_before(&self, first: LogIndex) -> Result<Term, LogError> {
        // The virtual entry at index 0 always has term 0, serving as the
        // implicit predecessor of index 1.
        if first == LogIndex::new(1) && self.snapshot.is_none() {
            return Ok(Term::new(0));
        }
        let previous = LogIndex::new(first.get() - 1);
        self.term(previous)
    }
}
