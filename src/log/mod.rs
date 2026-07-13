//! In-memory logical log operations across a compacted snapshot boundary.

mod entry;
mod unstable;

use std::ops::RangeInclusive;

pub use entry::{Entry, EntryPayload};

use crate::{LogError, LogIndex, RecoveredState, SnapshotRecord, Term};
use unstable::Unstable;

/// A validated log suffix together with its compacted snapshot boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RaftLog<C> {
    snapshot: Option<SnapshotRecord>,
    entries: Vec<Entry<C>>,
    committed: LogIndex,
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

    /// Returns the first entry still waiting for persistence confirmation.
    pub const fn unstable_from(&self) -> Option<LogIndex> {
        self.unstable.from()
    }

    /// Looks up a term at an available index or at the snapshot boundary.
    pub fn term(&self, index: LogIndex) -> Result<Term, LogError> {
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
        if incoming[0].index() > expected_after_last {
            return Err(LogError::NonContiguousEntries {
                expected: expected_after_last,
                actual: incoming[0].index(),
            });
        }
        self.validate_continuity(&incoming, incoming[0].index())?;

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
                Some(existing) if existing.term() == entry.term() => continue,
                Some(_) => {
                    replace_at = Some(offset);
                    break;
                }
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

        let Some(offset) = replace_at else {
            return Ok(());
        };
        let from = incoming[offset].index();
        if from <= self.committed {
            return Err(LogError::WouldTruncateCommitted {
                from,
                committed: self.committed,
            });
        }

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

    /// Installs a snapshot boundary and discards only the prefix it represents.
    pub fn compact(&mut self, snapshot: SnapshotRecord) -> Result<(), LogError> {
        let through = snapshot.metadata().index();
        if through > self.committed {
            return Err(LogError::CompactPastCommit {
                through,
                committed: self.committed,
            });
        }
        if self.term(through)? != snapshot.metadata().term() {
            return Err(LogError::SnapshotBoundaryMismatch {
                index: through,
                expected: self.term(through)?,
                actual: snapshot.metadata().term(),
            });
        }

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

    fn entry_at(&self, index: LogIndex) -> Option<&Entry<C>> {
        let first = self.first_index();
        if index < first {
            return None;
        }
        let offset = usize::try_from(index.get() - first.get()).ok()?;
        self.entries.get(offset)
    }

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

    fn term_before(&self, first: LogIndex) -> Result<Term, LogError> {
        if first == LogIndex::new(1) && self.snapshot.is_none() {
            return Ok(Term::new(0));
        }
        let previous = LogIndex::new(first.get() - 1);
        self.term(previous)
    }
}
