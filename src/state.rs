use crate::{Entry, LogIndex, NodeId, RecoveryError, SnapshotMetadata, SnapshotRef, Term};

/// The durable Raft state that changes independently of log entries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HardState {
    current_term: Term,
    voted_for: Option<NodeId>,
    commit_index: LogIndex,
}

impl HardState {
    /// Creates durable term, vote, and commit state.
    pub const fn new(
        current_term: Term,
        voted_for: Option<NodeId>,
        commit_index: LogIndex,
    ) -> Self {
        Self {
            current_term,
            voted_for,
            commit_index,
        }
    }

    /// Returns the durable current term.
    pub const fn current_term(&self) -> Term {
        self.current_term
    }

    /// Returns the node selected in the durable vote record, if any.
    pub const fn voted_for(&self) -> Option<NodeId> {
        self.voted_for
    }

    /// Returns the highest known committed index.
    pub const fn commit_index(&self) -> LogIndex {
        self.commit_index
    }
}

/// A durable snapshot reference paired with the metadata that makes it a Raft boundary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotRecord {
    metadata: SnapshotMetadata,
    snapshot_ref: SnapshotRef,
}

impl SnapshotRecord {
    /// Pairs validated metadata with an opaque durable snapshot reference.
    pub fn new(metadata: SnapshotMetadata, snapshot_ref: SnapshotRef) -> Self {
        Self {
            metadata,
            snapshot_ref,
        }
    }

    /// Returns the snapshot's Raft metadata.
    pub const fn metadata(&self) -> &SnapshotMetadata {
        &self.metadata
    }

    /// Returns the storage-defined snapshot reference.
    pub const fn snapshot_ref(&self) -> &SnapshotRef {
        &self.snapshot_ref
    }
}

/// Validated durable input used to reconstruct a Raft core after restart.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecoveredState<C> {
    hard_state: HardState,
    snapshot: Option<SnapshotRecord>,
    entries: Vec<Entry<C>>,
    format_version: u32,
}

impl<C> RecoveredState<C> {
    /// The only durable-state format understood by this release.
    pub const FORMAT_VERSION: u32 = 1;

    /// Creates a state record in the current durable format.
    pub fn new(
        hard_state: HardState,
        snapshot: Option<SnapshotRecord>,
        entries: Vec<Entry<C>>,
    ) -> Result<Self, RecoveryError> {
        Self::from_parts(Self::FORMAT_VERSION, hard_state, snapshot, entries)
    }

    /// Creates a state record after checking its durable format version and invariants.
    pub fn from_parts(
        format_version: u32,
        hard_state: HardState,
        snapshot: Option<SnapshotRecord>,
        entries: Vec<Entry<C>>,
    ) -> Result<Self, RecoveryError> {
        if format_version != Self::FORMAT_VERSION {
            return Err(RecoveryError::UnsupportedFormat {
                found: format_version,
                supported: Self::FORMAT_VERSION,
            });
        }

        let snapshot_index = snapshot
            .as_ref()
            .map_or(LogIndex::new(0), |record| record.metadata().index());
        let snapshot_term = snapshot
            .as_ref()
            .map_or(Term::new(0), |record| record.metadata().term());

        if hard_state.commit_index() < snapshot_index {
            return Err(RecoveryError::CommitBeforeSnapshot {
                commit: hard_state.commit_index(),
                snapshot: snapshot_index,
            });
        }

        if let Some(record) = snapshot.as_ref() {
            if let Some(voted_for) = hard_state.voted_for() {
                if !record.metadata().members().contains(&voted_for) {
                    return Err(RecoveryError::VotedForNonMember(voted_for));
                }
            }
        }

        let mut expected = snapshot_index
            .checked_next()
            .map_err(|_| RecoveryError::IndexOverflow { at: snapshot_index })?;
        let mut previous_term = snapshot_term;
        for entry in &entries {
            if entry.index() != expected {
                return Err(RecoveryError::LogGap {
                    expected,
                    actual: entry.index(),
                });
            }
            if entry.term() < previous_term {
                return Err(RecoveryError::TermRegression {
                    index: entry.index(),
                    previous: previous_term,
                    actual: entry.term(),
                });
            }
            previous_term = entry.term();
            expected = expected
                .checked_next()
                .map_err(|_| RecoveryError::IndexOverflow { at: entry.index() })?;
        }

        if hard_state.current_term() < previous_term {
            return Err(RecoveryError::CurrentTermBehindLog {
                current: hard_state.current_term(),
                observed: previous_term,
            });
        }

        let last_index = entries.last().map_or(snapshot_index, Entry::index);
        if hard_state.commit_index() > last_index {
            return Err(RecoveryError::CommitPastLog {
                commit: hard_state.commit_index(),
                last: last_index,
            });
        }

        Ok(Self {
            hard_state,
            snapshot,
            entries,
            format_version,
        })
    }

    /// Returns the validated durable hard state.
    pub const fn hard_state(&self) -> &HardState {
        &self.hard_state
    }

    /// Returns the durable snapshot boundary, if one exists.
    pub const fn snapshot(&self) -> Option<&SnapshotRecord> {
        self.snapshot.as_ref()
    }

    /// Returns the continuous suffix following the snapshot boundary.
    pub fn entries(&self) -> &[Entry<C>] {
        &self.entries
    }

    /// Returns the validated durable format version.
    pub const fn format_version(&self) -> u32 {
        self.format_version
    }
}
