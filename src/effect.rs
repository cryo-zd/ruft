//! Effects emitted by the Raft core for host execution.

#![allow(missing_docs)]

use crate::{Entry, HardState, LogIndex, NodeId, SnapshotRecord};

/// Result reported after an asynchronous effect completes.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EffectOutcome {
    Persisted,
    Applied { through: LogIndex },
    Failed,
}

/// A durable mutation requested from the host storage adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistBatch<C> {
    pub hard_state: Option<HardState>,
    pub entries: Vec<Entry<C>>,
    pub snapshot: Option<SnapshotRecord>,
}

/// A host-side action produced by the core.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Effect<C> {
    /// Persists a Raft batch before dependent protocol actions proceed.
    Persist {
        id: crate::EffectId,
        batch: PersistBatch<C>,
    },
    /// Applies committed entries to the host state machine.
    Apply {
        id: crate::EffectId,
        entries: Vec<Entry<C>>,
    },
    /// Emits an informational record for host observability.
    Diagnostic { node: NodeId },
}
