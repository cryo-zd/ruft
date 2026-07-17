//! Local snapshot build, durability, and compaction coordination.

use crate::{EffectId, LogIndex, SnapshotRecord};

/// The single in-flight local snapshot operation, if any.
pub(crate) enum LocalSnapshotState {
    /// No local snapshot operation is active.
    Idle,
    /// The host is building the immutable snapshot body.
    Building { id: EffectId, through: LogIndex },
    /// The snapshot metadata and reference await durable persistence.
    Persisting {
        id: EffectId,
        record: SnapshotRecord,
    },
    /// The host may now remove the durable log prefix.
    Compacting {
        id: EffectId,
        record: SnapshotRecord,
    },
}

impl LocalSnapshotState {
    /// Returns whether a local snapshot operation is active.
    pub(crate) const fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }
}
