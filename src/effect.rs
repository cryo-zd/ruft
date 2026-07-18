//! Effects emitted by the Raft core for host execution.

#![allow(missing_docs)]

use crate::{
    Entry, HardState, LogIndex, NodeId, ProposalId, ReadId, SnapshotMetadata, SnapshotRecord,
    SnapshotRef,
};

/// The terminal outcome for one host proposal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProposalResult {
    /// The proposal command was applied through its log index.
    Applied { index: LogIndex },
    /// The proposal was not committed before local leadership ended.
    LeadershipLost,
    /// The core could not accept the proposal at this time.
    Rejected,
}

/// Result reported after an asynchronous effect completes.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EffectOutcome {
    Persisted,
    Applied {
        through: LogIndex,
    },
    /// A host-built local snapshot body and its validated metadata.
    SnapshotChunkRead {
        snapshot_id: crate::SnapshotId,
        offset: u64,
        bytes: Vec<u8>,
        done: bool,
    },
    SnapshotChunkStored {
        snapshot_id: crate::SnapshotId,
        next_offset: u64,
        snapshot_ref: Option<SnapshotRef>,
    },
    SnapshotInstalled {
        snapshot_id: crate::SnapshotId,
    },
    SnapshotBuilt {
        metadata: SnapshotMetadata,
        snapshot_ref: SnapshotRef,
    },
    /// The durable log prefix through an index was removed.
    Compacted {
        through: LogIndex,
    },
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
    /// Sends one Raft RPC through the host transport.
    SendMessage {
        to: NodeId,
        message: crate::Message<C>,
    },
    /// Persists a Raft batch before dependent protocol actions proceed.
    Persist {
        id: crate::EffectId,
        batch: PersistBatch<C>,
    },
    /// Reports a terminal host proposal outcome.
    ProposalResult {
        proposal_id: ProposalId,
        result: ProposalResult,
    },
    /// Releases a linearizable read after quorum confirmation and local apply.
    ReadReady {
        read_id: ReadId,
        read_index: LogIndex,
    },
    /// Applies committed entries to the host state machine.
    Apply {
        id: crate::EffectId,
        entries: Vec<Entry<C>>,
    },
    /// Reads one bounded chunk from a local snapshot body for transmission.
    ReadSnapshotChunk {
        id: crate::EffectId,
        snapshot: SnapshotRecord,
        offset: u64,
        max_len: usize,
    },
    /// Stores one validated incoming snapshot chunk.
    StoreSnapshotChunk {
        id: crate::EffectId,
        metadata: SnapshotMetadata,
        offset: u64,
        bytes: Vec<u8>,
    },
    /// Installs a durable snapshot into the host state machine.
    InstallSnapshot {
        id: crate::EffectId,
        record: SnapshotRecord,
    },
    /// Builds an externally stored snapshot body through an applied log index.
    BuildSnapshot {
        id: crate::EffectId,
        through: LogIndex,
    },
    /// Removes a durable prefix after snapshot metadata is durable.
    CompactLog {
        id: crate::EffectId,
        through: LogIndex,
    },
    /// Emits an informational record for host observability.
    Diagnostic { node: NodeId },
}
