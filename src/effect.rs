//! Effects emitted by the Raft core for host execution.

#![allow(missing_docs)]

use crate::{Entry, HardState, LogIndex, NodeId, ProposalId, ReadId, SnapshotRecord};

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
    /// Emits an informational record for host observability.
    Diagnostic { node: NodeId },
}
