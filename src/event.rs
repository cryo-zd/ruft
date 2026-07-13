//! Inputs supplied by the host to one Raft core.

#![allow(missing_docs)]

use crate::{EffectId, EffectOutcome, Envelope, ProposalId, ReadId};

/// A logical timer source.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TickKind {
    Election,
    Heartbeat,
}

/// A serial input accepted by `RaftCore::step`.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Event<C> {
    /// Advances a logical protocol timer.
    Tick(TickKind),
    /// Delivers a transport-validated RPC envelope.
    MessageReceived(Envelope<C>),
    /// Requests replication of one application command.
    Propose {
        proposal_id: ProposalId,
        command: C,
        encoded_len: usize,
    },
    /// Requests a linearizable read barrier.
    Read { read_id: ReadId, context: Vec<u8> },
    /// Reports completion of an earlier effect.
    EffectCompleted {
        id: EffectId,
        outcome: EffectOutcome,
    },
    /// Stops admission of new client work.
    Shutdown,
}
