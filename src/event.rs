//! Inputs supplied by the host to one Raft core.
//!
//! Each [`Event`] represents a serialized stimulus that drives the Raft state
//! machine forward. The host calls [`RaftCore::step`](crate::RaftCore::step)
//! with one event at a time and executes the returned [`Effect`](crate::Effect)
//! values before delivering the next event.
//!
//! Events are processed sequentially within a single core instance. The host
//! is responsible for serializing access — concurrent `step` calls on the
//! same core are not supported.

use crate::{EffectId, EffectOutcome, Envelope, ProposalId, ReadId};

/// A logical timer source.
///
/// The host maintains independent election and heartbeat timers and delivers
/// the corresponding tick when each timer fires. The core does not track
/// wall-clock time — ticks are purely logical.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TickKind {
    /// Fires when the election timeout elapses.
    ///
    /// In non-leader roles this triggers a pre-election (PreVote). Leaders
    /// ignore election ticks — they only step down via CheckQuorum on
    /// heartbeat ticks or when they discover a higher term.
    Election,
    /// Fires at the heartbeat interval (typically shorter than the election
    /// timeout minimum).
    ///
    /// Leaders use this to send AppendEntries heartbeats and to run
    /// CheckQuorum (stepping down if fewer than a quorum of followers have
    /// responded recently). Non-leaders ignore heartbeat ticks.
    Heartbeat,
}

/// A serial input accepted by [`RaftCore::step`](crate::RaftCore::step).
///
/// Each event represents one discrete stimulus. The core processes the event,
/// optionally emits effects, and returns. The host must complete all returned
/// effects before delivering the next event (for that core instance).
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Event<C> {
    /// Advances a logical protocol timer.
    ///
    /// See [`TickKind`] for the semantics of each tick variant.
    Tick(TickKind),
    /// Delivers a transport-validated RPC envelope.
    ///
    /// The host is responsible for authenticating the sender and setting
    /// [`Envelope::from`] to the verified identity. The core rejects messages
    /// from unknown senders or with a mismatched cluster ID.
    MessageReceived(Envelope<C>),
    /// Requests replication of one application command.
    ///
    /// The core appends a new log entry in the current term (if leader) and
    /// replicates it to followers. The host receives the outcome
    /// asynchronously via [`Effect::ProposalResult`](crate::Effect::ProposalResult).
    Propose {
        /// Host-assigned identifier for correlating the result.
        proposal_id: ProposalId,
        /// The application command to replicate.
        command: C,
        /// The host's estimate of the command's encoded size, used for
        /// per-RPC byte budgeting. Must be nonzero.
        encoded_len: usize,
    },
    /// Requests a linearizable read barrier.
    ///
    /// The core confirms leadership via a quorum heartbeat round and waits
    /// for the local state machine to apply through the confirmed read index
    /// before emitting [`Effect::ReadReady`](crate::Effect::ReadReady).
    Read {
        /// Host-assigned identifier for correlating the result.
        read_id: ReadId,
        /// Opaque context — reserved for future use, currently ignored.
        context: Vec<u8>,
    },
    /// Requests a local snapshot at the highest applied index.
    ///
    /// The core initiates the local snapshot pipeline (build → persist →
    /// compact) if the number of applied entries since the last snapshot
    /// exceeds the configured threshold.
    SnapshotRequested,
    /// Reports completion of an earlier effect.
    ///
    /// The `id` must match an outstanding [`Effect`](crate::Effect) emitted
    /// by this core generation. The `outcome` must match the effect type
    /// (for example, a [`Persist`](crate::Effect::Persist) must complete
    /// with [`EffectOutcome::Persisted`]). Mismatches or duplicates are
    /// rejected as input errors.
    EffectCompleted {
        /// The effect identifier from the original effect.
        id: EffectId,
        /// The result reported by the host after executing the effect.
        outcome: EffectOutcome,
    },
    /// Stops admission of new client work.
    ///
    /// The core stops accepting proposals and read requests. Already-issued
    /// effects may still complete. After shutdown,
    /// [`RaftCore::is_stopped`](crate::RaftCore::is_stopped) returns `true`.
    Shutdown,
}
