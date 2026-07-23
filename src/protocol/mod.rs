//! Wire-independent Raft message data-transfer objects.
//!
//! Every Raft RPC is represented as a [`Message`] variant wrapped in an
//! [`Envelope`] that carries routing metadata (cluster, sender, recipient).
//! The core validates envelopes before dispatching to protocol handlers.
//!
//! Messages carry no transport framing, codec, or authentication — the host
//! is responsible for serialization, integrity, and peer identity binding.

use crate::{ClusterId, Entry, LogIndex, NodeId, SnapshotMetadata, Term};

/// A follower hint that lets a leader skip impossible AppendEntries prefixes.
///
/// When a follower rejects an AppendEntries request because of a log
/// inconsistency, it includes a [`ConflictHint`] so the leader can jump
/// directly to a plausible probe point instead of decrementing `next_index`
/// one entry at a time (Raft paper §5.3 optimization).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConflictHint {
    /// The next index the leader should probe.
    pub index: LogIndex,
    /// The conflicting local term, when the follower has a log entry at the
    /// rejected `prev_log_index`. When `None`, the follower has no entry at
    /// that index (either compacted or beyond its log).
    pub term: Option<Term>,
}

impl ConflictHint {
    /// Creates a conflict hint with an optional local conflicting term.
    pub const fn new(index: LogIndex, term: Option<Term>) -> Self {
        Self { index, term }
    }
}

/// A message bound to one Raft group and an intended recipient.
///
/// The envelope carries plain-text routing fields — the host must
/// authenticate the sender before constructing the envelope and must
/// validate the cluster ID before delivering to a core.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Envelope<C> {
    cluster_id: ClusterId,
    from: NodeId,
    to: NodeId,
    message: Message<C>,
}

impl<C> Envelope<C> {
    /// Creates a transport-independent Raft envelope.
    pub fn new(cluster_id: ClusterId, from: NodeId, to: NodeId, message: Message<C>) -> Self {
        Self {
            cluster_id,
            from,
            to,
            message,
        }
    }
    /// Returns the logical group identifier. Used by the core to reject
    /// messages destined for a different Raft group.
    pub const fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }
    /// Returns the sender node (host-authenticated identity).
    pub const fn from(&self) -> NodeId {
        self.from
    }
    /// Returns the intended recipient.
    pub const fn to(&self) -> NodeId {
        self.to
    }
    /// Returns the Raft payload.
    pub const fn message(&self) -> &Message<C> {
        &self.message
    }
}

/// Raft RPC payloads used by the runtime-neutral core.
///
/// Each variant corresponds to one Raft protocol operation. The core does
/// not serialize these — the host codec is responsible for converting between
/// this representation and the wire format.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Message<C> {
    /// A heartbeat with no log entries.
    ///
    /// Used by the leader during read-index rounds to carry the read context
    /// without appending entries. Also sent as a keep-alive when no entries
    /// need replication.
    Heartbeat,
    /// A pre-election request for the supplied prospective term.
    ///
    /// PreVote asks followers whether they *would* vote for the sender in the
    /// next term, without incrementing the durable term. This prevents a
    /// partitioned node from disrupting an active cluster by repeatedly
    /// incrementing its term and forcing elections.
    PreVote {
        /// The prospective term (current term + 1).
        term: Term,
        /// The candidate's last log index, used for the log completeness check.
        last_log_index: LogIndex,
        /// The term of the candidate's last log entry.
        last_log_term: Term,
    },
    /// A durable vote request for the supplied term.
    ///
    /// Sent by a candidate that has already persisted its incremented term
    /// and self-vote. Followers grant at most one vote per term, and only
    /// to candidates whose log is at least as up-to-date as their own.
    RequestVote {
        /// The candidate's current term.
        term: Term,
        /// The candidate's last log index, for the log completeness check.
        last_log_index: LogIndex,
        /// The term of the candidate's last log entry.
        last_log_term: Term,
    },
    /// A response to a pre-election request.
    PreVoteResponse {
        /// The responder's current term (for the candidate to detect a higher term).
        term: Term,
        /// Whether the responder grants the pre-vote.
        granted: bool,
    },
    /// A response to a durable vote request.
    VoteResponse {
        /// The responder's current term.
        term: Term,
        /// Whether the vote was granted.
        granted: bool,
    },
    /// A log replication request with its preceding-log proof and commit point.
    ///
    /// The leader proves log consistency by supplying the term at
    /// `prev_log_index`. A follower accepts the request only if it has a
    /// matching entry at that index. This is the core Raft log matching
    /// property (Raft paper §5.3).
    AppendEntries {
        /// The leader's current term.
        term: Term,
        /// The index immediately before the first entry in this batch.
        /// Used as the consistency anchor.
        prev_log_index: LogIndex,
        /// The term of the entry at `prev_log_index`.
        prev_log_term: Term,
        /// The leader's durable commit index (so followers can advance their
        /// own commit point).
        leader_commit: LogIndex,
        /// An opaque read-round context. When `Some`, followers echo this
        /// back in their response so the leader can confirm read-index quorum.
        read_context: Option<Vec<u8>>,
        /// Zero or more log entries to append, in ascending index order.
        entries: Vec<Entry<C>>,
    },
    /// A response to an AppendEntries request.
    AppendEntriesResponse {
        /// The follower's current term (may be higher than the leader's).
        term: Term,
        /// Whether the previous-log proof matched.
        success: bool,
        /// The highest index replicated to this follower (on success) or
        /// the rejected `prev_log_index` (on failure).
        match_index: LogIndex,
        /// When `success` is false, an optional hint for faster retry.
        conflict: Option<ConflictHint>,
        /// The echoed read-round context, if the leader sent one.
        read_context: Option<Vec<u8>>,
    },
    /// One bounded chunk of a snapshot transfer.
    ///
    /// Snapshots are streamed in chunks to avoid exceeding message size
    /// limits. The first chunk has `offset == 0`; the last has `done == true`.
    /// Chunks must arrive in order and the final digest must match the
    /// snapshot metadata.
    InstallSnapshot {
        /// The leader's current term.
        term: Term,
        /// Metadata describing the snapshot being transferred.
        metadata: SnapshotMetadata,
        /// Byte offset of this chunk within the snapshot body.
        offset: u64,
        /// The chunk payload.
        bytes: Vec<u8>,
        /// Whether this is the final chunk.
        done: bool,
    },
    /// A response after a snapshot is durably installed.
    InstallSnapshotResponse {
        /// The follower's current term.
        term: Term,
        /// Whether the snapshot was successfully installed.
        success: bool,
    },
}
