//! Wire-independent Raft message data-transfer objects.

#![allow(missing_docs)]

use crate::{ClusterId, Entry, LogIndex, NodeId, SnapshotMetadata, Term};

/// A follower hint that lets a leader skip impossible AppendEntries prefixes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ConflictHint {
    /// The next index a leader should probe.
    pub index: LogIndex,
    /// The conflicting local term, when the follower has one.
    pub term: Option<Term>,
}

impl ConflictHint {
    /// Creates a conflict hint with an optional local conflicting term.
    pub const fn new(index: LogIndex, term: Option<Term>) -> Self {
        Self { index, term }
    }
}

/// A message bound to one Raft group and an intended recipient.
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
    /// Returns the logical group identifier.
    pub const fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }
    /// Returns the sender node.
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
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Message<C> {
    /// A heartbeat with no log entries.
    Heartbeat,
    /// A pre-election request for the supplied prospective term.
    PreVote {
        term: Term,
        last_log_index: LogIndex,
        last_log_term: Term,
    },
    /// A durable vote request for the supplied term.
    RequestVote {
        term: Term,
        last_log_index: LogIndex,
        last_log_term: Term,
    },
    /// A response to a pre-election request.
    PreVoteResponse { term: Term, granted: bool },
    /// A response to a durable vote request.
    VoteResponse { term: Term, granted: bool },
    /// A log replication request with its preceding-log proof and commit point.
    AppendEntries {
        term: Term,
        prev_log_index: LogIndex,
        prev_log_term: Term,
        leader_commit: LogIndex,
        entries: Vec<Entry<C>>,
    },
    /// A response to an AppendEntries request.
    AppendEntriesResponse {
        term: Term,
        success: bool,
        match_index: LogIndex,
        conflict: Option<ConflictHint>,
    },
    /// A snapshot transfer metadata message.
    InstallSnapshot {
        term: Term,
        metadata: SnapshotMetadata,
    },
}
