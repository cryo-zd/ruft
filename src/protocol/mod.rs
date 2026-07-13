//! Wire-independent Raft message data-transfer objects.

#![allow(missing_docs)]

use crate::{ClusterId, Entry, NodeId, SnapshotMetadata, Term};

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
    PreVote { term: Term },
    /// A durable vote request for the supplied term.
    RequestVote { term: Term },
    /// A log replication request.
    AppendEntries { term: Term, entries: Vec<Entry<C>> },
    /// A snapshot transfer metadata message.
    InstallSnapshot {
        term: Term,
        metadata: SnapshotMetadata,
    },
}
