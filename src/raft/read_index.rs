//! Quorum-confirmed linearizable read rounds.

use crate::progress::QuorumTracker;
use crate::{NodeId, ReadId};

/// Requests sharing one heartbeat context and its acknowledgement tracker.
pub(crate) struct ReadRound {
    context: Vec<u8>,
    acknowledgments: QuorumTracker,
    requests: Vec<ReadId>,
    safe_index: Option<crate::LogIndex>,
}

impl ReadRound {
    /// Starts a round with the local leader acknowledgement already recorded.
    pub(crate) fn new(
        context: Vec<u8>,
        members: &[NodeId],
        local_id: NodeId,
        read_id: ReadId,
    ) -> Self {
        Self {
            context,
            acknowledgments: QuorumTracker::with_local_vote(members, local_id),
            requests: vec![read_id],
            safe_index: None,
        }
    }

    /// Returns the context that must be echoed by followers.
    pub(crate) fn context(&self) -> &[u8] {
        &self.context
    }

    /// Returns the number of requests sharing this round.
    pub(crate) fn request_count(&self) -> usize {
        self.requests.len()
    }

    /// Adds another request to this in-flight round.
    pub(crate) fn push(&mut self, read_id: ReadId) {
        self.requests.push(read_id);
    }

    /// Records an exact-context acknowledgement.
    pub(crate) fn acknowledge(&mut self, node: NodeId) {
        self.acknowledgments.record(node, true);
    }

    /// Returns whether this round has a quorum acknowledgement.
    pub(crate) fn has_quorum(&self) -> bool {
        self.acknowledgments.has_quorum()
    }

    /// Stores the commit index captured when quorum was reached.
    pub(crate) fn set_safe_index(&mut self, index: crate::LogIndex) {
        self.safe_index = Some(index);
    }

    /// Returns the captured safe index, if quorum has responded.
    pub(crate) fn safe_index(&self) -> Option<crate::LogIndex> {
        self.safe_index
    }

    /// Consumes the read identifiers after their barrier is satisfied.
    pub(crate) fn into_requests(self) -> Vec<ReadId> {
        self.requests
    }
}
