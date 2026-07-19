//! Quorum-confirmed linearizable read rounds.
//!
//! ReadIndex (Raft paper §6.4) lets a leader serve linearizable reads without
//! writing to the log. The protocol works in three steps:
//!
//! 1. **Heartbeat confirmation**: the leader sends empty AppendEntries RPCs
//!    carrying a unique context (current term + monotonic counter) to all
//!    followers. A quorum of acknowledgements confirms the leader still holds
//!    its leadership at the time the context was sent.
//!
//! 2. **Capture safe index**: once quorum is reached, the current
//!    `commit_index` is captured as the read's safe index — any read served
//!    at or beyond this index is guaranteed to be linearizable.
//!
//! 3. **Apply barrier**: reads are released only after the local state
//!    machine has applied through the safe index. This ensures the read
//!    observes all entries that were committed when leadership was confirmed.
//!
//! Multiple concurrent reads share a single [`ReadRound`] — the leader
//! batches them into one heartbeat round and releases them together once the
//! apply barrier is satisfied.

use crate::progress::QuorumTracker;
use crate::{NodeId, ReadId};

/// Requests sharing one heartbeat context and its acknowledgement tracker.
///
/// The context is `current_term || counter`, making it unique per leader term.
/// Followers echo this context back in their AppendEntries responses; the
/// leader counts matching acknowledgements toward quorum.
pub(crate) struct ReadRound {
    /// The unique context embedded in heartbeat AppendEntries RPCs.
    context: Vec<u8>,
    /// Tracks which members have acknowledged this round's context.
    acknowledgments: QuorumTracker,
    /// Read requests batched into this round.
    requests: Vec<ReadId>,
    /// The commit index captured when quorum was first reached.
    /// Reads must wait for the local applied index to reach this point.
    safe_index: Option<crate::LogIndex>,
}

impl ReadRound {
    /// Starts a round with the local leader acknowledgement already recorded.
    /// The leader implicitly acknowledges its own context.
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

    /// Adds another request to this in-flight round (batching).
    pub(crate) fn push(&mut self, read_id: ReadId) {
        self.requests.push(read_id);
    }

    /// Records an exact-context acknowledgement from a follower.
    pub(crate) fn acknowledge(&mut self, node: NodeId) {
        self.acknowledgments.record(node, true);
    }

    /// Returns whether this round has a quorum acknowledgement.
    pub(crate) fn has_quorum(&self) -> bool {
        self.acknowledgments.has_quorum()
    }

    /// Stores the commit index captured when quorum was reached.
    /// This is the lowest index at which reads in this round are safe.
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
