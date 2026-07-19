//! Fixed-membership vote accounting.
//!
//! A [`QuorumTracker`] records distinct grants and rejections for one
//! election round (PreVote, RequestVote) or read-index confirmation round.
//! It answers two questions: "do we have a majority?" and "is it impossible
//! to ever reach a majority?".

use std::collections::BTreeSet;

use crate::NodeId;

/// Records distinct grants and rejections for one election or read round.
///
/// Each member can respond at most once — the first response (grant or
/// reject) is final. Later duplicate responses for the same member are
/// silently ignored. The local node's vote is recorded at construction.
///
/// # Quorum calculation
///
/// Quorum is `⌊n/2⌋ + 1` (strict majority). For a 3-node cluster, quorum
/// is 2; for a 5-node cluster, quorum is 3.
#[derive(Debug)]
pub(crate) struct QuorumTracker {
    /// All members of the group.
    members: BTreeSet<NodeId>,
    /// Members that have granted their vote/acknowledgement.
    granted: BTreeSet<NodeId>,
    /// Members that have explicitly rejected.
    rejected: BTreeSet<NodeId>,
}

impl QuorumTracker {
    /// Starts a round with the local node's implicit vote already recorded.
    pub(crate) fn with_local_vote(members: &[NodeId], local_id: NodeId) -> Self {
        let members = members.iter().copied().collect();
        let mut granted = BTreeSet::new();
        granted.insert(local_id);
        Self {
            members,
            granted,
            rejected: BTreeSet::new(),
        }
    }

    /// Records one response. Non-members and duplicate responses are silently
    /// ignored — once a node has responded, its position is locked.
    pub(crate) fn record(&mut self, node: NodeId, granted: bool) {
        if !self.members.contains(&node)
            || self.granted.contains(&node)
            || self.rejected.contains(&node)
        {
            return;
        }
        if granted {
            self.granted.insert(node);
        } else {
            self.rejected.insert(node);
        }
    }

    /// Returns whether a strict majority has granted this round.
    pub(crate) fn has_quorum(&self) -> bool {
        self.granted.len() >= self.quorum()
    }

    /// Returns whether the remaining votes cannot form a majority.
    ///
    /// Computed as: `total_members - rejected < quorum`. If there are already
    /// enough rejections that even unanimous grants from the remaining
    /// members cannot reach quorum, the round is hopeless.
    pub(crate) fn cannot_win(&self) -> bool {
        self.members.len().saturating_sub(self.rejected.len()) < self.quorum()
    }

    fn quorum(&self) -> usize {
        self.members.len() / 2 + 1
    }
}
