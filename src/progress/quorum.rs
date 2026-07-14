//! Fixed-membership vote accounting.

use std::collections::BTreeSet;

use crate::NodeId;

/// Records distinct grants and rejections for one election round.
#[derive(Debug)]
pub(crate) struct QuorumTracker {
    members: BTreeSet<NodeId>,
    granted: BTreeSet<NodeId>,
    rejected: BTreeSet<NodeId>,
}

impl QuorumTracker {
    /// Starts a round with the local node\x27s implicit vote.
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

    /// Records one response. Later duplicate responses cannot change its outcome.
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
    pub(crate) fn cannot_win(&self) -> bool {
        self.members.len().saturating_sub(self.rejected.len()) < self.quorum()
    }

    fn quorum(&self) -> usize {
        self.members.len() / 2 + 1
    }
}
