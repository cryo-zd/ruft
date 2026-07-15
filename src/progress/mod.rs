//! Per-follower replication progress for a fixed Raft group.

mod inflights;
mod quorum;

use crate::LogIndex;
use inflights::Inflights;

pub(crate) use quorum::QuorumTracker;

/// The replication strategy selected for one follower.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgressState {
    /// Send one conservative probe and wait for its response.
    Probe,
    /// Pipeline bounded append batches after a successful probe.
    Replicate,
    /// A future snapshot transfer is required before log replication resumes.
    Snapshot,
}

/// Volatile leader-side replication state for one follower.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Progress {
    match_index: LogIndex,
    next_index: LogIndex,
    state: ProgressState,
    inflights: Inflights,
    recently_active: bool,
}

impl Progress {
    /// Creates a follower in Probe state at the supplied next index.
    pub(crate) fn new(next_index: LogIndex, max_inflight: usize) -> Self {
        Self {
            match_index: LogIndex::new(0),
            next_index,
            state: ProgressState::Probe,
            inflights: Inflights::new(max_inflight),
            recently_active: false,
        }
    }

    /// Returns the highest index known replicated by this follower.
    pub const fn match_index(&self) -> LogIndex {
        self.match_index
    }
    /// Returns the next index this follower should receive.
    pub const fn next_index(&self) -> LogIndex {
        self.next_index
    }
    /// Returns the current replication strategy.
    pub const fn state(&self) -> ProgressState {
        self.state
    }
    /// Returns the number of requests awaiting acknowledgement.
    /// Returns the number of requests awaiting acknowledgement.
    pub fn inflight_count(&self) -> usize {
        self.inflights.len()
    }
    /// Returns whether the follower has recently replied.
    pub const fn recently_active(&self) -> bool {
        self.recently_active
    }

    pub(crate) fn can_send(&self) -> bool {
        match self.state {
            ProgressState::Probe => self.inflights.len() == 0,
            ProgressState::Replicate => !self.inflights.is_full(),
            ProgressState::Snapshot => false,
        }
    }

    pub(crate) fn sent(&mut self, end_index: LogIndex) {
        let _ = self.inflights.push(end_index);
        if self.state == ProgressState::Replicate {
            self.next_index = end_index
                .checked_next()
                .expect("append index cannot overflow after bounded log validation");
        }
    }

    pub(crate) fn acknowledged(&mut self, index: LogIndex) -> bool {
        self.recently_active = true;
        if index < self.match_index {
            return false;
        }
        self.match_index = index;
        let next = index
            .checked_next()
            .expect("acknowledged log index cannot overflow");
        if next > self.next_index {
            self.next_index = next;
        }
        self.inflights.free_through(index);
        if self.state == ProgressState::Probe {
            self.state = ProgressState::Replicate;
        }
        true
    }

    pub(crate) fn reject(&mut self, rejected: LogIndex, next_index: LogIndex) -> bool {
        self.recently_active = true;
        if rejected
            .checked_next()
            .is_ok_and(|next| next < self.next_index)
        {
            return false;
        }
        self.state = ProgressState::Probe;
        self.inflights.clear();
        self.next_index = next_index.max(
            self.match_index
                .checked_next()
                .expect("match index cannot overflow"),
        );
        true
    }

    pub(crate) fn reset_activity(&mut self) {
        self.recently_active = false;
    }

    pub(crate) fn enter_snapshot(&mut self) {
        self.state = ProgressState::Snapshot;
        self.inflights.clear();
    }
}
