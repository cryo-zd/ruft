//! Per-follower replication progress for a fixed Raft group.
//!
//! Each follower's replication state is modeled as a three-state machine:
//!
//! ```text
//! Probe ──(ack)──▶ Replicate ──(reject)──▶ Probe
//!   │                                         │
//!   └───────────(next < first_index)──────────▶ Snapshot
//!                                                  │
//!   ◀──────────(snapshot installed)────────────────┘
//! ```
//!
//! - **Probe**: the leader sends one conservative AppendEntries at a time,
//!   waiting for the response before sending the next. Used for new leaders
//!   and after rejections.
//! - **Replicate**: the leader pipelines up to `max_inflight_appends`
//!   outstanding AppendEntries, optimising for throughput.
//! - **Snapshot**: the follower needs entries that have been compacted into a
//!   snapshot. The leader streams the snapshot before resuming replication.

mod inflights;
mod quorum;

use crate::LogIndex;
use inflights::Inflights;

pub(crate) use quorum::QuorumTracker;

/// The replication strategy selected for one follower.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProgressState {
    /// Send one conservative probe and wait for its response.
    /// Only one outstanding AppendEntries is allowed.
    Probe,
    /// Pipeline bounded append batches after a successful probe.
    /// Multiple AppendEntries can be in flight concurrently.
    Replicate,
    /// A snapshot transfer is in progress. Log replication is paused until
    /// the snapshot is installed and the follower acknowledges.
    Snapshot,
}

/// Volatile leader-side replication state for one follower.
///
/// Tracks three key indices: `match_index` (highest known replicated),
/// `next_index` (next entry to send), and the inflight window for pipelining.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Progress {
    /// Highest log index known to be replicated on this follower.
    match_index: LogIndex,
    /// Next log index to send to this follower.
    next_index: LogIndex,
    /// Current replication strategy.
    state: ProgressState,
    /// Bounded FIFO of outstanding AppendEntries end indexes.
    inflights: Inflights,
    /// Whether this follower has responded since the last heartbeat tick.
    /// Used by CheckQuorum to detect unresponsive followers.
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
    pub fn inflight_count(&self) -> usize {
        self.inflights.len()
    }
    /// Returns whether the follower has recently replied.
    pub const fn recently_active(&self) -> bool {
        self.recently_active
    }

    /// Returns whether another AppendEntries can be sent to this follower.
    ///
    /// - **Probe**: at most one outstanding request.
    /// - **Replicate**: up to `max_inflight` outstanding requests.
    /// - **Snapshot**: blocked — the snapshot must complete first.
    pub(crate) fn can_send(&self) -> bool {
        match self.state {
            ProgressState::Probe => self.inflights.len() == 0,
            ProgressState::Replicate => !self.inflights.is_full(),
            ProgressState::Snapshot => false,
        }
    }

    /// Records that an AppendEntries was sent, ending at `end_index`.
    /// Advances `next_index` when in Replicate mode (pipelining).
    pub(crate) fn sent(&mut self, end_index: LogIndex) {
        let _ = self.inflights.push(end_index);
        if self.state == ProgressState::Replicate {
            self.next_index = end_index
                .checked_next()
                .expect("append index cannot overflow after bounded log validation");
        }
    }

    /// Records a successful acknowledgement up to `index`.
    ///
    /// Advances `match_index`, frees inflight entries, and transitions from
    /// Probe to Replicate when the first probe succeeds.
    pub(crate) fn acknowledged(&mut self, index: LogIndex) -> bool {
        self.recently_active = true;
        // Stale acknowledgement (from a retransmission or old term). Ignore.
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
        // Free all inflight entries up to and including the acknowledged index.
        self.inflights.free_through(index);
        // Raft optimisation: a successful probe transitions to Replicate
        // state so subsequent appends can be pipelined.
        if self.state == ProgressState::Probe {
            self.state = ProgressState::Replicate;
        }
        true
    }

    /// Records a rejection at `rejected` with the suggested `next_index`.
    ///
    /// Drops back to Probe state, clears all inflight requests, and sets
    /// `next_index` to the maximum of the hint and `match_index + 1`. The
    /// `match_index + 1` floor prevents regressing past already-replicated
    /// entries.
    pub(crate) fn reject(&mut self, rejected: LogIndex, next_index: LogIndex) -> bool {
        self.recently_active = true;
        // Stale rejection: the follower already advanced past the rejection
        // point in a later response. Ignore.
        if rejected
            .checked_next()
            .is_ok_and(|next| next < self.next_index)
        {
            return false;
        }
        self.state = ProgressState::Probe;
        self.inflights.clear();
        // The new next_index is the leader's computed retry point, but never
        // below match_index + 1 (we already know entries up to match_index
        // are replicated).
        self.next_index = next_index.max(
            self.match_index
                .checked_next()
                .expect("match index cannot overflow"),
        );
        true
    }

    /// Resets the activity flag at the start of each heartbeat round.
    /// CheckQuorum uses this to detect unresponsive followers.
    pub(crate) fn reset_activity(&mut self) {
        self.recently_active = false;
    }

    /// Transitions to Snapshot state. Called when the follower's `next_index`
    /// falls before the leader's first available log entry.
    pub(crate) fn enter_snapshot(&mut self) {
        self.state = ProgressState::Snapshot;
        self.inflights.clear();
    }

    /// Resets to Probe state after a snapshot installation completes.
    pub(crate) fn restore_probe(&mut self, next_index: LogIndex) {
        self.state = ProgressState::Probe;
        self.next_index = next_index;
        self.inflights.clear();
    }
}
