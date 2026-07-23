//! Bounded outstanding AppendEntries acknowledgements.
//!
//! When in Replicate mode, the leader pipelines multiple AppendEntries RPCs
//! to a follower. The [`Inflights`] queue tracks the end index of each
//! outstanding request so the leader can enforce a maximum window size and
//! free entries as acknowledgements arrive.

use std::collections::VecDeque;

use crate::LogIndex;

/// A fixed-capacity FIFO of append request end indexes.
///
/// Each entry records the `end_index` (last log index) of one in-flight
/// AppendEntries RPC. When a follower acknowledges through `match_index`,
/// all entries with `end_index <= match_index` are freed.
///
/// This is cumulative acknowledgement: acknowledging index N implicitly
/// acknowledges all entries up to N, so all inflight requests ending at or
/// before N are considered complete.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Inflights {
    /// The maximum number of outstanding AppendEntries requests.
    capacity: usize,
    /// FIFO queue of request end indexes, in send order.
    indexes: VecDeque<LogIndex>,
}

impl Inflights {
    /// Creates an empty queue with a nonzero validated capacity.
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            indexes: VecDeque::with_capacity(capacity),
        }
    }

    /// Returns whether the window is full — no more requests can be
    /// pipelined until one completes.
    pub(crate) fn is_full(&self) -> bool {
        self.indexes.len() >= self.capacity
    }

    /// Records a request end index. Returns `false` if the window is full.
    pub(crate) fn push(&mut self, index: LogIndex) -> bool {
        if self.is_full() {
            return false;
        }
        self.indexes.push_back(index);
        true
    }

    /// Releases every request whose end index is ≤ `index`.
    ///
    /// Because AppendEntries are sent in order and acknowledged cumulatively,
    /// an acknowledgement through index N means all requests ending at or
    /// before N have been received by the follower.
    pub(crate) fn free_through(&mut self, index: LogIndex) {
        while self.indexes.front().is_some_and(|front| *front <= index) {
            self.indexes.pop_front();
        }
    }

    /// Discards all outstanding requests (e.g., after a rejection or state
    /// reset).
    pub(crate) fn clear(&mut self) {
        self.indexes.clear();
    }

    /// Returns the current number of outstanding requests.
    pub(crate) fn len(&self) -> usize {
        self.indexes.len()
    }
}
