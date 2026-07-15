//! Bounded outstanding AppendEntries acknowledgements.

use std::collections::VecDeque;

use crate::LogIndex;

/// A fixed-capacity FIFO of append request end indexes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Inflights {
    capacity: usize,
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

    /// Returns whether another request can be pipelined.
    pub(crate) fn is_full(&self) -> bool {
        self.indexes.len() >= self.capacity
    }

    /// Records a request end index after checking capacity.
    pub(crate) fn push(&mut self, index: LogIndex) -> bool {
        if self.is_full() {
            return false;
        }
        self.indexes.push_back(index);
        true
    }

    /// Releases every request acknowledged through `index`.
    pub(crate) fn free_through(&mut self, index: LogIndex) {
        while self.indexes.front().is_some_and(|front| *front <= index) {
            self.indexes.pop_front();
        }
    }

    /// Discards all outstanding requests after a rejection.
    pub(crate) fn clear(&mut self) {
        self.indexes.clear();
    }

    /// Returns the current number of outstanding requests.
    pub(crate) fn len(&self) -> usize {
        self.indexes.len()
    }
}
