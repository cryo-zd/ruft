use crate::LogIndex;

/// Tracks the continuous range of entries that have not yet been confirmed durable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct Unstable {
    from: Option<LogIndex>,
    through: Option<LogIndex>,
}

impl Unstable {
    pub(super) const fn from(self) -> Option<LogIndex> {
        self.from
    }

    pub(super) fn mark_range(&mut self, from: LogIndex, through: LogIndex) {
        debug_assert!(from <= through);
        self.from = Some(self.from.map_or(from, |current| current.min(from)));
        self.through = Some(self.through.map_or(through, |current| current.max(through)));
    }

    pub(super) fn truncate_from(&mut self, from: LogIndex) {
        let (Some(first), Some(last)) = (self.from, self.through) else {
            return;
        };
        if from <= first {
            self.from = None;
            self.through = None;
        } else if from <= last {
            self.through = Some(LogIndex::new(from.get() - 1));
        }
    }

    pub(super) fn mark_stable_through(&mut self, index: LogIndex) {
        let (Some(from), Some(through)) = (self.from, self.through) else {
            return;
        };
        if index < from {
            return;
        }
        if index >= through {
            self.from = None;
            self.through = None;
            return;
        }
        self.from = index.checked_next().ok();
    }

    pub(super) fn discard_through(&mut self, index: LogIndex) {
        self.mark_stable_through(index);
    }
}
