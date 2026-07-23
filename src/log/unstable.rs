use crate::LogIndex;

/// Tracks the continuous range of entries that have not yet been confirmed
/// durable.
///
/// When entries are appended or merged from a leader, they are "unstable" —
/// present in the logical log but not yet persisted. Once the host confirms
/// persistence through an index, the unstable window shrinks from below.
///
/// The window may expand (new appends extend the `through` boundary) or be
/// truncated (conflict replacement may discard the suffix). Snapshot
/// installation discards the entire window since all entries below the
/// snapshot are implicitly durable.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct Unstable {
    /// The first index not yet confirmed durable, if any.
    from: Option<LogIndex>,
    /// The last index in the unstable range, if any.
    through: Option<LogIndex>,
}

impl Unstable {
    pub(super) const fn from(self) -> Option<LogIndex> {
        self.from
    }

    /// Marks `[from, through]` as unstable, extending the existing window
    /// if one exists. Multiple appends merge into a single window by taking
    /// the minimum `from` and maximum `through`.
    pub(super) fn mark_range(&mut self, from: LogIndex, through: LogIndex) {
        debug_assert!(from <= through);
        self.from = Some(self.from.map_or(from, |current| current.min(from)));
        self.through = Some(self.through.map_or(through, |current| current.max(through)));
    }

    /// Truncates the unstable window when entries are removed by conflict
    /// replacement. If the truncation point is at or before `from`, the
    /// entire window is cleared.
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

    /// Confirms durability through `index`, shrinking the window from below.
    /// If all entries are now durable, the window becomes empty.
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

    /// Discards the unstable window through `index`, used when a snapshot
    /// covers entries that were previously tracked as unstable.
    pub(super) fn discard_through(&mut self, index: LogIndex) {
        self.mark_stable_through(index);
    }
}
