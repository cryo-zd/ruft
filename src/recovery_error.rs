use core::fmt;

use crate::{LogIndex, NodeId, Term};

/// An error in the structural fields of a log entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EntryError {
    /// Log index zero is a virtual origin and cannot hold an entry.
    ZeroLogIndex,
    /// Term zero is a virtual origin and cannot create an entry.
    ZeroTerm,
    /// Commands must declare a nonzero encoded length for replication limits.
    ZeroEncodedLength,
}

impl fmt::Display for EntryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroLogIndex => {
                formatter.write_str("log entries require an index greater than zero")
            }
            Self::ZeroTerm => formatter.write_str("log entries require a term greater than zero"),
            Self::ZeroEncodedLength => {
                formatter.write_str("commands require a nonzero encoded length")
            }
        }
    }
}

impl std::error::Error for EntryError {}

/// A durable-state inconsistency discovered before a Raft core starts.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum RecoveryError {
    /// The durable record belongs to an unsupported format version.
    UnsupportedFormat {
        /// Version stored in durable state.
        found: u32,
        /// Version supported by this library.
        supported: u32,
    },
    /// A suffix index was not the next index after its predecessor.
    LogGap {
        /// Required next index.
        expected: LogIndex,
        /// Index actually stored.
        actual: LogIndex,
    },
    /// Commit index falls before the installed snapshot boundary.
    CommitBeforeSnapshot {
        /// Durable commit index.
        commit: LogIndex,
        /// Installed snapshot index.
        snapshot: LogIndex,
    },
    /// Commit index refers to data absent from both snapshot and suffix.
    CommitPastLog {
        /// Durable commit index.
        commit: LogIndex,
        /// Highest available snapshot or log index.
        last: LogIndex,
    },
    /// A later log entry has a smaller term than its predecessor.
    TermRegression {
        /// Index at which term order regressed.
        index: LogIndex,
        /// Previous term.
        previous: Term,
        /// Invalid lower term.
        actual: Term,
    },
    /// Durable current term is lower than a stored snapshot or log term.
    CurrentTermBehindLog {
        /// Durable current term.
        current: Term,
        /// Highest stored term.
        observed: Term,
    },
    /// The durable vote names a node absent from snapshot membership.
    VotedForNonMember(NodeId),
    /// Computing the expected next index would overflow.
    IndexOverflow {
        /// Index that cannot be incremented.
        at: LogIndex,
    },
}

impl fmt::Display for RecoveryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedFormat { found, supported } => write!(
                formatter,
                "recovery format version {found} is unsupported; expected {supported}"
            ),
            Self::LogGap { expected, actual } => {
                write!(
                    formatter,
                    "recovered log gap: expected index {expected}, found {actual}"
                )
            }
            Self::CommitBeforeSnapshot { commit, snapshot } => write!(
                formatter,
                "commit index {commit} precedes snapshot index {snapshot}"
            ),
            Self::CommitPastLog { commit, last } => write!(
                formatter,
                "commit index {commit} exceeds available index {last}"
            ),
            Self::TermRegression {
                index,
                previous,
                actual,
            } => write!(
                formatter,
                "term regression at index {index}: previous term {previous}, actual term {actual}"
            ),
            Self::CurrentTermBehindLog { current, observed } => write!(
                formatter,
                "current term {current} is behind stored term {observed}"
            ),
            Self::VotedForNonMember(node) => {
                write!(formatter, "durable vote names non-member node {node}")
            }
            Self::IndexOverflow { at } => write!(formatter, "index {at} cannot be incremented"),
        }
    }
}

impl std::error::Error for RecoveryError {}

/// A logical-log operation that would violate continuity or durability rules.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum LogError {
    /// The requested index has already been compacted into a snapshot.
    Compacted {
        /// Requested index.
        index: LogIndex,
        /// Last index represented by the snapshot.
        snapshot_index: LogIndex,
    },
    /// The requested index is beyond the known suffix.
    Unavailable {
        /// Requested index.
        index: LogIndex,
        /// Last available index.
        last: LogIndex,
    },
    /// An inclusive range has its start after its end.
    InvalidRange {
        /// Range start.
        start: LogIndex,
        /// Range end.
        end: LogIndex,
    },
    /// A proposed suffix skipped an index.
    NonContiguousEntries {
        /// Required next index.
        expected: LogIndex,
        /// Proposed index.
        actual: LogIndex,
    },
    /// Proposed terms are not monotonic.
    TermRegression {
        /// Index at which term order regressed.
        index: LogIndex,
        /// Previous term.
        previous: Term,
        /// Invalid lower term.
        actual: Term,
    },
    /// Replacing a suffix would overwrite an already committed entry.
    WouldTruncateCommitted {
        /// First index that would be replaced.
        from: LogIndex,
        /// Highest committed index.
        committed: LogIndex,
    },
    /// Snapshot metadata does not match the term already stored at its boundary.
    SnapshotBoundaryMismatch {
        /// Snapshot boundary index.
        index: LogIndex,
        /// Locally stored term.
        expected: Term,
        /// Term carried by snapshot or proposed entry.
        actual: Term,
    },
    /// Compaction would discard an uncommitted suffix.
    CompactPastCommit {
        /// Proposed compacted-through index.
        through: LogIndex,
        /// Highest committed index.
        committed: LogIndex,
    },
    /// A stability acknowledgment exceeds the known log.
    StablePastLog {
        /// Reported durable index.
        stable: LogIndex,
        /// Highest known index.
        last: LogIndex,
    },
    /// An index does not fit the platform's addressable slice space.
    IndexTooLarge {
        /// Unrepresentable index.
        index: LogIndex,
    },
    /// Incrementing an index would wrap.
    IndexOverflow {
        /// Index that cannot be incremented.
        at: LogIndex,
    },
}

impl fmt::Display for LogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Compacted {
                index,
                snapshot_index,
            } => write!(
                formatter,
                "index {index} was compacted at snapshot index {snapshot_index}"
            ),
            Self::Unavailable { index, last } => {
                write!(
                    formatter,
                    "index {index} is unavailable; last index is {last}"
                )
            }
            Self::InvalidRange { start, end } => {
                write!(formatter, "invalid log range {start}..={end}")
            }
            Self::NonContiguousEntries { expected, actual } => write!(
                formatter,
                "non-contiguous entries: expected index {expected}, found {actual}"
            ),
            Self::TermRegression {
                index,
                previous,
                actual,
            } => write!(
                formatter,
                "term regression at index {index}: previous term {previous}, actual term {actual}"
            ),
            Self::WouldTruncateCommitted { from, committed } => write!(
                formatter,
                "replacing from index {from} would truncate committed index {committed}"
            ),
            Self::SnapshotBoundaryMismatch {
                index,
                expected,
                actual,
            } => write!(
                formatter,
                "snapshot boundary {index} has term {actual}, expected {expected}"
            ),
            Self::CompactPastCommit { through, committed } => write!(
                formatter,
                "cannot compact through {through}; commit index is only {committed}"
            ),
            Self::StablePastLog { stable, last } => write!(
                formatter,
                "stable index {stable} exceeds last log index {last}"
            ),
            Self::IndexTooLarge { index } => {
                write!(formatter, "index {index} exceeds platform slice limits")
            }
            Self::IndexOverflow { at } => write!(formatter, "index {at} cannot be incremented"),
        }
    }
}

impl std::error::Error for LogError {}
