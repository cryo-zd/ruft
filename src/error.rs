use core::fmt;

use crate::NodeId;

/// A violation of an internal Raft safety invariant.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum InvariantViolation {
    /// The state machine has advanced beyond the durable commit index.
    AppliedPastCommit,
    /// The durable commit index is beyond the available log.
    CommitPastLog,
    /// The available suffix leaves a gap after the committed prefix.
    CommittedGap,
    /// The logical log and durable hard state disagree on the commit index.
    CommitIndexMismatch,
    /// Cached last-log metadata disagrees with the logical log.
    LastLogMismatch,
    /// An index required by invariant validation cannot be incremented.
    IndexOverflow,
    /// The log suffix is not continuous or its terms regress.
    Log(crate::LogError),
}

impl fmt::Display for InvariantViolation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AppliedPastCommit => formatter.write_str("applied index is beyond commit index"),
            Self::CommitPastLog => formatter.write_str("commit index is beyond last log index"),
            Self::CommittedGap => formatter.write_str("committed prefix has a gap in the log"),
            Self::CommitIndexMismatch => {
                formatter.write_str("hard state and log commit indexes differ")
            }
            Self::LastLogMismatch => {
                formatter.write_str("cached last-log metadata differs from the log")
            }
            Self::IndexOverflow => {
                formatter.write_str("index overflow during invariant validation")
            }
            Self::Log(error) => write!(formatter, "log invariant failed: {error}"),
        }
    }
}

impl std::error::Error for InvariantViolation {}

/// A failure that makes continued operation of this core unsafe.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum FatalError {
    /// Storage did not complete an operation required for correctness.
    Storage,
    /// The application state machine could not apply or build state.
    StateMachine,
    /// Internal state no longer satisfies a Raft safety invariant.
    Invariant(InvariantViolation),
}

impl fmt::Display for FatalError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage => formatter.write_str("fatal storage failure"),
            Self::StateMachine => formatter.write_str("fatal state machine failure"),
            Self::Invariant(error) => write!(formatter, "fatal invariant violation: {error}"),
        }
    }
}

impl std::error::Error for FatalError {}

/// The reason a core no longer accepts protocol work.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StoppedReason {
    /// The host explicitly shut down this core.
    Shutdown,
    /// A fatal correctness fault was reported by the host or detected locally.
    Fatal(FatalError),
}

/// An error raised by checked arithmetic on a protocol counter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ArithmeticError {
    /// Incrementing the underlying integer would wrap.
    Overflow,
}

impl fmt::Display for ArithmeticError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Overflow => formatter.write_str("protocol counter overflow"),
        }
    }
}

impl std::error::Error for ArithmeticError {}

/// An error that prevents a Raft core from being initialized safely.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum InitError {
    /// A Raft group must contain at least one member.
    EmptyMembership,
    /// A node occurs more than once in the fixed member set.
    DuplicateMember(NodeId),
    /// The local node is not part of the fixed member set.
    LocalNodeNotMember(NodeId),
    /// The configured member count exceeds its defensive upper bound.
    TooManyMembers {
        /// Number of supplied members.
        actual: usize,
        /// Configured upper bound.
        maximum: usize,
    },
    /// The election timeout range is non-random or starts at zero.
    InvalidElectionRange {
        /// Inclusive lower bound.
        min: u64,
        /// Inclusive upper bound.
        max: u64,
    },
    /// Heartbeats would not occur strictly before an election timeout.
    InvalidHeartbeatTicks {
        /// Configured heartbeat interval.
        heartbeat: u64,
        /// Minimum election timeout.
        election_min: u64,
    },
    /// CheckQuorum would expire before the minimum election timeout.
    InvalidCheckQuorumTicks {
        /// Configured CheckQuorum interval.
        check_quorum: u64,
        /// Minimum election timeout.
        election_min: u64,
    },
    /// A defensive resource limit was configured as zero.
    ZeroCapacityLimit(&'static str),
    /// Two nonzero capacity limits would still prevent protocol progress.
    InvalidCapacityRelationship {
        /// Limit whose value is too small.
        limit: &'static str,
        /// Configured value of the smaller limit.
        value: usize,
        /// Limit that establishes the required minimum.
        required_at_least: &'static str,
        /// Required minimum value.
        minimum: usize,
    },
    /// Snapshot metadata violates a structural invariant.
    InvalidSnapshotMetadata(&'static str),
}

impl fmt::Display for InitError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyMembership => formatter.write_str("membership must not be empty"),
            Self::DuplicateMember(node) => write!(formatter, "duplicate member {node}"),
            Self::LocalNodeNotMember(node) => {
                write!(formatter, "local node {node} is not a cluster member")
            }
            Self::TooManyMembers { actual, maximum } => write!(
                formatter,
                "membership contains {actual} nodes, exceeding the limit of {maximum}"
            ),
            Self::InvalidElectionRange { min, max } => {
                write!(formatter, "invalid election tick range {min}..={max}")
            }
            Self::InvalidHeartbeatTicks {
                heartbeat,
                election_min,
            } => write!(
                formatter,
                "heartbeat interval {heartbeat} must be nonzero and below election minimum {election_min}"
            ),
            Self::InvalidCheckQuorumTicks {
                check_quorum,
                election_min,
            } => write!(
                formatter,
                "CheckQuorum interval {check_quorum} must be at least election minimum {election_min}"
            ),
            Self::ZeroCapacityLimit(name) => {
                write!(formatter, "capacity limit {name} must be greater than zero")
            }
            Self::InvalidCapacityRelationship {
                limit,
                value,
                required_at_least,
                minimum,
            } => write!(
                formatter,
                "capacity limit {limit} ({value}) must be at least {required_at_least} ({minimum})"
            ),
            Self::InvalidSnapshotMetadata(reason) => {
                write!(formatter, "invalid snapshot metadata: {reason}")
            }
        }
    }
}

impl std::error::Error for InitError {}
