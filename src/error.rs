use core::fmt;

use crate::NodeId;

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
