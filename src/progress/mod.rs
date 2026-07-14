//! Vote and replication progress tracked by the Raft core.

mod quorum;

pub(crate) use quorum::QuorumTracker;
