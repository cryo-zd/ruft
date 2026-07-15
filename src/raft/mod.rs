//! Raft state-transition helpers.

mod commit;
mod election;
mod replication;

pub(crate) use replication::{PrefixDecision, rejected_next, validate_prefix};

pub(crate) use commit::quorum_commit;
pub(crate) use election::is_log_up_to_date;
