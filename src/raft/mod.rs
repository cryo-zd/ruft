//! Raft state-transition helpers.

mod election;
mod replication;

pub(crate) use replication::{PrefixDecision, validate_prefix};

pub(crate) use election::is_log_up_to_date;
