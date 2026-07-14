//! Raft state-transition helpers.

mod election;

pub(crate) use election::is_log_up_to_date;
