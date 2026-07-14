//! Election-specific ordering rules.

use crate::{LogIndex, Term};

/// Returns whether a candidate log is at least as up to date as the local log.
///
/// Raft compares the final term first and uses the final index only to break a
/// term tie. This prevents a candidate missing a newer-term entry from winning.
pub(crate) fn is_log_up_to_date(
    candidate_index: LogIndex,
    candidate_term: Term,
    local_index: LogIndex,
    local_term: Term,
) -> bool {
    candidate_term > local_term || (candidate_term == local_term && candidate_index >= local_index)
}
