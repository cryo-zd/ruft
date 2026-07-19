//! Election-specific ordering rules.
//!
//! Raft's leader completeness property (Figure 3) guarantees that a leader
//! possesses every committed entry from prior terms. It enforces this by
//! requiring voters to compare logs: a voter grants its vote only if the
//! candidate's log is at least as up-to-date as its own.

use crate::{LogIndex, Term};

/// Returns whether a candidate log is at least as up to date as the local log.
///
/// Implements the Raft leader completeness property: the voter grants its vote
/// only if the candidate's log is at least as up-to-date as the voter's.
///
/// The comparison is lexicographic on `(last_term, last_index)`: the term
/// dominates, and the index only breaks a term tie. This ordering prevents a
/// candidate that is missing a newer-term committed entry from winning an
/// election — a node with a higher last term has strictly more information
/// about committed entries.
pub(crate) fn is_log_up_to_date(
    candidate_index: LogIndex,
    candidate_term: Term,
    local_index: LogIndex,
    local_term: Term,
) -> bool {
    candidate_term > local_term || (candidate_term == local_term && candidate_index >= local_index)
}
