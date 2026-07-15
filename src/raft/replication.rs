//! Follower-side AppendEntries prefix validation and conflict classification.

use crate::{ConflictHint, LogError, LogIndex, RaftLog, Term};

/// The result of validating an AppendEntries previous-log proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PrefixDecision {
    /// The follower can merge the supplied entries.
    Match,
    /// The leader must retry from the supplied conflict hint.
    Reject(ConflictHint),
}

/// Validates a leader previous-log proof against a follower log.
pub(crate) fn validate_prefix<C>(
    log: &RaftLog<C>,
    prev_log_index: LogIndex,
    prev_log_term: Term,
) -> Result<PrefixDecision, LogError> {
    match log.term(prev_log_index) {
        Ok(term) if term == prev_log_term => Ok(PrefixDecision::Match),
        Ok(term) => Ok(PrefixDecision::Reject(ConflictHint::new(
            log.first_index_of_term(term).unwrap_or(prev_log_index),
            Some(term),
        ))),
        Err(LogError::Compacted { .. }) => Ok(PrefixDecision::Reject(ConflictHint::new(
            log.first_index(),
            None,
        ))),
        Err(LogError::Unavailable { .. }) => Ok(PrefixDecision::Reject(ConflictHint::new(
            log.last_index()
                .checked_next()
                .map_err(|_| LogError::IndexOverflow {
                    at: log.last_index(),
                })?,
            None,
        ))),
        Err(error) => Err(error),
    }
}
