//! Follower-side AppendEntries prefix validation and conflict classification.
//!
//! When a follower receives an AppendEntries RPC, it must verify that the
//! leader's log prefix matches its own at `prev_log_index`. This module
//! classifies the result and produces a [`ConflictHint`] that lets the leader
//! skip entire conflicting terms rather than decrementing `next_index` one
//! entry at a time (Raft paper §5.3 conflict optimization).

use crate::{ConflictHint, LogError, LogIndex, RaftLog, Term};

/// The result of validating an AppendEntries previous-log proof.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PrefixDecision {
    /// The follower's log matches the leader's prefix at `prev_log_index`.
    /// The follower can safely merge the supplied entries.
    Match,
    /// The follower's log disagrees at `prev_log_index`. The supplied
    /// [`ConflictHint`] tells the leader where to probe next.
    Reject(ConflictHint),
}

/// Validates a leader previous-log proof against a follower log.
///
/// Four cases:
///
/// 1. **Match**: the term at `prev_log_index` matches `prev_log_term` —
///    the follower can merge the leader's entries.
///
/// 2. **Conflict, term known**: the follower has an entry at `prev_log_index`
///    but with a different term. The hint returns the *first* index of that
///    conflicting term so the leader can skip the entire term range.
///
/// 3. **Compacted**: `prev_log_index` is below the follower's snapshot
///    boundary. The hint points to the follower's first available index.
///    No term hint is available (the term at that index is in the snapshot).
///
/// 4. **Unavailable**: `prev_log_index` is beyond the follower's log. The
///    hint points to `last_index + 1` to tell the leader to back up further.
pub(crate) fn validate_prefix<C>(
    log: &RaftLog<C>,
    prev_log_index: LogIndex,
    prev_log_term: Term,
) -> Result<PrefixDecision, LogError> {
    match log.term(prev_log_index) {
        // Case 1: terms match — the log prefix is consistent.
        Ok(term) if term == prev_log_term => Ok(PrefixDecision::Match),
        // Case 2: conflicting term at an existing entry. Return the first
        // index of the conflicting term so the leader can skip all entries
        // of that term (Raft's conflict-range optimization).
        Ok(term) => Ok(PrefixDecision::Reject(ConflictHint::new(
            log.first_index_of_term(term).unwrap_or(prev_log_index),
            Some(term),
        ))),
        // Case 3: the requested index has been compacted into a snapshot.
        // The leader is too far behind; it must send a snapshot instead.
        Err(LogError::Compacted { .. }) => Ok(PrefixDecision::Reject(ConflictHint::new(
            log.first_index(),
            None,
        ))),
        // Case 4: the requested index is beyond the follower's log. Point
        // the leader to last_index + 1 so it backs up.
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

/// Chooses the next leader probe index after a follower rejection.
///
/// If the conflict hint includes a term and the leader also has entries from
/// that term, skip to right after the last entry of that term. This avoids
/// probing each index individually — the leader jumps past the entire
/// conflicting term range in one step. Otherwise, fall back to the hint's
/// index directly.
pub(crate) fn rejected_next<C>(log: &RaftLog<C>, hint: ConflictHint) -> Result<LogIndex, LogError> {
    if let Some(term) = hint.term {
        // The follower told us which term conflicted. If we also have entries
        // from that term, skip past all of them at once.
        if let Some(index) = log.last_index_of_term(term) {
            return index
                .checked_next()
                .map_err(|_| LogError::IndexOverflow { at: index });
        }
    }
    // No term hint or the leader doesn't share that term. Fall back to the
    // raw index from the hint.
    Ok(hint.index)
}
