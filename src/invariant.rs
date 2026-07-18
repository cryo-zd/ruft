//! Internal validation of Raft safety properties.

use crate::{HardState, InvariantViolation, LogIndex, RaftLog, Term};

/// Verifies invariants that must hold after every accepted state transition.
pub(crate) fn validate<C>(
    hard_state: &HardState,
    log: &RaftLog<C>,
    applied_index: LogIndex,
    last_log_index: LogIndex,
    last_log_term: Term,
) -> Result<(), InvariantViolation> {
    let commit_index = hard_state.commit_index();
    if applied_index > commit_index {
        return Err(InvariantViolation::AppliedPastCommit);
    }
    if commit_index > log.last_index() {
        return Err(InvariantViolation::CommitPastLog);
    }
    if log.committed_index() != commit_index {
        return Err(InvariantViolation::CommitIndexMismatch);
    }
    let committed_next = commit_index
        .checked_next()
        .map_err(|_| InvariantViolation::IndexOverflow)?;
    if log.first_index() > committed_next {
        return Err(InvariantViolation::CommittedGap);
    }
    if last_log_index != log.last_index()
        || last_log_term
            != log
                .term(log.last_index())
                .map_err(InvariantViolation::Log)?
    {
        return Err(InvariantViolation::LastLogMismatch);
    }
    log.validate()
}
