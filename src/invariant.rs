//! Internal validation of Raft safety properties.
//!
//! After every successful state transition, [`RaftCore::step`] calls
//! [`validate`] to verify that the in-memory state still satisfies Raft's
//! structural invariants. A violation indicates a correctness bug and is
//! treated as a fatal error — the core stops immediately rather than
//! risking an unsafe continuation.

use crate::{HardState, InvariantViolation, LogIndex, RaftLog, Term};

/// Verifies invariants that must hold after every accepted state transition.
///
/// Each check corresponds to a Raft safety property:
///
/// 1. **Applied ≤ Committed**: the state machine must never apply entries
///    that haven't been committed. Violation means uncommitted data became
///    externally visible.
///
/// 2. **Commit ≤ Last Log**: the commit index must point to an entry that
///    exists. Violation means the core tried to commit a fabricated index.
///
/// 3. **Durable commit = logical commit**: the hard state and RaftLog must
///    agree on the commit index. Violation means a persist was lost or
///    applied out of order.
///
/// 4. **No gap in committed prefix**: every index from `first_index` through
///    `commit_index` must be present. Violation means a snapshot or
///    compaction operation left a hole.
///
/// 5. **Last-log cache matches log**: `last_log_index` and `last_log_term`
///    are cached for fast access in vote comparisons and append logic. They
///    must be consistent with the actual log contents.
///
/// 6. **Log suffix is valid**: entries form a continuous sequence with
///    non-decreasing terms.
pub(crate) fn validate<C>(
    hard_state: &HardState,
    log: &RaftLog<C>,
    applied_index: LogIndex,
    last_log_index: LogIndex,
    last_log_term: Term,
) -> Result<(), InvariantViolation> {
    let commit_index = hard_state.commit_index();

    // (1) Applied must not exceed committed — the state machine must not
    // see uncommitted entries.
    if applied_index > commit_index {
        return Err(InvariantViolation::AppliedPastCommit);
    }
    // (2) Commit must not exceed the available log.
    if commit_index > log.last_index() {
        return Err(InvariantViolation::CommitPastLog);
    }
    // (3) Durable and in-memory commit must agree.
    if log.committed_index() != commit_index {
        return Err(InvariantViolation::CommitIndexMismatch);
    }
    // (4) The committed prefix must be gap-free. If first_index >
    // commit_index + 1, a snapshot or compaction left a hole.
    let committed_next = commit_index
        .checked_next()
        .map_err(|_| InvariantViolation::IndexOverflow)?;
    if log.first_index() > committed_next {
        return Err(InvariantViolation::CommittedGap);
    }
    // (5) Cached last-log metadata must match the actual log.
    if last_log_index != log.last_index()
        || last_log_term
            != log
                .term(log.last_index())
                .map_err(InvariantViolation::Log)?
    {
        return Err(InvariantViolation::LastLogMismatch);
    }
    // (6) Log suffix must be continuous and term-monotonic.
    log.validate()
}
