//! Current-term quorum commit calculation.

use std::collections::BTreeMap;

use crate::{LogError, LogIndex, NodeId, Progress, RaftLog, Term};

/// Returns the newly committable index, if a current-term majority has one.
pub(crate) fn quorum_commit<C>(
    log: &RaftLog<C>,
    progress: &BTreeMap<NodeId, Progress>,
    quorum: usize,
    current_term: Term,
) -> Result<Option<LogIndex>, LogError> {
    let mut matched: Vec<_> = progress.values().map(Progress::match_index).collect();
    matched.push(log.last_index());
    matched.sort_unstable();
    let candidate = matched[matched.len() - quorum];
    if candidate > log.committed_index() && log.term(candidate)? == current_term {
        Ok(Some(candidate))
    } else {
        Ok(None)
    }
}
