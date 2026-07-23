//! Current-term quorum commit calculation.
//!
//! Raft's commit rule (§5.4.2): a leader may commit an entry only if (a) the
//! entry is stored on a quorum of servers, and (b) at least one entry from the
//! leader's current term is also stored on that quorum. This prevents a leader
//! from concluding that a prior-term entry is committed when it actually isn't
//! (Figure 8 scenario).

use std::collections::BTreeMap;

use crate::{LogError, LogIndex, NodeId, Progress, RaftLog, Term};

/// Returns the newly committable index, if a current-term majority has one.
///
/// Collects every follower's [`Progress::match_index`] plus the leader's own
/// `last_index`, sorts them, and takes the element at position `n - quorum`
/// (the Nth largest, i.e., the index with at least `quorum` elements ≥ it).
///
/// An index only commits if its term equals the current term. Entries from
/// prior terms never commit directly — they commit indirectly when a
/// current-term entry that follows them commits (log matching property
/// guarantees all prior entries are also replicated).
pub(crate) fn quorum_commit<C>(
    log: &RaftLog<C>,
    progress: &BTreeMap<NodeId, Progress>,
    quorum: usize,
    current_term: Term,
) -> Result<Option<LogIndex>, LogError> {
    let mut matched: Vec<_> = progress.values().map(Progress::match_index).collect();
    // The leader's own log counts toward the quorum.
    matched.push(log.last_index());
    matched.sort_unstable();
    // The element at position (n - quorum) has at least `quorum` elements ≥ it.
    let candidate = matched[matched.len() - quorum];
    // Raft's commit restriction: only commit entries from the current term.
    // This prevents the Figure 8 scenario where a prior-term entry appears
    // committed but can still be overwritten by a future leader.
    if candidate > log.committed_index() && log.term(candidate)? == current_term {
        Ok(Some(candidate))
    } else {
        Ok(None)
    }
}
