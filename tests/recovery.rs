use ruft::{
    Entry, HardState, LogError, LogIndex, NodeId, RaftLog, RecoveredState, RecoveryError,
    SnapshotDigest, SnapshotId, SnapshotMetadata, SnapshotRecord, SnapshotRef, Term,
};

fn command(index: u64, term: u64, value: &str) -> Entry<String> {
    Entry::command(
        LogIndex::new(index),
        Term::new(term),
        value.to_owned(),
        value.len(),
    )
    .unwrap()
}

fn hard_state(term: u64, voted_for: Option<u64>, commit_index: u64) -> HardState {
    HardState::new(
        Term::new(term),
        voted_for.map(NodeId::new),
        LogIndex::new(commit_index),
    )
}

fn snapshot(index: u64, term: u64) -> SnapshotRecord {
    let metadata = SnapshotMetadata::new(
        SnapshotId::new(1),
        LogIndex::new(index),
        Term::new(term),
        vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
        0,
        SnapshotDigest::new([0; 32]),
    )
    .unwrap();
    SnapshotRecord::new(metadata, SnapshotRef::new(b"snapshot/1".to_vec()))
}

#[test]
fn recovered_entries_must_start_after_snapshot_and_be_continuous() {
    let recovered = RecoveredState::new(
        hard_state(3, Some(2), 6),
        Some(snapshot(5, 2)),
        vec![command(6, 3, "six"), command(8, 3, "eight")],
    );

    assert!(matches!(
        recovered,
        Err(RecoveryError::LogGap {
            expected: LogIndex(7),
            actual: LogIndex(8),
        })
    ));
}

#[test]
fn recovered_entries_must_begin_immediately_after_a_snapshot() {
    let recovered = RecoveredState::new(
        hard_state(3, None, 7),
        Some(snapshot(5, 2)),
        vec![command(7, 3, "seven")],
    );

    assert!(matches!(
        recovered,
        Err(RecoveryError::LogGap {
            expected: LogIndex(6),
            actual: LogIndex(7),
        })
    ));
}

#[test]
fn commit_must_be_available_in_snapshot_or_log() {
    let recovered = RecoveredState::new(hard_state(3, None, 9), None, vec![command(1, 3, "one")]);

    assert!(matches!(
        recovered,
        Err(RecoveryError::CommitPastLog {
            commit: LogIndex(9),
            last: LogIndex(1),
        })
    ));
}

#[test]
fn recovery_rejects_term_regression_and_current_term_behind_log() {
    let regression = RecoveredState::new(
        hard_state(4, None, 2),
        None,
        vec![command(1, 3, "one"), command(2, 2, "two")],
    );
    assert!(matches!(
        regression,
        Err(RecoveryError::TermRegression {
            index: LogIndex(2),
            previous: Term(3),
            actual: Term(2),
        })
    ));

    let stale_hard_state = RecoveredState::new(
        hard_state(2, None, 3),
        None,
        vec![
            command(1, 2, "one"),
            command(2, 3, "two"),
            command(3, 3, "three"),
        ],
    );
    assert!(matches!(
        stale_hard_state,
        Err(RecoveryError::CurrentTermBehindLog {
            current: Term(2),
            observed: Term(3),
        })
    ));
}

#[test]
fn snapshot_boundary_provides_the_compacted_term() {
    let recovered = RecoveredState::new(
        hard_state(3, Some(1), 6),
        Some(snapshot(5, 2)),
        vec![command(6, 3, "six")],
    )
    .unwrap();
    let log = RaftLog::from_recovered(&recovered);

    assert_eq!(log.term(LogIndex::new(5)), Ok(Term::new(2)));
    assert!(matches!(
        log.term(LogIndex::new(4)),
        Err(LogError::Compacted {
            index: LogIndex(4),
            snapshot_index: LogIndex(5),
        })
    ));
    assert_eq!(
        log.entries(LogIndex::new(6)..=LogIndex::new(6))
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn replacement_never_truncates_a_committed_entry() {
    let recovered = RecoveredState::new(
        hard_state(3, Some(1), 3),
        None,
        vec![
            command(1, 1, "one"),
            command(2, 1, "two"),
            command(3, 2, "three"),
            command(4, 2, "four"),
        ],
    )
    .unwrap();
    let mut log = RaftLog::from_recovered(&recovered);

    let error = log
        .replace_conflict(vec![command(3, 3, "replacement")])
        .unwrap_err();

    assert_eq!(
        error,
        LogError::WouldTruncateCommitted {
            from: LogIndex(3),
            committed: LogIndex(3),
        }
    );
}

#[test]
fn replacement_rewrites_only_the_uncommitted_suffix() {
    let recovered = RecoveredState::new(
        hard_state(3, Some(1), 2),
        None,
        vec![
            command(1, 1, "one"),
            command(2, 1, "two"),
            command(3, 2, "three"),
            command(4, 2, "four"),
        ],
    )
    .unwrap();
    let mut log = RaftLog::from_recovered(&recovered);

    log.replace_conflict(vec![command(3, 3, "new-three"), command(4, 3, "new-four")])
        .unwrap();

    assert_eq!(log.term(LogIndex::new(2)), Ok(Term::new(1)));
    assert_eq!(log.term(LogIndex::new(3)), Ok(Term::new(3)));
    assert_eq!(log.term(LogIndex::new(4)), Ok(Term::new(3)));
    assert_eq!(log.unstable_from(), Some(LogIndex::new(3)));
}

#[test]
fn compaction_keeps_the_snapshot_term_and_retains_the_later_suffix() {
    let recovered = RecoveredState::new(
        hard_state(2, Some(1), 3),
        None,
        vec![
            command(1, 1, "one"),
            command(2, 1, "two"),
            command(3, 2, "three"),
        ],
    )
    .unwrap();
    let mut log = RaftLog::from_recovered(&recovered);

    log.compact(snapshot(2, 1)).unwrap();

    assert!(matches!(
        log.term(LogIndex::new(1)),
        Err(LogError::Compacted {
            snapshot_index: LogIndex(2),
            ..
        })
    ));
    assert_eq!(log.term(LogIndex::new(2)), Ok(Term::new(1)));
    assert_eq!(log.term(LogIndex::new(3)), Ok(Term::new(2)));
}

#[test]
fn append_and_stability_confirmation_track_the_unstable_suffix() {
    let recovered = RecoveredState::new(
        hard_state(2, Some(1), 2),
        None,
        vec![command(1, 1, "one"), command(2, 2, "two")],
    )
    .unwrap();
    let mut log = RaftLog::from_recovered(&recovered);

    log.append(vec![command(3, 2, "three")]).unwrap();
    assert_eq!(log.unstable_from(), Some(LogIndex::new(3)));
    log.mark_stable(LogIndex::new(2)).unwrap();
    assert_eq!(log.unstable_from(), Some(LogIndex::new(3)));
    log.mark_stable(LogIndex::new(3)).unwrap();
    assert_eq!(log.unstable_from(), None);
}

#[test]
fn recovery_rejects_unknown_format_before_using_durable_data() {
    let recovered = RecoveredState::<String>::from_parts(
        RecoveredState::<String>::FORMAT_VERSION + 1,
        hard_state(0, None, 0),
        None,
        Vec::new(),
    );

    assert!(matches!(
        recovered,
        Err(RecoveryError::UnsupportedFormat {
            found: 2,
            supported: 1,
        })
    ));
}

#[test]
fn entry_clone_shares_a_non_clone_command() {
    #[derive(Debug, Eq, PartialEq)]
    struct NonCloneCommand(String);

    let entry = Entry::command(
        LogIndex::new(1),
        Term::new(1),
        NonCloneCommand("command".to_owned()),
        7,
    )
    .unwrap();
    let clone = entry.clone();

    assert_eq!(entry.index(), clone.index());
    assert_eq!(entry.term(), clone.term());
}

#[test]
fn partial_stability_confirmation_keeps_the_remaining_unstable_entries() {
    let recovered = RecoveredState::new(
        hard_state(2, Some(1), 2),
        None,
        vec![command(1, 1, "one"), command(2, 2, "two")],
    )
    .unwrap();
    let mut log = RaftLog::from_recovered(&recovered);

    log.append(vec![command(3, 2, "three")]).unwrap();
    log.append(vec![command(4, 2, "four")]).unwrap();
    log.mark_stable(LogIndex::new(3)).unwrap();

    assert_eq!(log.unstable_from(), Some(LogIndex::new(4)));
}

#[test]
fn replacement_that_skips_the_local_tail_reports_a_gap() {
    let recovered = RecoveredState::new(
        hard_state(2, Some(1), 2),
        None,
        vec![command(1, 1, "one"), command(2, 2, "two")],
    )
    .unwrap();
    let mut log = RaftLog::from_recovered(&recovered);

    let error = log
        .replace_conflict(vec![command(4, 2, "four")])
        .unwrap_err();

    assert_eq!(
        error,
        LogError::NonContiguousEntries {
            expected: LogIndex::new(3),
            actual: LogIndex::new(4),
        }
    );
}

#[test]
fn partial_stability_confirmation_preserves_the_tail_of_one_append_batch() {
    let recovered = RecoveredState::new(
        hard_state(2, Some(1), 2),
        None,
        vec![command(1, 1, "one"), command(2, 2, "two")],
    )
    .unwrap();
    let mut log = RaftLog::from_recovered(&recovered);

    log.append(vec![command(3, 2, "three"), command(4, 2, "four")])
        .unwrap();
    log.mark_stable(LogIndex::new(3)).unwrap();

    assert_eq!(log.unstable_from(), Some(LogIndex::new(4)));
}

#[test]
fn replacement_discards_the_replaced_unstable_tail() {
    let recovered = RecoveredState::new(
        hard_state(2, Some(1), 2),
        None,
        vec![command(1, 1, "one"), command(2, 2, "two")],
    )
    .unwrap();
    let mut log = RaftLog::from_recovered(&recovered);

    log.append(vec![
        command(3, 2, "three"),
        command(4, 2, "four"),
        command(5, 2, "five"),
    ])
    .unwrap();
    log.replace_conflict(vec![command(4, 3, "new-four"), command(5, 3, "new-five")])
        .unwrap();
    log.mark_stable(LogIndex::new(3)).unwrap();

    assert_eq!(log.unstable_from(), Some(LogIndex::new(4)));
    log.mark_stable(LogIndex::new(5)).unwrap();
    assert_eq!(log.unstable_from(), None);
}
