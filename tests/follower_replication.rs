use ruft::{
    ClusterId, Config, Effect, EffectOutcome, Entry, Envelope, Event, HardState, LogIndex, Message,
    NodeId, RaftCore, RecoveredState, SnapshotDigest, SnapshotId, SnapshotMetadata, SnapshotRecord,
    SnapshotRef, Term,
};

fn config() -> Config {
    Config::builder(
        NodeId::new(2),
        [NodeId::new(1), NodeId::new(2), NodeId::new(3)],
    )
    .cluster_id(ClusterId::new(9))
    .heartbeat_ticks(2)
    .election_ticks(10..=20)
    .check_quorum_ticks(10)
    .build()
    .unwrap()
}

fn entry(index: u64, term: u64) -> Entry<String> {
    Entry::command(LogIndex::new(index), Term::new(term), format!("{index}"), 1).unwrap()
}

fn follower(entries: Vec<Entry<String>>, commit: u64) -> RaftCore<String> {
    let current_term = entries
        .iter()
        .map(Entry::term)
        .max()
        .unwrap_or(Term::new(1));
    let recovered = RecoveredState::new(
        HardState::new(current_term, None, LogIndex::new(commit)),
        None,
        entries,
    )
    .unwrap();
    RaftCore::new(config(), recovered).unwrap()
}

fn append(
    core: &mut RaftCore<String>,
    term: u64,
    previous_index: u64,
    previous_term: u64,
    leader_commit: u64,
    entries: Vec<Entry<String>>,
) -> Vec<Effect<String>> {
    core.step(Event::MessageReceived(Envelope::new(
        ClusterId::new(9),
        NodeId::new(1),
        NodeId::new(2),
        Message::AppendEntries {
            term: Term::new(term),
            prev_log_index: LogIndex::new(previous_index),
            prev_log_term: Term::new(previous_term),
            leader_commit: LogIndex::new(leader_commit),
            entries,
        },
    )))
    .unwrap()
    .effects
}

fn persist_id(effects: &[Effect<String>]) -> ruft::EffectId {
    match effects {
        [Effect::Persist { id, .. }] => *id,
        other => panic!("expected exactly one persistence effect, got {other:?}"),
    }
}

#[test]
fn follower_replies_success_only_after_entries_are_durable() {
    let mut core = follower(vec![entry(1, 1)], 0);
    let effects = append(&mut core, 1, 1, 1, 0, vec![entry(2, 1)]);
    let id = persist_id(&effects);
    assert!(
        matches!(effects.as_slice(), [Effect::Persist { batch, .. }] if batch.entries.iter().map(Entry::index).eq([LogIndex::new(2)]))
    );

    let effects = core
        .step(Event::EffectCompleted {
            id,
            outcome: EffectOutcome::Persisted,
        })
        .unwrap()
        .effects;
    assert!(
        matches!(effects.as_slice(), [Effect::SendMessage { to, message: Message::AppendEntriesResponse { term: Term(1), success: true, conflict: None, .. } }] if *to == NodeId::new(1))
    );
}

#[test]
fn missing_previous_index_returns_next_probe_hint_without_persistence() {
    let mut core = follower(vec![entry(1, 1)], 0);
    let effects = append(&mut core, 1, 3, 1, 0, Vec::new());
    assert!(
        matches!(effects.as_slice(), [Effect::SendMessage { message: Message::AppendEntriesResponse { success: false, conflict: Some(hint), .. }, .. }] if hint.index == LogIndex::new(2) && hint.term.is_none())
    );
}

#[test]
fn wrong_previous_term_returns_first_index_for_the_conflicting_term() {
    let mut core = follower(vec![entry(1, 1), entry(2, 2), entry(3, 2)], 0);
    let effects = append(&mut core, 2, 3, 1, 0, Vec::new());
    assert!(
        matches!(effects.as_slice(), [Effect::SendMessage { message: Message::AppendEntriesResponse { success: false, conflict: Some(hint), .. }, .. }] if hint.index == LogIndex::new(2) && hint.term == Some(Term::new(2)))
    );
}

#[test]
fn duplicate_append_succeeds_without_a_redundant_persist() {
    let mut core = follower(vec![entry(1, 1), entry(2, 1)], 0);
    let effects = append(&mut core, 1, 1, 1, 0, vec![entry(2, 1)]);
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendMessage {
            message: Message::AppendEntriesResponse {
                success: true,
                conflict: None,
                ..
            },
            ..
        }]
    ));
}

#[test]
fn conflict_replaces_only_the_uncommitted_suffix() {
    let mut core = follower(vec![entry(1, 1), entry(2, 1), entry(3, 2)], 1);
    let effects = append(&mut core, 2, 1, 1, 0, vec![entry(2, 2), entry(3, 2)]);
    assert!(
        matches!(effects.as_slice(), [Effect::Persist { batch, .. }] if batch.entries.iter().map(Entry::index).eq([LogIndex::new(2), LogIndex::new(3)]))
    );
}

#[test]
fn heartbeat_persists_commit_advance_before_success_response() {
    let mut core = follower(vec![entry(1, 1), entry(2, 1)], 0);
    let effects = append(&mut core, 1, 2, 1, 2, Vec::new());
    let id = match effects.as_slice() {
        [Effect::Persist { id, batch }] => {
            assert_eq!(
                batch.hard_state.as_ref().unwrap().commit_index(),
                LogIndex::new(2)
            );
            *id
        }
        other => panic!("expected commit persistence, got {other:?}"),
    };
    let effects = core
        .step(Event::EffectCompleted {
            id,
            outcome: EffectOutcome::Persisted,
        })
        .unwrap()
        .effects;
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendMessage {
            message: Message::AppendEntriesResponse { success: true, .. },
            ..
        }]
    ));
}

#[test]
fn committed_prefix_conflict_is_rejected_as_a_log_invariant_error() {
    let mut core = follower(vec![entry(1, 1), entry(2, 1)], 2);
    let result = core.step(Event::MessageReceived(Envelope::new(
        ClusterId::new(9),
        NodeId::new(1),
        NodeId::new(2),
        Message::AppendEntries {
            term: Term::new(2),
            prev_log_index: LogIndex::new(0),
            prev_log_term: Term::new(0),
            leader_commit: LogIndex::new(0),
            entries: vec![entry(1, 2)],
        },
    )));
    assert!(matches!(
        result,
        Err(ruft::StepError::Log(
            ruft::LogError::WouldTruncateCommitted { .. }
        ))
    ));
}

#[test]
fn compacted_prefix_returns_the_first_available_index() {
    let metadata = SnapshotMetadata::new(
        SnapshotId::new(7),
        LogIndex::new(3),
        Term::new(1),
        vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
        0,
        SnapshotDigest::new([0; 32]),
    )
    .unwrap();
    let recovered = RecoveredState::new(
        HardState::new(Term::new(1), None, LogIndex::new(3)),
        Some(SnapshotRecord::new(metadata, SnapshotRef::new(vec![1]))),
        Vec::<Entry<String>>::new(),
    )
    .unwrap();
    let mut core = RaftCore::new(config(), recovered).unwrap();
    let effects = append(&mut core, 1, 1, 1, 3, Vec::new());
    assert!(
        matches!(effects.as_slice(), [Effect::SendMessage { message: Message::AppendEntriesResponse { success: false, conflict: Some(hint), .. }, .. }] if hint.index == LogIndex::new(4) && hint.term.is_none())
    );
}

#[test]
fn stale_leader_term_is_rejected_without_persistence() {
    let mut core = follower(vec![entry(1, 2)], 0);
    let effects = append(&mut core, 1, 1, 2, 0, Vec::new());
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendMessage {
            message: Message::AppendEntriesResponse {
                term: Term(2),
                success: false,
                conflict: None,
                ..
            },
            ..
        }]
    ));
}

#[test]
fn higher_term_rejection_persists_the_new_term_before_replying() {
    let mut core = follower(vec![entry(1, 1)], 0);
    let effects = append(&mut core, 3, 2, 1, 0, Vec::new());
    let id = match effects.as_slice() {
        [Effect::Persist { id, batch }] => {
            assert_eq!(
                batch.hard_state.as_ref().unwrap().current_term(),
                Term::new(3)
            );
            *id
        }
        other => panic!("expected higher term persistence, got {other:?}"),
    };
    let effects = core
        .step(Event::EffectCompleted {
            id,
            outcome: EffectOutcome::Persisted,
        })
        .unwrap()
        .effects;
    assert!(matches!(
        effects.as_slice(),
        [Effect::SendMessage {
            message: Message::AppendEntriesResponse {
                term: Term(3),
                success: false,
                ..
            },
            ..
        }]
    ));
}
