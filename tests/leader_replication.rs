use ruft::{
    ClusterId, Config, ConflictHint, Effect, EffectOutcome, Entry, Envelope, Event, HardState,
    LogIndex, Message, NodeId, ProgressState, RaftCore, RecoveredState, Role, Term, TickKind,
};

fn entry(index: u64, term: u64, bytes: usize) -> Entry<String> {
    Entry::command(
        LogIndex::new(index),
        Term::new(term),
        format!("{index}"),
        bytes,
    )
    .unwrap()
}

fn leader() -> RaftCore<String> {
    let config = Config::builder(
        NodeId::new(1),
        [NodeId::new(1), NodeId::new(2), NodeId::new(3)],
    )
    .cluster_id(ClusterId::new(9))
    .heartbeat_ticks(2)
    .election_ticks(10..=20)
    .check_quorum_ticks(10)
    .max_entries_per_rpc(1)
    .max_bytes_per_rpc(8)
    .max_inflight_appends(2)
    .build()
    .unwrap();
    let recovered = RecoveredState::new(
        HardState::new(Term::new(2), None, Default::default()),
        None,
        vec![entry(1, 1, 2), entry(2, 1, 2), entry(3, 2, 2)],
    )
    .unwrap();
    RaftCore::new(config, recovered).unwrap()
}

fn receive(
    core: &mut RaftCore<String>,
    from: u64,
    message: Message<String>,
) -> Vec<Effect<String>> {
    core.step(Event::MessageReceived(Envelope::new(
        ClusterId::new(9),
        NodeId::new(from),
        NodeId::new(1),
        message,
    )))
    .unwrap()
    .effects
}

fn elected_leader() -> RaftCore<String> {
    let mut core = leader();
    core.step(Event::Tick(TickKind::Election)).unwrap();
    let effects = receive(
        &mut core,
        2,
        Message::PreVoteResponse {
            term: Term::new(1),
            granted: true,
        },
    );
    let vote_id = match effects.as_slice() {
        [Effect::Persist { id, .. }] => *id,
        other => panic!("expected vote persistence, got {other:?}"),
    };
    core.step(Event::EffectCompleted {
        id: vote_id,
        outcome: EffectOutcome::Persisted,
    })
    .unwrap();
    let effects = receive(
        &mut core,
        2,
        Message::VoteResponse {
            term: Term::new(3),
            granted: true,
        },
    );
    let noop_id = match effects.as_slice() {
        [Effect::Persist { id, .. }] => *id,
        other => panic!("expected no-op persistence, got {other:?}"),
    };
    let effects = core
        .step(Event::EffectCompleted {
            id: noop_id,
            outcome: EffectOutcome::Persisted,
        })
        .unwrap()
        .effects;
    assert_eq!(core.status().role(), Role::Leader);
    assert_eq!(effects.len(), 2);
    core
}

#[test]
fn successful_probe_enters_replicate_and_respects_window() {
    let mut core = elected_leader();
    let progress = core.progress(NodeId::new(2)).unwrap();
    assert_eq!(progress.state(), ProgressState::Probe);
    assert_eq!(progress.inflight_count(), 1);

    let effects = receive(
        &mut core,
        2,
        Message::AppendEntriesResponse {
            term: Term::new(3),
            success: false,
            match_index: LogIndex::new(4),
            conflict: Some(ConflictHint::new(LogIndex::new(1), None)),
        },
    );
    assert!(
        matches!(effects.as_slice(), [Effect::SendMessage { message: Message::AppendEntries { prev_log_index: LogIndex(0), entries, .. }, .. }] if entries.len() == 1 && entries[0].index() == LogIndex::new(1))
    );

    let effects = receive(
        &mut core,
        2,
        Message::AppendEntriesResponse {
            term: Term::new(3),
            success: true,
            match_index: LogIndex::new(1),
            conflict: None,
        },
    );
    assert_eq!(
        core.progress(NodeId::new(2)).unwrap().state(),
        ProgressState::Replicate
    );
    assert!(
        matches!(effects.as_slice(), [Effect::SendMessage { message: Message::AppendEntries { prev_log_index: LogIndex(1), entries, .. }, .. }] if entries.len() == 1 && entries[0].index() == LogIndex::new(2))
    );

    let effects = core.step(Event::Tick(TickKind::Heartbeat)).unwrap().effects;
    assert_eq!(effects.len(), 1);
    assert_eq!(core.progress(NodeId::new(2)).unwrap().inflight_count(), 2);
    assert!(
        core.step(Event::Tick(TickKind::Heartbeat))
            .unwrap()
            .effects
            .is_empty()
    );
}

#[test]
fn conflict_term_hint_skips_to_the_last_leader_index_for_that_term() {
    let mut core = elected_leader();
    let effects = receive(
        &mut core,
        2,
        Message::AppendEntriesResponse {
            term: Term::new(3),
            success: false,
            match_index: LogIndex::new(4),
            conflict: Some(ConflictHint::new(LogIndex::new(1), Some(Term::new(1)))),
        },
    );
    assert!(
        matches!(effects.as_slice(), [Effect::SendMessage { message: Message::AppendEntries { prev_log_index: LogIndex(2), entries, .. }, .. }] if entries[0].index() == LogIndex::new(3))
    );
}

#[test]
fn stale_rejection_and_out_of_order_success_do_not_regress_progress() {
    let mut core = elected_leader();
    receive(
        &mut core,
        2,
        Message::AppendEntriesResponse {
            term: Term::new(3),
            success: false,
            match_index: LogIndex::new(4),
            conflict: Some(ConflictHint::new(LogIndex::new(1), None)),
        },
    );
    receive(
        &mut core,
        2,
        Message::AppendEntriesResponse {
            term: Term::new(3),
            success: true,
            match_index: LogIndex::new(1),
            conflict: None,
        },
    );
    let next = core.progress(NodeId::new(2)).unwrap().next_index();
    assert!(
        receive(
            &mut core,
            2,
            Message::AppendEntriesResponse {
                term: Term::new(3),
                success: false,
                match_index: LogIndex::new(0),
                conflict: Some(ConflictHint::new(LogIndex::new(1), None)),
            }
        )
        .is_empty()
    );
    receive(
        &mut core,
        2,
        Message::AppendEntriesResponse {
            term: Term::new(3),
            success: true,
            match_index: LogIndex::new(0),
            conflict: None,
        },
    );
    assert_eq!(core.progress(NodeId::new(2)).unwrap().next_index(), next);
}
