use ruft::{
    ClusterId, Config, Effect, EffectOutcome, Event, FatalError, HardState, InputError, NodeId,
    ProposalId, RaftCore, RecoveredState, StepError, StoppedReason, Term, TickKind,
};

fn core() -> RaftCore<String> {
    let config = Config::builder(NodeId::new(1), [NodeId::new(1)])
        .cluster_id(ClusterId::new(9))
        .heartbeat_ticks(2)
        .election_ticks(10..=20)
        .check_quorum_ticks(10)
        .build()
        .unwrap();
    let recovered = RecoveredState::new(
        HardState::new(Term::new(0), None, Default::default()),
        None,
        Vec::new(),
    )
    .unwrap();
    RaftCore::new(config, recovered).unwrap()
}

#[test]
fn storage_failure_stops_protocol_work_and_client_admission() {
    let mut core = core();
    let effects = core.step(Event::Tick(TickKind::Election)).unwrap().effects;
    let persist_id = match effects.as_slice() {
        [Effect::Persist { id, .. }] => *id,
        other => panic!("expected self-vote persist, got {other:?}"),
    };

    assert!(matches!(
        core.step(Event::EffectCompleted {
            id: persist_id,
            outcome: EffectOutcome::Failed,
        }),
        Err(StepError::Fatal(FatalError::Storage))
    ));
    assert!(core.is_stopped());
    assert!(matches!(
        core.status().stopped_reason(),
        Some(StoppedReason::Fatal(FatalError::Storage))
    ));
    assert!(
        core.step(Event::Tick(TickKind::Election))
            .unwrap()
            .effects
            .is_empty()
    );
    assert!(matches!(
        core.step(Event::Propose {
            proposal_id: ProposalId::new(7),
            command: "x".to_owned(),
            encoded_len: 1,
        }),
        Err(StepError::Stopped(StoppedReason::Fatal(
            FatalError::Storage
        )))
    ));
}

#[test]
fn successful_completion_is_idempotent_and_conflicts_are_rejected() {
    let mut core = core();
    let effects = core.step(Event::Tick(TickKind::Election)).unwrap().effects;
    let persist_id = match effects.as_slice() {
        [Effect::Persist { id, .. }] => *id,
        other => panic!("expected self-vote persist, got {other:?}"),
    };
    core.step(Event::EffectCompleted {
        id: persist_id,
        outcome: EffectOutcome::Persisted,
    })
    .unwrap();

    assert!(matches!(
        core.step(Event::EffectCompleted {
            id: persist_id,
            outcome: EffectOutcome::Persisted,
        }),
        Err(StepError::Input(InputError::AlreadyCompleted))
    ));
    assert!(matches!(
        core.step(Event::EffectCompleted {
            id: persist_id,
            outcome: EffectOutcome::Failed,
        }),
        Err(StepError::Input(InputError::ConflictingEffectOutcome))
    ));
}

#[test]
fn shutdown_is_idempotent_and_preserves_its_reason() {
    let mut core = core();
    core.step(Event::Shutdown).unwrap();
    core.step(Event::Shutdown).unwrap();

    assert!(core.is_stopped());
    assert_eq!(
        core.status().stopped_reason(),
        Some(StoppedReason::Shutdown)
    );
}
