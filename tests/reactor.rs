use ruft::{
    ClusterId, Config, EffectId, EffectOutcome, Envelope, Event, HardState, Message, NodeId,
    RaftCore, RecoveredState, StepError, Term,
};

fn core(generation: u64) -> RaftCore<String> {
    let config = Config::builder(
        NodeId::new(1),
        [NodeId::new(1), NodeId::new(2), NodeId::new(3)],
    )
    .cluster_id(ClusterId::new(9))
    .heartbeat_ticks(2)
    .election_ticks(10..=20)
    .check_quorum_ticks(10)
    .generation(generation)
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
fn rejects_wrong_recipient_without_changing_status() {
    let mut core = core(9);
    let before = core.status();
    let event = Event::MessageReceived(Envelope::new(
        ClusterId::new(9),
        NodeId::new(2),
        NodeId::new(3),
        Message::Heartbeat,
    ));

    assert!(matches!(core.step(event), Err(StepError::Input(_))));
    assert_eq!(core.status(), before);
}

#[test]
fn rejects_completion_from_an_old_generation() {
    let mut core = core(9);
    let event = Event::EffectCompleted {
        id: EffectId::new(8, 1),
        outcome: EffectOutcome::Persisted,
    };

    assert!(matches!(core.step(event), Err(StepError::Input(_))));
}
