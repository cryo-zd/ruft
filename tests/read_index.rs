use ruft::{
    ClusterId, Config, Effect, EffectOutcome, Envelope, Event, HardState, LogIndex, Message,
    NodeId, RaftCore, ReadId, RecoveredState, Role, Term, TickKind,
};

fn config(members: impl IntoIterator<Item = NodeId>) -> Config {
    Config::builder(NodeId::new(1), members)
        .cluster_id(ClusterId::new(9))
        .heartbeat_ticks(2)
        .election_ticks(10..=20)
        .check_quorum_ticks(10)
        .build()
        .unwrap()
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

fn elected_leader_before_apply() -> (RaftCore<String>, ruft::EffectId) {
    let recovered = RecoveredState::new(
        HardState::new(Term::new(0), None, Default::default()),
        None,
        Vec::<ruft::Entry<String>>::new(),
    )
    .unwrap();
    let mut core = RaftCore::new(
        config([NodeId::new(1), NodeId::new(2), NodeId::new(3)]),
        recovered,
    )
    .unwrap();
    core.step(Event::Tick(TickKind::Election)).unwrap();
    let effects = receive(
        &mut core,
        2,
        Message::PreVoteResponse {
            term: Term::new(0),
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
            term: Term::new(1),
            granted: true,
        },
    );
    let noop_id = match effects.as_slice() {
        [Effect::Persist { id, .. }] => *id,
        other => panic!("expected no-op persistence, got {other:?}"),
    };
    core.step(Event::EffectCompleted {
        id: noop_id,
        outcome: EffectOutcome::Persisted,
    })
    .unwrap();
    assert_eq!(core.status().role(), Role::Leader);
    let effects = receive(
        &mut core,
        2,
        Message::AppendEntriesResponse {
            term: Term::new(1),
            success: true,
            match_index: LogIndex::new(1),
            conflict: None,
            read_context: None,
        },
    );
    let commit_id = match effects.iter().find_map(|effect| match effect {
        Effect::Persist { id, batch }
            if batch
                .hard_state
                .as_ref()
                .is_some_and(|state| state.commit_index() == LogIndex::new(1)) =>
        {
            Some(*id)
        }
        _ => None,
    }) {
        Some(id) => id,
        None => panic!("expected no-op commit persistence, got {effects:?}"),
    };
    let effects = core
        .step(Event::EffectCompleted {
            id: commit_id,
            outcome: EffectOutcome::Persisted,
        })
        .unwrap()
        .effects;
    let apply_id = match effects.iter().find_map(|effect| match effect {
        Effect::Apply { id, .. } => Some(*id),
        _ => None,
    }) {
        Some(id) => id,
        None => panic!("expected no-op apply, got {effects:?}"),
    };
    (core, apply_id)
}

fn read_context(effects: &[Effect<String>]) -> Vec<u8> {
    match effects.iter().find_map(|effect| match effect {
        Effect::SendMessage {
            message:
                Message::AppendEntries {
                    read_context: Some(context),
                    ..
                },
            ..
        } => Some(context.clone()),
        _ => None,
    }) {
        Some(context) => context,
        None => panic!("expected a ReadIndex heartbeat, got {effects:?}"),
    }
}

#[test]
fn read_waits_for_exact_quorum_context_and_local_apply() {
    let (mut core, apply_id) = elected_leader_before_apply();
    let effects = core
        .step(Event::Read {
            read_id: ReadId::new(44),
            context: b"client-a".to_vec(),
        })
        .unwrap()
        .effects;
    let context = read_context(&effects);

    let effects = receive(
        &mut core,
        2,
        Message::AppendEntriesResponse {
            term: Term::new(1),
            success: true,
            match_index: LogIndex::new(1),
            conflict: None,
            read_context: Some(vec![0; context.len()]),
        },
    );
    assert!(
        effects
            .iter()
            .all(|effect| !matches!(effect, Effect::ReadReady { .. }))
    );

    let effects = receive(
        &mut core,
        2,
        Message::AppendEntriesResponse {
            term: Term::new(1),
            success: true,
            match_index: LogIndex::new(1),
            conflict: None,
            read_context: Some(context),
        },
    );
    assert!(
        effects
            .iter()
            .all(|effect| !matches!(effect, Effect::ReadReady { .. }))
    );

    let effects = core
        .step(Event::EffectCompleted {
            id: apply_id,
            outcome: EffectOutcome::Applied {
                through: LogIndex::new(1),
            },
        })
        .unwrap()
        .effects;
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReadReady {
            read_id: ReadId(44),
            read_index: LogIndex(1)
        }]
    ));
}

#[test]
fn reads_share_one_round_and_release_together() {
    let (mut core, apply_id) = elected_leader_before_apply();
    let effects = core
        .step(Event::Read {
            read_id: ReadId::new(10),
            context: Vec::new(),
        })
        .unwrap()
        .effects;
    let context = read_context(&effects);
    assert!(
        core.step(Event::Read {
            read_id: ReadId::new(11),
            context: Vec::new()
        })
        .unwrap()
        .effects
        .is_empty()
    );
    receive(
        &mut core,
        2,
        Message::AppendEntriesResponse {
            term: Term::new(1),
            success: true,
            match_index: LogIndex::new(1),
            conflict: None,
            read_context: Some(context),
        },
    );
    let effects = core
        .step(Event::EffectCompleted {
            id: apply_id,
            outcome: EffectOutcome::Applied {
                through: LogIndex::new(1),
            },
        })
        .unwrap()
        .effects;
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::ReadReady {
                read_id: ReadId(10),
                read_index: LogIndex(1)
            },
            Effect::ReadReady {
                read_id: ReadId(11),
                read_index: LogIndex(1)
            },
        ]
    ));
}

#[test]
fn read_before_noop_commit_is_held_until_the_leader_is_confirmed() {
    let recovered = RecoveredState::new(
        HardState::new(Term::new(0), None, Default::default()),
        None,
        Vec::<ruft::Entry<String>>::new(),
    )
    .unwrap();
    let mut core = RaftCore::new(
        config([NodeId::new(1), NodeId::new(2), NodeId::new(3)]),
        recovered,
    )
    .unwrap();
    core.step(Event::Tick(TickKind::Election)).unwrap();
    let effects = receive(
        &mut core,
        2,
        Message::PreVoteResponse {
            term: Term::new(0),
            granted: true,
        },
    );
    let vote_id = match effects.as_slice() {
        [Effect::Persist { id, .. }] => *id,
        _ => unreachable!(),
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
            term: Term::new(1),
            granted: true,
        },
    );
    let noop_id = match effects.as_slice() {
        [Effect::Persist { id, .. }] => *id,
        _ => unreachable!(),
    };
    core.step(Event::EffectCompleted {
        id: noop_id,
        outcome: EffectOutcome::Persisted,
    })
    .unwrap();
    let effects = core
        .step(Event::Read {
            read_id: ReadId::new(8),
            context: Vec::new(),
        })
        .unwrap()
        .effects;
    assert!(effects.is_empty());

    let effects = receive(
        &mut core,
        2,
        Message::AppendEntriesResponse {
            term: Term::new(1),
            success: true,
            match_index: LogIndex::new(1),
            conflict: None,
            read_context: None,
        },
    );
    let commit_id = match effects.iter().find_map(|effect| match effect {
        Effect::Persist { id, .. } => Some(*id),
        _ => None,
    }) {
        Some(id) => id,
        None => panic!("expected commit persistence"),
    };
    let effects = core
        .step(Event::EffectCompleted {
            id: commit_id,
            outcome: EffectOutcome::Persisted,
        })
        .unwrap()
        .effects;
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::SendMessage {
            message: Message::AppendEntries {
                read_context: Some(_),
                ..
            },
            ..
        }
    )));
}

#[test]
fn single_node_read_is_ready_after_the_noop_is_applied() {
    let recovered = RecoveredState::new(
        HardState::new(Term::new(0), None, Default::default()),
        None,
        Vec::<ruft::Entry<String>>::new(),
    )
    .unwrap();
    let mut core = RaftCore::new(config([NodeId::new(1)]), recovered).unwrap();
    let effects = core.step(Event::Tick(TickKind::Election)).unwrap().effects;
    let vote_id = match effects.as_slice() {
        [Effect::Persist { id, .. }] => *id,
        _ => unreachable!(),
    };
    let effects = core
        .step(Event::EffectCompleted {
            id: vote_id,
            outcome: EffectOutcome::Persisted,
        })
        .unwrap()
        .effects;
    let noop_id = match effects.as_slice() {
        [Effect::Persist { id, .. }] => *id,
        _ => unreachable!(),
    };
    let effects = core
        .step(Event::EffectCompleted {
            id: noop_id,
            outcome: EffectOutcome::Persisted,
        })
        .unwrap()
        .effects;
    let commit_id = match effects.as_slice() {
        [Effect::Persist { id, .. }] => *id,
        _ => unreachable!(),
    };
    let effects = core
        .step(Event::EffectCompleted {
            id: commit_id,
            outcome: EffectOutcome::Persisted,
        })
        .unwrap()
        .effects;
    let apply_id = match effects.as_slice() {
        [Effect::Apply { id, .. }] => *id,
        _ => unreachable!(),
    };
    core.step(Event::EffectCompleted {
        id: apply_id,
        outcome: EffectOutcome::Applied {
            through: LogIndex::new(1),
        },
    })
    .unwrap();
    let effects = core
        .step(Event::Read {
            read_id: ReadId::new(99),
            context: Vec::new(),
        })
        .unwrap()
        .effects;
    assert!(matches!(
        effects.as_slice(),
        [Effect::ReadReady {
            read_id: ReadId(99),
            read_index: LogIndex(1)
        }]
    ));
}
