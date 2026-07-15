use ruft::{
    ClusterId, Config, Effect, EffectOutcome, Entry, Envelope, Event, HardState, LogIndex, Message,
    NodeId, RaftCore, RecoveredState, Role, Term, TickKind,
};

fn core() -> RaftCore<String> {
    let config = Config::builder(
        NodeId::new(1),
        [NodeId::new(1), NodeId::new(2), NodeId::new(3)],
    )
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

#[test]
fn prevote_does_not_raise_term_and_vote_waits_for_persistence() {
    let mut core = core();
    let effects = core.step(Event::Tick(TickKind::Election)).unwrap().effects;

    assert_eq!(core.status().role(), Role::PreCandidate);
    assert_eq!(core.status().term(), Term::new(0));
    assert_eq!(effects.len(), 2);
    assert!(effects.iter().all(|effect| matches!(
        effect,
        Effect::SendMessage {
            message: Message::PreVote { term: Term(1), .. },
            ..
        }
    )));

    let effects = receive(
        &mut core,
        2,
        Message::PreVoteResponse {
            term: Term::new(0),
            granted: true,
        },
    );
    let persist_id = match effects.as_slice() {
        [Effect::Persist { id, batch }] => {
            assert_eq!(
                batch.hard_state.as_ref().unwrap().current_term(),
                Term::new(1)
            );
            assert_eq!(
                batch.hard_state.as_ref().unwrap().voted_for(),
                Some(NodeId::new(1))
            );
            *id
        }
        other => panic!("expected one persist effect, got {other:?}"),
    };
    assert_eq!(core.status().role(), Role::Candidate);

    let effects = core
        .step(Event::EffectCompleted {
            id: persist_id,
            outcome: EffectOutcome::Persisted,
        })
        .unwrap()
        .effects;
    assert_eq!(effects.len(), 2);
    assert!(effects.iter().all(|effect| matches!(
        effect,
        Effect::SendMessage {
            message: Message::RequestVote { term: Term(1), .. },
            ..
        }
    )));
}

fn elect_leader(core: &mut RaftCore<String>) {
    core.step(Event::Tick(TickKind::Election)).unwrap();
    let persist = receive(
        core,
        2,
        Message::PreVoteResponse {
            term: Term::new(0),
            granted: true,
        },
    );
    let id = match persist.as_slice() {
        [Effect::Persist { id, .. }] => *id,
        other => panic!("expected vote persistence, got {other:?}"),
    };
    core.step(Event::EffectCompleted {
        id,
        outcome: EffectOutcome::Persisted,
    })
    .unwrap();
    let persist = receive(
        core,
        2,
        Message::VoteResponse {
            term: Term::new(1),
            granted: true,
        },
    );
    let id = match persist.as_slice() {
        [Effect::Persist { id, batch }] => {
            assert_eq!(batch.entries.len(), 1);
            *id
        }
        other => panic!("expected leader no-op persistence, got {other:?}"),
    };
    core.step(Event::EffectCompleted {
        id,
        outcome: EffectOutcome::Persisted,
    })
    .unwrap();
    assert_eq!(core.status().role(), Role::Leader);
}

#[test]
fn leader_noop_is_durable_before_leader_heartbeats() {
    let mut core = core();
    elect_leader(&mut core);

    let effects = core.step(Event::Tick(TickKind::Heartbeat)).unwrap().effects;
    assert_eq!(core.status().role(), Role::Follower);
    assert!(effects.is_empty());
}

#[test]
fn leader_retains_authority_when_a_quorum_is_active() {
    let mut core = core();
    elect_leader(&mut core);
    receive(&mut core, 2, Message::Heartbeat);

    let effects = core.step(Event::Tick(TickKind::Heartbeat)).unwrap().effects;
    assert_eq!(core.status().role(), Role::Leader);
    assert!(effects.is_empty());
}

#[test]
fn vote_is_persisted_before_a_grant_is_sent() {
    let mut core = core();
    let effects = receive(
        &mut core,
        2,
        Message::RequestVote {
            term: Term::new(1),
            last_log_index: Default::default(),
            last_log_term: Default::default(),
        },
    );
    let id = match effects.as_slice() {
        [Effect::Persist { id, batch }] => {
            assert_eq!(
                batch.hard_state.as_ref().unwrap().voted_for(),
                Some(NodeId::new(2))
            );
            *id
        }
        other => panic!("expected persistence before vote response, got {other:?}"),
    };
    let effects = core
        .step(Event::EffectCompleted {
            id,
            outcome: EffectOutcome::Persisted,
        })
        .unwrap()
        .effects;
    assert!(
        matches!(effects.as_slice(), [Effect::SendMessage { to, message: Message::VoteResponse { term: Term(1), granted: true } }] if *to == NodeId::new(2))
    );
}

#[test]
fn higher_term_response_steps_down_and_persists_the_new_term() {
    let mut core = core();
    core.step(Event::Tick(TickKind::Election)).unwrap();
    let effects = receive(
        &mut core,
        2,
        Message::PreVoteResponse {
            term: Term::new(4),
            granted: false,
        },
    );
    assert_eq!(core.status().role(), Role::Follower);
    assert_eq!(core.status().term(), Term::new(4));
    assert!(
        matches!(effects.as_slice(), [Effect::Persist { batch, .. }] if batch.hard_state.as_ref().is_some_and(|state| state.current_term() == Term::new(4)))
    );
}

#[test]
fn stale_log_is_not_granted_a_vote() {
    let config = Config::builder(
        NodeId::new(1),
        [NodeId::new(1), NodeId::new(2), NodeId::new(3)],
    )
    .cluster_id(ClusterId::new(9))
    .heartbeat_ticks(2)
    .election_ticks(10..=20)
    .check_quorum_ticks(10)
    .build()
    .unwrap();
    let recovered = RecoveredState::new(
        HardState::new(Term::new(2), None, Default::default()),
        None,
        vec![Entry::command(LogIndex::new(1), Term::new(2), String::from("entry"), 5).unwrap()],
    )
    .unwrap();
    let mut core = RaftCore::new(config, recovered).unwrap();

    let effects = receive(
        &mut core,
        2,
        Message::RequestVote {
            term: Term::new(3),
            last_log_index: LogIndex::new(1),
            last_log_term: Term::new(1),
        },
    );
    let id = match effects.as_slice() {
        [Effect::Persist { id, batch }] => {
            assert_eq!(batch.hard_state.as_ref().unwrap().voted_for(), None);
            *id
        }
        other => panic!("expected term persistence, got {other:?}"),
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
            message: Message::VoteResponse {
                term: Term(3),
                granted: false
            },
            ..
        }]
    ));
}

#[test]
fn split_vote_returns_to_follower_and_can_retry_prevote() {
    let mut core = core();
    core.step(Event::Tick(TickKind::Election)).unwrap();
    let effects = receive(
        &mut core,
        2,
        Message::PreVoteResponse {
            term: Term::new(0),
            granted: true,
        },
    );
    let id = match effects.as_slice() {
        [Effect::Persist { id, .. }] => *id,
        other => panic!("expected candidate persistence, got {other:?}"),
    };
    core.step(Event::EffectCompleted {
        id,
        outcome: EffectOutcome::Persisted,
    })
    .unwrap();
    receive(
        &mut core,
        2,
        Message::VoteResponse {
            term: Term::new(1),
            granted: false,
        },
    );
    receive(
        &mut core,
        3,
        Message::VoteResponse {
            term: Term::new(1),
            granted: false,
        },
    );
    assert_eq!(core.status().role(), Role::Follower);

    let effects = core.step(Event::Tick(TickKind::Election)).unwrap().effects;
    assert_eq!(core.status().role(), Role::PreCandidate);
    assert_eq!(effects.len(), 2);
}

#[test]
fn single_node_election_persists_noop_before_becoming_leader() {
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
        Vec::<Entry<String>>::new(),
    )
    .unwrap();
    let mut core = RaftCore::new(config, recovered).unwrap();

    let effects = core.step(Event::Tick(TickKind::Election)).unwrap().effects;
    let vote_id = match effects.as_slice() {
        [Effect::Persist { id, .. }] => *id,
        other => panic!("expected self-vote persistence, got {other:?}"),
    };
    let effects = core
        .step(Event::EffectCompleted {
            id: vote_id,
            outcome: EffectOutcome::Persisted,
        })
        .unwrap()
        .effects;
    assert_eq!(core.status().role(), Role::Candidate);
    let noop_id = match effects.as_slice() {
        [Effect::Persist { id, batch }] if batch.entries.len() == 1 => *id,
        other => panic!("expected no-op persistence, got {other:?}"),
    };
    core.step(Event::EffectCompleted {
        id: noop_id,
        outcome: EffectOutcome::Persisted,
    })
    .unwrap();
    assert_eq!(core.status().role(), Role::Leader);
}
