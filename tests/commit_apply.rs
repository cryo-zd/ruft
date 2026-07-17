use ruft::{
    ClusterId, Config, Effect, EffectOutcome, Entry, Envelope, Event, HardState, LogIndex, Message,
    NodeId, ProposalId, ProposalResult, RaftCore, RecoveredState, Role, Term, TickKind,
};

fn entry(index: u64, term: u64) -> Entry<String> {
    Entry::command(LogIndex::new(index), Term::new(term), format!("{index}"), 1).unwrap()
}

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

fn leader_with_prior_entry() -> RaftCore<String> {
    let recovered = RecoveredState::new(
        HardState::new(Term::new(1), None, Default::default()),
        None,
        vec![entry(1, 1)],
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
            term: Term::new(1),
            granted: true,
        },
    );
    let vote_id = match effects.as_slice() {
        [Effect::Persist { id, .. }] => *id,
        other => panic!("expected self-vote persistence, got {other:?}"),
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
            term: Term::new(2),
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
    core
}

#[test]
fn prior_term_entry_waits_for_a_current_term_quorum() {
    let mut core = leader_with_prior_entry();
    let effects = receive(
        &mut core,
        2,
        Message::AppendEntriesResponse {
            term: Term::new(2),
            success: true,
            match_index: LogIndex::new(1),
            conflict: None,
            read_context: None,
        },
    );
    assert!(effects.iter().all(|effect| !matches!(effect, Effect::Persist { batch, .. } if batch.hard_state.as_ref().is_some_and(|state| state.commit_index() > LogIndex::new(0)))));

    let effects = receive(
        &mut core,
        2,
        Message::AppendEntriesResponse {
            term: Term::new(2),
            success: true,
            match_index: LogIndex::new(2),
            conflict: None,
            read_context: None,
        },
    );
    assert!(effects.iter().any(|effect| matches!(effect, Effect::Persist { batch, .. } if batch.hard_state.as_ref().is_some_and(|state| state.commit_index() == LogIndex::new(2)))));
}

#[test]
fn quorum_commit_is_durable_before_ordered_apply_and_proposal_result() {
    let mut core = leader_with_prior_entry();
    let effects = core
        .step(Event::Propose {
            proposal_id: ProposalId::new(44),
            command: String::from("write"),
            encoded_len: 5,
        })
        .unwrap()
        .effects;
    let proposal_persist = match effects.as_slice() {
        [Effect::Persist { id, batch }] if batch.entries[0].index() == LogIndex::new(3) => *id,
        other => panic!("expected proposal persistence, got {other:?}"),
    };
    let effects = core
        .step(Event::EffectCompleted {
            id: proposal_persist,
            outcome: EffectOutcome::Persisted,
        })
        .unwrap()
        .effects;
    assert!(
        effects
            .iter()
            .all(|effect| !matches!(effect, Effect::ProposalResult { .. }))
    );

    let effects = receive(
        &mut core,
        2,
        Message::AppendEntriesResponse {
            term: Term::new(2),
            success: true,
            match_index: LogIndex::new(3),
            conflict: None,
            read_context: None,
        },
    );
    let commit_persist = match effects.iter().find_map(|effect| match effect {
        Effect::Persist { id, batch }
            if batch
                .hard_state
                .as_ref()
                .is_some_and(|state| state.commit_index() == LogIndex::new(3)) =>
        {
            Some(*id)
        }
        _ => None,
    }) {
        Some(id) => id,
        None => panic!("expected durable commit, got {effects:?}"),
    };
    let effects = core
        .step(Event::EffectCompleted {
            id: commit_persist,
            outcome: EffectOutcome::Persisted,
        })
        .unwrap()
        .effects;
    let apply_id = match effects.as_slice() {
        [Effect::Apply { id, entries }] => {
            assert_eq!(entries.first().unwrap().index(), LogIndex::new(1));
            assert_eq!(entries.last().unwrap().index(), LogIndex::new(3));
            *id
        }
        other => panic!("expected one ordered apply, got {other:?}"),
    };
    let effects = core
        .step(Event::EffectCompleted {
            id: apply_id,
            outcome: EffectOutcome::Applied {
                through: LogIndex::new(3),
            },
        })
        .unwrap()
        .effects;
    assert!(matches!(
        effects.as_slice(),
        [Effect::ProposalResult {
            proposal_id: ProposalId(44),
            result: ProposalResult::Applied { index: LogIndex(3) }
        }]
    ));
    assert!(matches!(
        core.step(Event::EffectCompleted {
            id: apply_id,
            outcome: EffectOutcome::Applied {
                through: LogIndex::new(3)
            }
        }),
        Err(ruft::StepError::Input(ruft::InputError::UnknownEffect))
    ));
}

#[test]
fn uncommitted_proposal_reports_leadership_lost_after_higher_term_message() {
    let mut core = leader_with_prior_entry();
    let effects = core
        .step(Event::Propose {
            proposal_id: ProposalId::new(9),
            command: String::from("write"),
            encoded_len: 5,
        })
        .unwrap()
        .effects;
    assert!(matches!(effects.as_slice(), [Effect::Persist { .. }]));
    let effects = receive(
        &mut core,
        2,
        Message::AppendEntriesResponse {
            term: Term::new(3),
            success: false,
            match_index: LogIndex::new(2),
            conflict: None,
            read_context: None,
        },
    );
    assert!(matches!(
        effects.as_slice(),
        [
            Effect::Persist { .. },
            Effect::ProposalResult {
                proposal_id: ProposalId(9),
                result: ProposalResult::LeadershipLost
            }
        ]
    ));
}

#[test]
fn apply_failure_stops_the_core() {
    let config = config([NodeId::new(1)]);
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
        other => panic!("expected self-vote persist, got {other:?}"),
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
        other => panic!("expected no-op persist, got {other:?}"),
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
        other => panic!("expected commit persist, got {other:?}"),
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
        other => panic!("expected apply, got {other:?}"),
    };
    assert!(matches!(
        core.step(Event::EffectCompleted {
            id: apply_id,
            outcome: EffectOutcome::Failed
        }),
        Err(ruft::StepError::ApplyFailed)
    ));
    assert!(core.status().is_stopped());
}
