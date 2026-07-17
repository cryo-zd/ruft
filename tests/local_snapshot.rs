use ruft::{
    ClusterId, Config, Effect, EffectOutcome, Entry, Event, HardState, LogIndex, NodeId, RaftCore,
    RecoveredState, SnapshotDigest, SnapshotId, SnapshotMetadata, SnapshotRef, Term,
};

fn entry(index: u64) -> Entry<String> {
    Entry::command(LogIndex::new(index), Term::new(2), format!("{index}"), 1).unwrap()
}

fn core() -> RaftCore<String> {
    let config = Config::builder(
        NodeId::new(2),
        [NodeId::new(1), NodeId::new(2), NodeId::new(3)],
    )
    .cluster_id(ClusterId::new(9))
    .heartbeat_ticks(2)
    .election_ticks(10..=20)
    .check_quorum_ticks(10)
    .snapshot_after_entries(1)
    .build()
    .unwrap();
    let recovered = RecoveredState::new(
        HardState::new(Term::new(2), None, LogIndex::new(2)),
        None,
        vec![entry(1), entry(2)],
    )
    .unwrap();
    RaftCore::new(config, recovered).unwrap()
}

fn metadata(index: u64, term: u64) -> SnapshotMetadata {
    SnapshotMetadata::new(
        SnapshotId::new(7),
        LogIndex::new(index),
        Term::new(term),
        vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
        12,
        SnapshotDigest::new([3; 32]),
    )
    .unwrap()
}

fn applied_core() -> (RaftCore<String>, ruft::EffectId) {
    let mut core = core();
    let effects = core.step(Event::SnapshotRequested).unwrap().effects;
    let apply_id = match effects.as_slice() {
        [Effect::Apply { id, .. }] => *id,
        other => panic!("expected recovery apply, got {other:?}"),
    };
    let effects = core
        .step(Event::EffectCompleted {
            id: apply_id,
            outcome: EffectOutcome::Applied {
                through: LogIndex::new(2),
            },
        })
        .unwrap()
        .effects;
    let build_id = match effects.as_slice() {
        [Effect::BuildSnapshot { id, through }] if *through == LogIndex::new(2) => *id,
        other => panic!("expected automatic snapshot build, got {other:?}"),
    };
    (core, build_id)
}

#[test]
fn compaction_waits_for_snapshot_body_and_metadata_durability() {
    let (mut core, build_id) = applied_core();
    assert!(
        core.step(Event::SnapshotRequested)
            .unwrap()
            .effects
            .is_empty()
    );

    let effects = core
        .step(Event::EffectCompleted {
            id: build_id,
            outcome: EffectOutcome::SnapshotBuilt {
                metadata: metadata(2, 2),
                snapshot_ref: SnapshotRef::new(vec![9]),
            },
        })
        .unwrap()
        .effects;
    let persist_id = match effects.as_slice() {
        [Effect::Persist { id, batch }]
            if batch
                .snapshot
                .as_ref()
                .is_some_and(|record| record.metadata().index() == LogIndex::new(2)) =>
        {
            *id
        }
        other => panic!("expected snapshot metadata persistence, got {other:?}"),
    };

    let effects = core
        .step(Event::EffectCompleted {
            id: persist_id,
            outcome: EffectOutcome::Persisted,
        })
        .unwrap()
        .effects;
    let compact_id = match effects.as_slice() {
        [Effect::CompactLog { id, through }] if *through == LogIndex::new(2) => *id,
        other => panic!("expected compaction after persistence, got {other:?}"),
    };
    assert_eq!(core.first_log_index(), LogIndex::new(1));

    core.step(Event::EffectCompleted {
        id: compact_id,
        outcome: EffectOutcome::Compacted {
            through: LogIndex::new(2),
        },
    })
    .unwrap();
    assert_eq!(core.first_log_index(), LogIndex::new(3));
}

#[test]
fn invalid_snapshot_metadata_keeps_the_build_active() {
    let (mut core, build_id) = applied_core();
    let error = core.step(Event::EffectCompleted {
        id: build_id,
        outcome: EffectOutcome::SnapshotBuilt {
            metadata: metadata(2, 1),
            snapshot_ref: SnapshotRef::new(vec![9]),
        },
    });
    assert!(matches!(error, Err(ruft::StepError::InvalidSnapshot)));
    assert!(
        core.step(Event::SnapshotRequested)
            .unwrap()
            .effects
            .is_empty()
    );
}

#[test]
fn snapshot_body_and_compaction_failures_stop_the_core() {
    let (mut core, build_id) = applied_core();
    assert!(matches!(
        core.step(Event::EffectCompleted {
            id: build_id,
            outcome: EffectOutcome::Failed
        }),
        Err(ruft::StepError::SnapshotFailed)
    ));
    assert!(core.status().is_stopped());

    let (mut core, build_id) = applied_core();
    let effects = core
        .step(Event::EffectCompleted {
            id: build_id,
            outcome: EffectOutcome::SnapshotBuilt {
                metadata: metadata(2, 2),
                snapshot_ref: SnapshotRef::new(vec![9]),
            },
        })
        .unwrap()
        .effects;
    let persist_id = match effects.as_slice() {
        [Effect::Persist { id, .. }] => *id,
        _ => unreachable!(),
    };
    let effects = core
        .step(Event::EffectCompleted {
            id: persist_id,
            outcome: EffectOutcome::Persisted,
        })
        .unwrap()
        .effects;
    let compact_id = match effects.as_slice() {
        [Effect::CompactLog { id, .. }] => *id,
        _ => unreachable!(),
    };
    assert!(matches!(
        core.step(Event::EffectCompleted {
            id: compact_id,
            outcome: EffectOutcome::Failed
        }),
        Err(ruft::StepError::CompactionFailed)
    ));
    assert!(core.status().is_stopped());
}

#[test]
fn zero_applied_work_does_not_start_a_snapshot() {
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
    assert!(
        core.step(Event::SnapshotRequested)
            .unwrap()
            .effects
            .is_empty()
    );
}
