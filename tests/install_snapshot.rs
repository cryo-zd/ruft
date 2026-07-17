use ruft::{
    ClusterId, Config, Effect, EffectOutcome, Entry, Envelope, Event, HardState, LogIndex, Message,
    NodeId, RaftCore, RecoveredState, SnapshotDigest, SnapshotId, SnapshotMetadata, SnapshotRef,
    Term,
};
use sha2::{Digest, Sha256};

fn entry(index: u64, term: u64) -> Entry<String> {
    Entry::command(LogIndex::new(index), Term::new(term), format!("{index}"), 1).unwrap()
}

fn follower() -> RaftCore<String> {
    let config = Config::builder(
        NodeId::new(2),
        [NodeId::new(1), NodeId::new(2), NodeId::new(3)],
    )
    .cluster_id(ClusterId::new(9))
    .heartbeat_ticks(2)
    .election_ticks(10..=20)
    .check_quorum_ticks(10)
    .snapshot_chunk_bytes(16)
    .build()
    .unwrap();
    let recovered = RecoveredState::new(
        HardState::new(Term::new(3), None, LogIndex::new(8)),
        None,
        (1..=8)
            .map(|index| entry(index, 2))
            .chain(core::iter::once(entry(9, 3)))
            .collect(),
    )
    .unwrap();
    RaftCore::new(config, recovered).unwrap()
}

fn metadata(body: &[u8]) -> SnapshotMetadata {
    let digest: [u8; 32] = Sha256::digest(body).into();
    SnapshotMetadata::new(
        SnapshotId::new(42),
        LogIndex::new(8),
        Term::new(2),
        vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
        u64::try_from(body.len()).unwrap(),
        SnapshotDigest::new(digest),
    )
    .unwrap()
}

#[test]
fn install_response_waits_for_body_metadata_and_state_machine() {
    let body = b"body";
    let mut follower = follower();
    let effects = follower
        .step(Event::MessageReceived(Envelope::new(
            ClusterId::new(9),
            NodeId::new(1),
            NodeId::new(2),
            Message::InstallSnapshot {
                term: Term::new(3),
                metadata: metadata(body),
                offset: 0,
                bytes: body.to_vec(),
                done: true,
            },
        )))
        .unwrap()
        .effects;
    let store_id = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StoreSnapshotChunk {
                id, offset, bytes, ..
            } if *offset == 0 && bytes == body => Some(*id),
            _ => None,
        })
        .expect("expected chunk store");

    let effects = follower
        .step(Event::EffectCompleted {
            id: store_id,
            outcome: EffectOutcome::SnapshotChunkStored {
                snapshot_id: SnapshotId::new(42),
                next_offset: 4,
                snapshot_ref: Some(SnapshotRef::new(vec![7])),
            },
        })
        .unwrap()
        .effects;
    let persist_id = match effects.as_slice() {
        [Effect::Persist { id, batch }] if batch.snapshot.is_some() => *id,
        other => panic!("expected snapshot persistence, got {other:?}"),
    };

    let effects = follower
        .step(Event::EffectCompleted {
            id: persist_id,
            outcome: EffectOutcome::Persisted,
        })
        .unwrap()
        .effects;
    let install_id = match effects.as_slice() {
        [Effect::InstallSnapshot { id, record }]
            if record.metadata().index() == LogIndex::new(8) =>
        {
            *id
        }
        other => panic!("expected state-machine install, got {other:?}"),
    };

    let effects = follower
        .step(Event::EffectCompleted {
            id: install_id,
            outcome: EffectOutcome::SnapshotInstalled {
                snapshot_id: SnapshotId::new(42),
            },
        })
        .unwrap()
        .effects;
    assert!(matches!(effects.as_slice(), [Effect::SendMessage {
        to, message: Message::InstallSnapshotResponse { term: Term(3), success: true }
    }] if *to == NodeId::new(1)));
    assert_eq!(follower.first_log_index(), LogIndex::new(9));
}

#[test]
fn corrupted_final_chunk_is_not_persisted() {
    let mut follower = follower();
    let body = b"body";
    let effects = follower
        .step(Event::MessageReceived(Envelope::new(
            ClusterId::new(9),
            NodeId::new(1),
            NodeId::new(2),
            Message::InstallSnapshot {
                term: Term::new(3),
                metadata: metadata(body),
                offset: 0,
                bytes: b"bad!".to_vec(),
                done: true,
            },
        )))
        .unwrap()
        .effects;
    let store_id = effects
        .iter()
        .find_map(|effect| match effect {
            Effect::StoreSnapshotChunk { id, .. } => Some(*id),
            _ => None,
        })
        .expect("expected chunk store");
    assert!(matches!(
        follower.step(Event::EffectCompleted {
            id: store_id,
            outcome: EffectOutcome::SnapshotChunkStored {
                snapshot_id: SnapshotId::new(42),
                next_offset: 4,
                snapshot_ref: Some(SnapshotRef::new(vec![7]))
            },
        }),
        Err(ruft::StepError::InvalidSnapshot)
    ));
}

#[test]
fn chunk_offset_must_be_contiguous() {
    let mut follower = follower();
    let body = b"body";
    assert!(matches!(
        follower.step(Event::MessageReceived(Envelope::new(
            ClusterId::new(9),
            NodeId::new(1),
            NodeId::new(2),
            Message::InstallSnapshot {
                term: Term::new(3),
                metadata: metadata(body),
                offset: 1,
                bytes: body.to_vec(),
                done: true
            },
        ))),
        Err(ruft::StepError::InvalidSnapshot)
    ));
}
