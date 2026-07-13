use ruft::{
    ArithmeticError, ClusterId, Config, EffectId, InitError, LogIndex, NodeId, SnapshotDigest,
    SnapshotId, SnapshotMetadata, SnapshotRef, Term,
};

fn valid_builder() -> ruft::ConfigBuilder {
    Config::builder(
        NodeId::new(2),
        [NodeId::new(3), NodeId::new(1), NodeId::new(2)],
    )
    .cluster_id(ClusterId::new(99))
    .heartbeat_ticks(2)
    .election_ticks(10..=20)
    .check_quorum_ticks(10)
    .random_seed(7)
    .generation(11)
}

#[test]
fn builds_a_valid_fixed_membership_configuration() {
    let config = valid_builder().build().unwrap();

    assert_eq!(config.local_id(), NodeId::new(2));
    assert_eq!(config.cluster_id(), ClusterId::new(99));
    assert_eq!(
        config.members(),
        &[NodeId::new(1), NodeId::new(2), NodeId::new(3)]
    );
    assert_eq!(config.quorum(), 2);
    assert_eq!(config.heartbeat_ticks(), 2);
    assert_eq!(config.election_ticks(), 10..=20);
    assert_eq!(config.check_quorum_ticks(), 10);
    assert_eq!(config.random_seed(), 7);
    assert_eq!(config.generation(), 11);
}

#[test]
fn supplies_bounded_resource_defaults() {
    let config = valid_builder().build().unwrap();

    assert_eq!(config.max_unstable_bytes(), 4 * 1024 * 1024);
    assert_eq!(config.max_unapplied_entries(), 16_384);
    assert_eq!(config.snapshot_after_entries(), 10_000);
    assert_eq!(config.snapshot_after_bytes(), 64 * 1024 * 1024);
    assert_eq!(config.max_uncompacted_bytes(), 256 * 1024 * 1024);
    assert_eq!(config.max_entries_per_rpc(), 256);
    assert_eq!(config.max_bytes_per_rpc(), 1024 * 1024);
    assert_eq!(config.max_inflight_appends(), 256);
    assert_eq!(config.max_pending_reads(), 4_096);
    assert_eq!(config.snapshot_chunk_bytes(), 64 * 1024);
    assert_eq!(config.max_snapshot_ref_bytes(), 4 * 1024);
    assert_eq!(config.max_members(), 1_024);
    assert_eq!(config.completion_history(), 8_192);
}

#[test]
fn rejects_invalid_membership() {
    assert!(matches!(
        Config::builder(NodeId::new(1), []).build(),
        Err(InitError::EmptyMembership)
    ));
    assert!(matches!(
        Config::builder(NodeId::new(1), [NodeId::new(1), NodeId::new(1)]).build(),
        Err(InitError::DuplicateMember(NodeId(1)))
    ));
    assert!(matches!(
        Config::builder(NodeId::new(9), [NodeId::new(1), NodeId::new(2)]).build(),
        Err(InitError::LocalNodeNotMember(NodeId(9)))
    ));
    assert!(matches!(
        Config::builder(NodeId::new(1), [NodeId::new(1), NodeId::new(2)])
            .max_members(1)
            .build(),
        Err(InitError::TooManyMembers {
            actual: 2,
            maximum: 1
        })
    ));
}

#[test]
fn rejects_invalid_tick_relationships() {
    // Runtime values preserve the invalid input without triggering Clippy's
    // compile-time reversed-range lint in this negative test.
    let reversed_min = 20;
    let reversed_max = 10;

    assert!(matches!(
        valid_builder().election_ticks(0..=10).build(),
        Err(InitError::InvalidElectionRange { min: 0, max: 10 })
    ));
    assert!(matches!(
        valid_builder().election_ticks(10..=10).build(),
        Err(InitError::InvalidElectionRange { min: 10, max: 10 })
    ));
    assert!(matches!(
        valid_builder()
            .election_ticks(reversed_min..=reversed_max)
            .build(),
        Err(InitError::InvalidElectionRange { min: 20, max: 10 })
    ));
    assert!(matches!(
        valid_builder().heartbeat_ticks(0).build(),
        Err(InitError::InvalidHeartbeatTicks {
            heartbeat: 0,
            election_min: 10
        })
    ));
    assert!(matches!(
        valid_builder().heartbeat_ticks(10).build(),
        Err(InitError::InvalidHeartbeatTicks {
            heartbeat: 10,
            election_min: 10
        })
    ));
    assert!(matches!(
        valid_builder().check_quorum_ticks(9).build(),
        Err(InitError::InvalidCheckQuorumTicks {
            check_quorum: 9,
            election_min: 10
        })
    ));
}

#[test]
fn rejects_zero_capacity_limits() {
    assert!(matches!(
        valid_builder().max_unstable_bytes(0).build(),
        Err(InitError::ZeroCapacityLimit("max_unstable_bytes"))
    ));
    assert!(matches!(
        valid_builder().snapshot_chunk_bytes(0).build(),
        Err(InitError::ZeroCapacityLimit("snapshot_chunk_bytes"))
    ));
    assert!(matches!(
        valid_builder().completion_history(0).build(),
        Err(InitError::ZeroCapacityLimit("completion_history"))
    ));
}

#[test]
fn rejects_capacity_relationships_that_cannot_make_progress() {
    assert!(matches!(
        valid_builder()
            .snapshot_after_bytes(1024)
            .max_uncompacted_bytes(512)
            .build(),
        Err(InitError::InvalidCapacityRelationship {
            limit: "max_uncompacted_bytes",
            value: 512,
            required_at_least: "snapshot_after_bytes",
            minimum: 1024,
        })
    ));
    assert!(matches!(
        valid_builder()
            .max_unstable_bytes(512)
            .max_bytes_per_rpc(1024)
            .build(),
        Err(InitError::InvalidCapacityRelationship {
            limit: "max_unstable_bytes",
            value: 512,
            required_at_least: "max_bytes_per_rpc",
            minimum: 1024,
        })
    ));
}

#[test]
fn term_and_log_index_increment_without_wrapping() {
    assert_eq!(Term::new(7).checked_next(), Ok(Term::new(8)));
    assert_eq!(LogIndex::new(9).checked_next(), Ok(LogIndex::new(10)));
    assert_eq!(
        Term::new(u64::MAX).checked_next(),
        Err(ArithmeticError::Overflow)
    );
    assert_eq!(
        LogIndex::new(u64::MAX).checked_next(),
        Err(ArithmeticError::Overflow)
    );
}

#[test]
fn effect_ids_are_scoped_by_generation_and_sequence() {
    let id = EffectId::new(4, 12);

    assert_eq!(id.generation(), 4);
    assert_eq!(id.sequence(), 12);
}

#[test]
fn snapshot_metadata_requires_a_real_log_position_and_members() {
    let digest = SnapshotDigest::new([0xAB; 32]);

    assert!(matches!(
        SnapshotMetadata::new(
            SnapshotId::new(1),
            LogIndex::new(0),
            Term::new(1),
            vec![NodeId::new(1)],
            0,
            digest,
        ),
        Err(InitError::InvalidSnapshotMetadata(
            "last included index must be greater than zero"
        ))
    ));
    assert!(matches!(
        SnapshotMetadata::new(
            SnapshotId::new(1),
            LogIndex::new(1),
            Term::new(1),
            Vec::new(),
            0,
            digest,
        ),
        Err(InitError::InvalidSnapshotMetadata(
            "snapshot membership must not be empty"
        ))
    ));
    assert!(matches!(
        SnapshotMetadata::new(
            SnapshotId::new(1),
            LogIndex::new(1),
            Term::new(0),
            vec![NodeId::new(1)],
            0,
            digest,
        ),
        Err(InitError::InvalidSnapshotMetadata(
            "last included term must be greater than zero"
        ))
    ));
    assert!(matches!(
        SnapshotMetadata::new(
            SnapshotId::new(1),
            LogIndex::new(1),
            Term::new(1),
            vec![NodeId::new(1), NodeId::new(1)],
            0,
            digest,
        ),
        Err(InitError::InvalidSnapshotMetadata(
            "snapshot membership contains a duplicate node"
        ))
    ));
}

#[test]
fn snapshot_metadata_and_reference_preserve_host_values() {
    let digest = SnapshotDigest::new([0xCD; 32]);
    let metadata = SnapshotMetadata::new(
        SnapshotId::new(5),
        LogIndex::new(13),
        Term::new(3),
        vec![NodeId::new(1), NodeId::new(2), NodeId::new(3)],
        0,
        digest,
    )
    .unwrap();
    let snapshot_ref = SnapshotRef::new(b"snapshots/5".to_vec());

    assert_eq!(metadata.id(), SnapshotId::new(5));
    assert_eq!(metadata.index(), LogIndex::new(13));
    assert_eq!(metadata.term(), Term::new(3));
    assert_eq!(
        metadata.members(),
        &[NodeId::new(1), NodeId::new(2), NodeId::new(3)]
    );
    assert_eq!(metadata.size(), 0);
    assert_eq!(metadata.digest(), digest);
    assert_eq!(snapshot_ref.as_bytes(), b"snapshots/5");
}
