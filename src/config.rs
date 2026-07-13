use std::{collections::BTreeSet, ops::RangeInclusive};

use crate::{ClusterId, InitError, NodeId};

const DEFAULT_HEARTBEAT_TICKS: u64 = 1;
const DEFAULT_ELECTION_TICKS: RangeInclusive<u64> = 10..=20;
const DEFAULT_CHECK_QUORUM_TICKS: u64 = 10;
const DEFAULT_MAX_UNSTABLE_BYTES: usize = 4 * 1024 * 1024;
const DEFAULT_MAX_UNAPPLIED_ENTRIES: usize = 16_384;
const DEFAULT_SNAPSHOT_AFTER_ENTRIES: usize = 10_000;
const DEFAULT_SNAPSHOT_AFTER_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_MAX_UNCOMPACTED_BYTES: usize = 256 * 1024 * 1024;
const DEFAULT_MAX_ENTRIES_PER_RPC: usize = 256;
const DEFAULT_MAX_BYTES_PER_RPC: usize = 1024 * 1024;
const DEFAULT_MAX_INFLIGHT_APPENDS: usize = 256;
const DEFAULT_MAX_PENDING_READS: usize = 4_096;
const DEFAULT_SNAPSHOT_CHUNK_BYTES: usize = 64 * 1024;
const DEFAULT_MAX_SNAPSHOT_REF_BYTES: usize = 4 * 1024;
const DEFAULT_MAX_MEMBERS: usize = 1_024;
const DEFAULT_COMPLETION_HISTORY: usize = 8_192;

/// Immutable, validated settings for one Raft group.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
// Deserialization must eventually route through ConfigBuilder so invalid durable
// data cannot bypass the same checks used for a fresh configuration.
pub struct Config {
    cluster_id: ClusterId,
    local_id: NodeId,
    members: Vec<NodeId>,
    quorum: usize,
    heartbeat_ticks: u64,
    election_ticks: RangeInclusive<u64>,
    check_quorum_ticks: u64,
    random_seed: u64,
    generation: u64,
    max_unstable_bytes: usize,
    max_unapplied_entries: usize,
    snapshot_after_entries: usize,
    snapshot_after_bytes: usize,
    max_uncompacted_bytes: usize,
    max_entries_per_rpc: usize,
    max_bytes_per_rpc: usize,
    max_inflight_appends: usize,
    max_pending_reads: usize,
    snapshot_chunk_bytes: usize,
    max_snapshot_ref_bytes: usize,
    max_members: usize,
    completion_history: usize,
}

impl Config {
    /// Starts a builder for one local node and a fixed member set.
    pub fn builder(local_id: NodeId, members: impl IntoIterator<Item = NodeId>) -> ConfigBuilder {
        ConfigBuilder::new(local_id, members.into_iter().collect())
    }

    /// Returns the logical Raft group identifier.
    pub const fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    /// Returns the node hosted by this core.
    pub const fn local_id(&self) -> NodeId {
        self.local_id
    }

    /// Returns the sorted, fixed member set.
    pub fn members(&self) -> &[NodeId] {
        &self.members
    }

    /// Returns the number of votes required for a strict majority.
    pub const fn quorum(&self) -> usize {
        self.quorum
    }

    /// Returns the heartbeat interval in logical ticks.
    pub const fn heartbeat_ticks(&self) -> u64 {
        self.heartbeat_ticks
    }

    /// Returns the inclusive randomized election timeout range.
    pub fn election_ticks(&self) -> RangeInclusive<u64> {
        self.election_ticks.clone()
    }

    /// Returns the CheckQuorum activity window in logical ticks.
    pub const fn check_quorum_ticks(&self) -> u64 {
        self.check_quorum_ticks
    }

    /// Returns the deterministic election PRNG seed.
    pub const fn random_seed(&self) -> u64 {
        self.random_seed
    }

    /// Returns the host-assigned recovery generation.
    pub const fn generation(&self) -> u64 {
        self.generation
    }
}

macro_rules! config_limit_getters {
    ($(($name:ident, $description:literal)),+ $(,)?) => {
        impl Config {
            $(
                #[doc = $description]
                pub const fn $name(&self) -> usize {
                    self.$name
                }
            )+
        }
    };
}

config_limit_getters!(
    (
        max_unstable_bytes,
        "Returns the maximum bytes waiting for durability."
    ),
    (
        max_unapplied_entries,
        "Returns the maximum committed entries waiting for apply."
    ),
    (
        snapshot_after_entries,
        "Returns the applied-entry snapshot trigger."
    ),
    (
        snapshot_after_bytes,
        "Returns the applied-byte snapshot trigger."
    ),
    (
        max_uncompacted_bytes,
        "Returns the hard uncompacted-log byte limit."
    ),
    (
        max_entries_per_rpc,
        "Returns the maximum entries in one append RPC."
    ),
    (
        max_bytes_per_rpc,
        "Returns the maximum encoded bytes in one append RPC."
    ),
    (
        max_inflight_appends,
        "Returns the per-follower append window size."
    ),
    (
        max_pending_reads,
        "Returns the maximum concurrent ReadIndex requests."
    ),
    (
        snapshot_chunk_bytes,
        "Returns the maximum snapshot chunk size."
    ),
    (
        max_snapshot_ref_bytes,
        "Returns the maximum opaque snapshot reference size."
    ),
    (max_members, "Returns the defensive member-count limit."),
    (
        completion_history,
        "Returns the detailed completed-effect history limit."
    ),
);

/// A consuming builder that validates all settings as one atomic operation.
#[derive(Clone, Debug)]
pub struct ConfigBuilder {
    cluster_id: ClusterId,
    local_id: NodeId,
    members: Vec<NodeId>,
    heartbeat_ticks: u64,
    election_ticks: RangeInclusive<u64>,
    check_quorum_ticks: u64,
    random_seed: u64,
    generation: u64,
    max_unstable_bytes: usize,
    max_unapplied_entries: usize,
    snapshot_after_entries: usize,
    snapshot_after_bytes: usize,
    max_uncompacted_bytes: usize,
    max_entries_per_rpc: usize,
    max_bytes_per_rpc: usize,
    max_inflight_appends: usize,
    max_pending_reads: usize,
    snapshot_chunk_bytes: usize,
    max_snapshot_ref_bytes: usize,
    max_members: usize,
    completion_history: usize,
}

impl ConfigBuilder {
    fn new(local_id: NodeId, members: Vec<NodeId>) -> Self {
        Self {
            cluster_id: ClusterId::default(),
            local_id,
            members,
            heartbeat_ticks: DEFAULT_HEARTBEAT_TICKS,
            election_ticks: DEFAULT_ELECTION_TICKS,
            check_quorum_ticks: DEFAULT_CHECK_QUORUM_TICKS,
            random_seed: 0,
            generation: 0,
            max_unstable_bytes: DEFAULT_MAX_UNSTABLE_BYTES,
            max_unapplied_entries: DEFAULT_MAX_UNAPPLIED_ENTRIES,
            snapshot_after_entries: DEFAULT_SNAPSHOT_AFTER_ENTRIES,
            snapshot_after_bytes: DEFAULT_SNAPSHOT_AFTER_BYTES,
            max_uncompacted_bytes: DEFAULT_MAX_UNCOMPACTED_BYTES,
            max_entries_per_rpc: DEFAULT_MAX_ENTRIES_PER_RPC,
            max_bytes_per_rpc: DEFAULT_MAX_BYTES_PER_RPC,
            max_inflight_appends: DEFAULT_MAX_INFLIGHT_APPENDS,
            max_pending_reads: DEFAULT_MAX_PENDING_READS,
            snapshot_chunk_bytes: DEFAULT_SNAPSHOT_CHUNK_BYTES,
            max_snapshot_ref_bytes: DEFAULT_MAX_SNAPSHOT_REF_BYTES,
            max_members: DEFAULT_MAX_MEMBERS,
            completion_history: DEFAULT_COMPLETION_HISTORY,
        }
    }

    /// Validates the builder and returns an immutable configuration.
    pub fn build(self) -> Result<Config, InitError> {
        if self.members.is_empty() {
            return Err(InitError::EmptyMembership);
        }

        for (name, value) in self.capacity_limits() {
            if value == 0 {
                return Err(InitError::ZeroCapacityLimit(name));
            }
        }

        // These relationships ensure that admission control cannot block the
        // work required to relieve its own backpressure.
        ensure_at_least(
            "max_uncompacted_bytes",
            self.max_uncompacted_bytes,
            "snapshot_after_bytes",
            self.snapshot_after_bytes,
        )?;
        ensure_at_least(
            "max_unstable_bytes",
            self.max_unstable_bytes,
            "max_bytes_per_rpc",
            self.max_bytes_per_rpc,
        )?;

        if self.members.len() > self.max_members {
            return Err(InitError::TooManyMembers {
                actual: self.members.len(),
                maximum: self.max_members,
            });
        }

        let mut unique_members = BTreeSet::new();
        for member in self.members {
            if !unique_members.insert(member) {
                return Err(InitError::DuplicateMember(member));
            }
        }
        if !unique_members.contains(&self.local_id) {
            return Err(InitError::LocalNodeNotMember(self.local_id));
        }

        let election_min = *self.election_ticks.start();
        let election_max = *self.election_ticks.end();
        if election_min == 0 || election_min >= election_max {
            return Err(InitError::InvalidElectionRange {
                min: election_min,
                max: election_max,
            });
        }
        if self.heartbeat_ticks == 0 || self.heartbeat_ticks >= election_min {
            return Err(InitError::InvalidHeartbeatTicks {
                heartbeat: self.heartbeat_ticks,
                election_min,
            });
        }
        if self.check_quorum_ticks < election_min {
            return Err(InitError::InvalidCheckQuorumTicks {
                check_quorum: self.check_quorum_ticks,
                election_min,
            });
        }

        let members: Vec<_> = unique_members.into_iter().collect();
        let quorum = members.len() / 2 + 1;

        Ok(Config {
            cluster_id: self.cluster_id,
            local_id: self.local_id,
            members,
            quorum,
            heartbeat_ticks: self.heartbeat_ticks,
            election_ticks: self.election_ticks,
            check_quorum_ticks: self.check_quorum_ticks,
            random_seed: self.random_seed,
            generation: self.generation,
            max_unstable_bytes: self.max_unstable_bytes,
            max_unapplied_entries: self.max_unapplied_entries,
            snapshot_after_entries: self.snapshot_after_entries,
            snapshot_after_bytes: self.snapshot_after_bytes,
            max_uncompacted_bytes: self.max_uncompacted_bytes,
            max_entries_per_rpc: self.max_entries_per_rpc,
            max_bytes_per_rpc: self.max_bytes_per_rpc,
            max_inflight_appends: self.max_inflight_appends,
            max_pending_reads: self.max_pending_reads,
            snapshot_chunk_bytes: self.snapshot_chunk_bytes,
            max_snapshot_ref_bytes: self.max_snapshot_ref_bytes,
            max_members: self.max_members,
            completion_history: self.completion_history,
        })
    }

    fn capacity_limits(&self) -> [(&'static str, usize); 13] {
        [
            ("max_unstable_bytes", self.max_unstable_bytes),
            ("max_unapplied_entries", self.max_unapplied_entries),
            ("snapshot_after_entries", self.snapshot_after_entries),
            ("snapshot_after_bytes", self.snapshot_after_bytes),
            ("max_uncompacted_bytes", self.max_uncompacted_bytes),
            ("max_entries_per_rpc", self.max_entries_per_rpc),
            ("max_bytes_per_rpc", self.max_bytes_per_rpc),
            ("max_inflight_appends", self.max_inflight_appends),
            ("max_pending_reads", self.max_pending_reads),
            ("snapshot_chunk_bytes", self.snapshot_chunk_bytes),
            ("max_snapshot_ref_bytes", self.max_snapshot_ref_bytes),
            ("max_members", self.max_members),
            ("completion_history", self.completion_history),
        ]
    }
}

fn ensure_at_least(
    limit: &'static str,
    value: usize,
    required_at_least: &'static str,
    minimum: usize,
) -> Result<(), InitError> {
    if value < minimum {
        return Err(InitError::InvalidCapacityRelationship {
            limit,
            value,
            required_at_least,
            minimum,
        });
    }
    Ok(())
}

macro_rules! builder_setters {
    ($(($name:ident, $type:ty)),+ $(,)?) => {
        impl ConfigBuilder {
            $(
                #[doc = concat!("Sets `", stringify!($name), "`.")]
                #[must_use]
                pub fn $name(mut self, value: $type) -> Self {
                    self.$name = value;
                    self
                }
            )+
        }
    };
}

builder_setters!(
    (cluster_id, ClusterId),
    (heartbeat_ticks, u64),
    (election_ticks, RangeInclusive<u64>),
    (check_quorum_ticks, u64),
    (random_seed, u64),
    (generation, u64),
    (max_unstable_bytes, usize),
    (max_unapplied_entries, usize),
    (snapshot_after_entries, usize),
    (snapshot_after_bytes, usize),
    (max_uncompacted_bytes, usize),
    (max_entries_per_rpc, usize),
    (max_bytes_per_rpc, usize),
    (max_inflight_appends, usize),
    (max_pending_reads, usize),
    (snapshot_chunk_bytes, usize),
    (max_snapshot_ref_bytes, usize),
    (max_members, usize),
    (completion_history, usize),
);
