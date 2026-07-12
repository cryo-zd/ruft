use std::collections::BTreeSet;

use crate::{ArithmeticError, InitError};

macro_rules! id_type {
    ($name:ident, $inner:ty, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
        #[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
        pub struct $name(pub $inner);

        impl $name {
            /// Creates an identifier from its wire-independent integer value.
            pub const fn new(value: $inner) -> Self {
                Self(value)
            }

            /// Returns the underlying integer value.
            pub const fn get(self) -> $inner {
                self.0
            }
        }

        impl core::fmt::Display for $name {
            fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
                self.0.fmt(formatter)
            }
        }
    };
}

id_type!(ClusterId, u128, "Identifies one logical Raft group.");
id_type!(NodeId, u64, "Identifies a node within a Raft group.");
id_type!(Term, u64, "A monotonically increasing Raft election term.");
id_type!(LogIndex, u64, "A one-based position in the replicated log.");
id_type!(ProposalId, u64, "Identifies a host write proposal.");
id_type!(ReadId, u64, "Identifies a host linearizable-read request.");
id_type!(SnapshotId, u128, "Identifies one immutable snapshot body.");

impl Term {
    /// Returns the next term without allowing integer wraparound.
    pub fn checked_next(self) -> Result<Self, ArithmeticError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(ArithmeticError::Overflow)
    }
}

impl LogIndex {
    /// Returns the next log index without allowing integer wraparound.
    pub fn checked_next(self) -> Result<Self, ArithmeticError> {
        self.0
            .checked_add(1)
            .map(Self)
            .ok_or(ArithmeticError::Overflow)
    }
}

/// Correlates an asynchronous effect with its completion event.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct EffectId {
    generation: u64,
    sequence: u64,
}

impl EffectId {
    /// Creates an effect identifier scoped to one recovered core generation.
    pub const fn new(generation: u64, sequence: u64) -> Self {
        Self {
            generation,
            sequence,
        }
    }

    /// Returns the host-provided recovery generation.
    pub const fn generation(self) -> u64 {
        self.generation
    }

    /// Returns the monotonically increasing sequence within the generation.
    pub const fn sequence(self) -> u64 {
        self.sequence
    }
}

/// An opaque, host-defined reference to durable snapshot contents.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct SnapshotRef(Vec<u8>);

impl SnapshotRef {
    /// Creates a reference without assigning a storage-specific interpretation.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Returns the opaque reference bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

/// A SHA-256 digest of an immutable snapshot body.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Deserialize, serde::Serialize))]
pub struct SnapshotDigest([u8; 32]);

impl SnapshotDigest {
    /// Wraps a previously computed SHA-256 digest.
    pub const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Returns the digest bytes.
    pub const fn into_bytes(self) -> [u8; 32] {
        self.0
    }

    /// Borrows the digest bytes.
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

/// Protocol metadata for an externally owned snapshot body.
#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
// Deserialization is intentionally deferred until it can call `new` and preserve
// the structural validation performed for host-created metadata.
pub struct SnapshotMetadata {
    id: SnapshotId,
    last_included_index: LogIndex,
    last_included_term: Term,
    members: Vec<NodeId>,
    size: u64,
    digest: SnapshotDigest,
}

impl SnapshotMetadata {
    /// Creates structurally valid snapshot metadata.
    pub fn new(
        id: SnapshotId,
        last_included_index: LogIndex,
        last_included_term: Term,
        members: Vec<NodeId>,
        size: u64,
        digest: SnapshotDigest,
    ) -> Result<Self, InitError> {
        if last_included_index == LogIndex::new(0) {
            return Err(InitError::InvalidSnapshotMetadata(
                "last included index must be greater than zero",
            ));
        }
        if last_included_term == Term::new(0) {
            return Err(InitError::InvalidSnapshotMetadata(
                "last included term must be greater than zero",
            ));
        }
        if members.is_empty() {
            return Err(InitError::InvalidSnapshotMetadata(
                "snapshot membership must not be empty",
            ));
        }

        let mut unique_members = BTreeSet::new();
        for member in members {
            if !unique_members.insert(member) {
                return Err(InitError::InvalidSnapshotMetadata(
                    "snapshot membership contains a duplicate node",
                ));
            }
        }

        Ok(Self {
            id,
            last_included_index,
            last_included_term,
            members: unique_members.into_iter().collect(),
            size,
            digest,
        })
    }

    /// Returns the immutable snapshot identifier.
    pub const fn id(&self) -> SnapshotId {
        self.id
    }

    /// Returns the highest log index represented by the snapshot.
    pub const fn index(&self) -> LogIndex {
        self.last_included_index
    }

    /// Returns the term at the snapshot boundary.
    pub const fn term(&self) -> Term {
        self.last_included_term
    }

    /// Returns the sorted fixed member set recorded in the snapshot.
    pub fn members(&self) -> &[NodeId] {
        &self.members
    }

    /// Returns the snapshot body size in bytes.
    pub const fn size(&self) -> u64 {
        self.size
    }

    /// Returns the expected SHA-256 digest of the snapshot body.
    pub const fn digest(&self) -> SnapshotDigest {
        self.digest
    }
}
