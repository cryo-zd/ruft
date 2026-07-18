//! Local snapshot build, durability, and compaction coordination.

use sha2::{Digest, Sha256};

use crate::{EffectId, LogIndex, SnapshotMetadata, SnapshotRecord, SnapshotRef};

/// The single in-flight local snapshot operation, if any.
pub(crate) enum LocalSnapshotState {
    /// No local snapshot operation is active.
    Idle,
    /// The host is building the immutable snapshot body.
    Building { id: EffectId, through: LogIndex },
    /// The snapshot metadata and reference await durable persistence.
    Persisting {
        id: EffectId,
        record: SnapshotRecord,
    },
    /// The host may now remove the durable log prefix.
    Compacting {
        id: EffectId,
        record: SnapshotRecord,
    },
}

impl LocalSnapshotState {
    /// Returns whether a local snapshot operation is active.
    pub(crate) const fn is_idle(&self) -> bool {
        matches!(self, Self::Idle)
    }
}

/// Validated in-progress state for one inbound snapshot body.
pub(crate) struct SnapshotReceiver {
    metadata: SnapshotMetadata,
    next_offset: u64,
    hasher: Sha256,
    last_chunk: Option<(u64, Vec<u8>)>,
    final_chunk: bool,
}

impl SnapshotReceiver {
    /// Starts receiving the body described by metadata.
    pub(crate) fn new(metadata: SnapshotMetadata) -> Self {
        Self {
            metadata,
            next_offset: 0,
            hasher: Sha256::new(),
            last_chunk: None,
            final_chunk: false,
        }
    }

    /// Returns metadata shared by every chunk in this transfer.
    pub(crate) fn metadata(&self) -> &SnapshotMetadata {
        &self.metadata
    }

    /// Returns the next required byte offset.
    pub(crate) const fn next_offset(&self) -> u64 {
        self.next_offset
    }

    /// Accepts the exact next chunk or an idempotent repeat of the last chunk.
    pub(crate) fn accept(&mut self, offset: u64, bytes: &[u8], done: bool) -> Result<bool, ()> {
        if offset < self.next_offset {
            return Ok(self.last_chunk.as_ref().is_some_and(|(previous, payload)| {
                *previous == offset && payload.as_slice() == bytes
            }));
        }
        if offset != self.next_offset || self.final_chunk {
            return Err(());
        }
        let length = u64::try_from(bytes.len()).map_err(|_| ())?;
        let next = offset.checked_add(length).ok_or(())?;
        if next > self.metadata.size() {
            return Err(());
        }
        self.hasher.update(bytes);
        self.next_offset = next;
        self.last_chunk = Some((offset, bytes.to_vec()));
        self.final_chunk = done;
        Ok(true)
    }

    /// Returns whether all advertised bytes arrived with the expected digest.
    pub(crate) fn is_complete_and_valid(&self) -> bool {
        self.final_chunk
            && self.next_offset == self.metadata.size()
            && self.hasher.clone().finalize().as_slice() == self.metadata.digest().as_bytes()
    }

    /// Produces the durable record after the host returns a body reference.
    pub(crate) fn record(&self, snapshot_ref: SnapshotRef) -> SnapshotRecord {
        SnapshotRecord::new(self.metadata.clone(), snapshot_ref)
    }
}

/// Leader-side cursor for one outbound snapshot body.
pub(crate) struct SnapshotSender {
    snapshot: SnapshotRecord,
    offset: u64,
}

impl SnapshotSender {
    /// Starts sending an immutable durable snapshot from offset zero.
    pub(crate) fn new(snapshot: SnapshotRecord) -> Self {
        Self {
            snapshot,
            offset: 0,
        }
    }
    /// Returns the snapshot record being sent.
    pub(crate) fn snapshot(&self) -> &SnapshotRecord {
        &self.snapshot
    }
    /// Returns the next byte offset to read.
    pub(crate) const fn offset(&self) -> u64 {
        self.offset
    }
    /// Advances after a host read completion verifies the same offset.
    pub(crate) fn advance(&mut self, bytes: usize) -> Result<(), ()> {
        self.offset = self
            .offset
            .checked_add(u64::try_from(bytes).map_err(|_| ())?)
            .ok_or(())?;
        Ok(())
    }
}
