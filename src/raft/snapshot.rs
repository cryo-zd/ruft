//! Local snapshot build, durability, and compaction coordination.
//!
//! Three sub-state-machines cooperate to handle snapshots:
//!
//! - [`LocalSnapshotState`] drives the local snapshot pipeline: **Build →
//!   Persist → Compact**. The host builds a snapshot body from the state
//!   machine, the core validates and persists the metadata, then the log
//!   prefix is compacted away.
//!
//! - [`SnapshotReceiver`] validates an incoming snapshot stream from a
//!   leader. Chunks arrive in order, are fed into a running SHA-256 hash,
//!   and the complete digest is verified before the snapshot is persisted
//!   and installed.
//!
//! - [`SnapshotSender`] drives the leader-side transmission: it tracks the
//!   current byte offset and paces chunk reads through the host.

use sha2::{Digest, Sha256};

use crate::{EffectId, LogIndex, SnapshotMetadata, SnapshotRecord, SnapshotRef};

/// The single in-flight local snapshot operation, if any.
///
/// The pipeline has four sequential phases:
///
/// ```text
/// Idle → Building → Persisting → Compacting → Idle
/// ```
///
/// Each phase corresponds to a distinct [`Effect`](crate::Effect) type, and
/// the core advances through the phases as the host reports completion.
pub(crate) enum LocalSnapshotState {
    /// No local snapshot operation is active.
    Idle,
    /// The host is building the immutable snapshot body through a specific
    /// applied log index.
    Building {
        /// The effect ID for this build operation.
        id: EffectId,
        /// The applied log index this snapshot will capture.
        through: LogIndex,
    },
    /// The snapshot metadata and reference have been validated and are now
    /// awaiting durable persistence before compaction can proceed.
    Persisting {
        /// The effect ID for this persist operation.
        id: EffectId,
        /// The validated snapshot record to persist.
        record: SnapshotRecord,
    },
    /// The snapshot is durable; the host may now remove the log prefix
    /// through the snapshot index.
    Compacting {
        /// The effect ID for this compaction.
        id: EffectId,
        /// The snapshot record whose prefix will be compacted.
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
///
/// Receives chunks from a leader's [`SnapshotSender`] via
/// [`Message::InstallSnapshot`](crate::Message::InstallSnapshot). Chunks must
/// arrive in order starting at offset 0. The receiver maintains a running
/// SHA-256 hash and verifies the final digest against the snapshot metadata.
pub(crate) struct SnapshotReceiver {
    metadata: SnapshotMetadata,
    /// The next byte offset expected from the leader.
    next_offset: u64,
    /// Running SHA-256 digest of all bytes received so far.
    hasher: Sha256,
    /// The most recently accepted chunk, stored for idempotent retry
    /// detection (the leader may resend the last chunk if the response is
    /// lost).
    last_chunk: Option<(u64, Vec<u8>)>,
    /// Whether the final chunk (`done == true`) has been received.
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
    ///
    /// Returns `Ok(true)` if the chunk was accepted (new data), `Ok(false)` if
    /// it was an idempotent retry (duplicate of the last chunk), or `Err(())`
    /// if the chunk is out of order or would exceed the advertised size.
    ///
    /// # Idempotent retry
    ///
    /// The leader may resend the last chunk if a
    /// [`StoreSnapshotChunk`](crate::Effect::StoreSnapshotChunk) response was
    /// lost. Rather than fail the transfer, the receiver accepts an exact
    /// repeat of `(offset, bytes)` from the most recently accepted chunk.
    /// This handles network retransmission without restarting the snapshot.
    pub(crate) fn accept(&mut self, offset: u64, bytes: &[u8], done: bool) -> Result<bool, ()> {
        // Idempotent retry: the leader resent the last chunk after a lost
        // response. Accept if (offset, bytes) exactly matches our last chunk.
        if offset < self.next_offset {
            return Ok(self.last_chunk.as_ref().is_some_and(|(previous, payload)| {
                *previous == offset && payload.as_slice() == bytes
            }));
        }
        // Reject out-of-order chunks (offset > next_offset) or chunks sent
        // after the final chunk was already received.
        if offset != self.next_offset || self.final_chunk {
            return Err(());
        }
        // Validate that this chunk (including all prior chunks) does not
        // exceed the advertised snapshot size.
        let length = u64::try_from(bytes.len()).map_err(|_| ())?;
        let next = offset.checked_add(length).ok_or(())?;
        if next > self.metadata.size() {
            return Err(());
        }
        // Feed the chunk into the running hash and advance the expected offset.
        self.hasher.update(bytes);
        self.next_offset = next;
        // Store the chunk for idempotent retry detection.
        self.last_chunk = Some((offset, bytes.to_vec()));
        self.final_chunk = done;
        Ok(true)
    }

    /// Returns whether all advertised bytes arrived with the expected digest.
    ///
    /// Three conditions must all hold:
    /// 1. The final chunk flag has been set (`done == true`).
    /// 2. All bytes have arrived (`next_offset == metadata.size`).
    /// 3. The SHA-256 hash of all received bytes matches the metadata digest.
    pub(crate) fn is_complete_and_valid(&self) -> bool {
        self.final_chunk
            && self.next_offset == self.metadata.size()
            && self.hasher.clone().finalize().as_slice() == self.metadata.digest().as_bytes()
    }

    /// Produces the durable record after the host returns a body reference.
    /// Called once after [`is_complete_and_valid`] returns `true`.
    pub(crate) fn record(&self, snapshot_ref: SnapshotRef) -> SnapshotRecord {
        SnapshotRecord::new(self.metadata.clone(), snapshot_ref)
    }
}

/// Leader-side cursor for one outbound snapshot body.
///
/// Tracks the current byte offset as chunks are read and sent. Created when
/// a follower's `next_index` falls before the leader's first available log
/// entry (entries compacted into a snapshot), and destroyed when the follower
/// acknowledges the snapshot installation.
pub(crate) struct SnapshotSender {
    /// The immutable snapshot being sent.
    snapshot: SnapshotRecord,
    /// The next byte offset to read from the snapshot body.
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
    /// Advances the cursor after a chunk was successfully read and sent.
    /// The host must have returned data at exactly this offset.
    pub(crate) fn advance(&mut self, bytes: usize) -> Result<(), ()> {
        self.offset = self
            .offset
            .checked_add(u64::try_from(bytes).map_err(|_| ())?)
            .ok_or(())?;
        Ok(())
    }
}
