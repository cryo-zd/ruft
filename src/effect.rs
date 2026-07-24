//! Effects emitted by the Raft core for host execution.
//!
//! Every [`Effect`] is a task the host must perform before the protocol can
//! advance. Correctness-critical effects (persist, apply, snapshot install)
//! act as asynchronous barriers: the core will not proceed past them until the
//! host reports a matching [`EffectCompleted`](crate::Event::EffectCompleted)
//! event with the correct [`EffectOutcome`].
//!
//! The host must execute effects in the order they are emitted within a single
//! `step` call, but may reorder or parallelize effects across different calls
//! as long as per-effect barrier semantics are preserved.

use crate::{
    Entry, HardState, LogIndex, NodeId, ProposalId, ReadId, SnapshotMetadata, SnapshotRecord,
    SnapshotRef,
};

/// The terminal outcome for one host proposal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProposalResult {
    /// The proposal was committed and applied at this log index.
    /// The host may return success to the client.
    Applied {
        /// The log index where the command was applied.
        index: LogIndex,
    },
    /// Leadership ended before this proposal could commit.
    /// The host should retry against the new leader (or redirect the client).
    LeadershipLost,
    /// The core could not accept the proposal at this time
    /// (for example, a persistence barrier is in flight or the command
    /// exceeds the per-RPC byte limit). The host may retry.
    Rejected,
}

/// Result reported after an asynchronous effect completes.
///
/// Each variant completes exactly one kind of [`Effect`]. The host must
/// supply the variant that matches the effect it executed; a mismatch is
/// rejected as an input error.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EffectOutcome {
    /// The host has durably persisted the batch.
    /// Completes a [`Effect::Persist`].
    Persisted,
    /// The host state machine applied committed entries through this index.
    /// Completes an [`Effect::Apply`].
    Applied {
        /// Highest log index that was applied.
        through: LogIndex,
    },
    /// The host read one chunk from a local snapshot body.
    /// Completes a [`Effect::ReadSnapshotChunk`].
    SnapshotChunkRead {
        /// The snapshot being read.
        snapshot_id: crate::SnapshotId,
        /// Byte offset of this chunk within the snapshot body.
        offset: u64,
        /// The chunk payload.
        bytes: Vec<u8>,
        /// Whether this is the final chunk of the snapshot.
        done: bool,
    },
    /// The host stored one chunk of an incoming snapshot.
    /// Completes a [`Effect::StoreSnapshotChunk`].
    SnapshotChunkStored {
        /// The snapshot being received.
        snapshot_id: crate::SnapshotId,
        /// The next byte offset the receiver expects.
        next_offset: u64,
        /// An opaque storage reference returned after the final chunk.
        snapshot_ref: Option<SnapshotRef>,
    },
    /// The host state machine installed a durable snapshot.
    /// Completes an [`Effect::InstallSnapshot`].
    SnapshotInstalled {
        /// The snapshot that was installed.
        snapshot_id: crate::SnapshotId,
    },
    /// The host built a local snapshot body and computed its metadata.
    /// Completes a [`Effect::BuildSnapshot`].
    SnapshotBuilt {
        /// Validated metadata for the built snapshot.
        metadata: SnapshotMetadata,
        /// Opaque storage reference to the snapshot body.
        snapshot_ref: SnapshotRef,
    },
    /// The host removed durable log entries through this index.
    /// Completes a [`Effect::CompactLog`].
    Compacted {
        /// Highest log index that was compacted away.
        through: LogIndex,
    },
    /// The host encountered a storage or state-machine error.
    /// Reported as a fatal error that stops the core.
    Failed,
}

/// A durable mutation requested from the host storage adapter.
///
/// The host must persist all three components atomically — partial persistence
/// (for example, entries written but hard state not updated) violates Raft
/// safety. When a component is `None` it requires no change; the host should
/// leave its existing durable state intact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PersistBatch<C> {
    /// Updated term, vote, and/or commit index to persist atomically.
    pub hard_state: Option<HardState>,
    /// New log entries to append to the durable suffix.
    pub entries: Vec<Entry<C>>,
    /// A new or updated snapshot record to persist.
    pub snapshot: Option<SnapshotRecord>,
}

/// A host-side action produced by the core.
///
/// Each effect must be executed by the host and its completion reported back
/// via [`Event::EffectCompleted`](crate::Event::EffectCompleted). Effects that
/// affect correctness ([`Persist`](Effect::Persist), [`Apply`](Effect::Apply),
/// snapshot operations) act as barriers — the core waits for the matching
/// outcome before advancing dependent protocol steps.
///
/// Effects that can fail ([`SendMessage`](Effect::SendMessage)) are retryable;
/// the host simply drops the send and the core will retry on the next
/// heartbeat tick. Storage and state-machine effects that fail must be
/// reported as [`EffectOutcome::Failed`], which stops the core permanently.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Effect<C> {
    /// Sends one Raft RPC through the host transport.
    ///
    /// Transport failures are retryable — the host may silently drop the
    /// message. The core will retry on the next heartbeat tick.
    SendMessage {
        /// The target node.
        to: NodeId,
        /// The Raft protocol message.
        message: crate::Message<C>,
    },
    /// Persists a Raft batch before dependent protocol actions proceed.
    ///
    /// The host must persist the batch atomically and durably before
    /// reporting completion. Until persistence is confirmed the core will
    /// not send dependent messages, advance commit, or apply entries.
    Persist {
        /// Unique identifier for this persist operation.
        id: crate::EffectId,
        /// The batch to persist atomically.
        batch: PersistBatch<C>,
    },
    /// Reports a terminal host proposal outcome.
    ///
    /// The host should use this to complete client proposals — either
    /// returning success (with the applied index) or signalling that the
    /// proposal must be retried.
    ProposalResult {
        /// The host-assigned proposal identifier.
        proposal_id: ProposalId,
        /// The terminal outcome.
        result: ProposalResult,
    },
    /// Releases a linearizable read after quorum confirmation and local apply.
    ///
    /// The host may now serve the read from its state machine at or beyond
    /// `read_index`. The read was confirmed by a quorum of followers
    /// acknowledging the leader's heartbeat within the current term.
    ReadReady {
        /// The host-assigned read request identifier.
        read_id: ReadId,
        /// The log index at or beyond which the read must be served.
        read_index: LogIndex,
    },
    /// Applies committed entries to the host state machine.
    ///
    /// Entries must be applied in index order without gaps. The host must
    /// report completion only after the state machine has durably applied
    /// all entries through the last entry's index.
    Apply {
        /// Unique identifier for this apply operation.
        id: crate::EffectId,
        /// Committed entries to apply, in ascending index order.
        entries: Vec<Entry<C>>,
    },
    /// Reads one bounded chunk from a local snapshot body for transmission.
    ///
    /// The host reads up to `max_len` bytes starting at `offset` from the
    /// snapshot body referenced by `snapshot`. Used by the leader to stream
    /// a snapshot to a lagging follower.
    ReadSnapshotChunk {
        /// Unique identifier for this chunk read.
        id: crate::EffectId,
        /// The durable snapshot record to read from.
        snapshot: SnapshotRecord,
        /// Byte offset within the snapshot body.
        offset: u64,
        /// Maximum number of bytes to read.
        max_len: usize,
    },
    /// Stores one validated incoming snapshot chunk.
    ///
    /// The host appends this chunk to its partial snapshot storage. When the
    /// final chunk (`done == true` in the corresponding InstallSnapshot
    /// message) has been stored, the host returns a [`SnapshotRef`] in the
    /// outcome so the core can persist and install the complete snapshot.
    StoreSnapshotChunk {
        /// Unique identifier for this chunk store.
        id: crate::EffectId,
        /// Metadata describing the snapshot being received.
        metadata: SnapshotMetadata,
        /// Byte offset of this chunk within the snapshot body.
        offset: u64,
        /// The chunk payload.
        bytes: Vec<u8>,
    },
    /// Installs a durable snapshot into the host state machine.
    ///
    /// The host replaces its current state machine state with the snapshot
    /// contents. After installation the applied index advances to at least
    /// the snapshot's `last_included_index`.
    InstallSnapshot {
        /// Unique identifier for this install operation.
        id: crate::EffectId,
        /// The durable snapshot record to install.
        record: SnapshotRecord,
    },
    /// Builds an externally stored snapshot body through an applied log index.
    ///
    /// The host creates a snapshot of its state machine at `through` and
    /// computes the corresponding [`SnapshotMetadata`] (including the SHA-256
    /// digest). The core validates the returned metadata before persisting
    /// and compacting.
    BuildSnapshot {
        /// Unique identifier for this build operation.
        id: crate::EffectId,
        /// The applied log index to snapshot through.
        through: LogIndex,
    },
    /// Removes a durable prefix after snapshot metadata is durable.
    ///
    /// The host may delete all log entries and any prior snapshots through
    /// `through` (inclusive). This is only emitted after the corresponding
    /// snapshot record has been persisted.
    CompactLog {
        /// Unique identifier for this compaction.
        id: crate::EffectId,
        /// The log index through which entries may be removed.
        through: LogIndex,
    },
    /// Emits an informational record for host observability.
    ///
    /// Carries no correctness semantics. The host may use this for logging,
    /// metrics, or debugging.
    Diagnostic {
        /// The local node identifier.
        node: NodeId,
    },
}
