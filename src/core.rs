//! Deterministic election state machine and effect-completion boundary.
//!
//! [`RaftCore`] owns every Raft protocol transition for one fixed-membership
//! group. The host drives it with [`Event`](crate::Event) values through
//! [`step`](RaftCore::step); each call yields [`Effect`](crate::Effect)
//! values that the host executes asynchronously and reports back as
//! [`Event::EffectCompleted`](crate::Event::EffectCompleted).
//!
//! Persistence is an explicit barrier: the `pending_persist` field blocks all
//! dependent transitions until the host confirms durability. Storage and
//! state-machine failures are fatal — the core stops permanently rather than
//! risking an unsafe continuation. Recovery requires validating durable state
//! and constructing a new core with a fresh effect generation.

use std::collections::{BTreeMap, BTreeSet};

use crate::progress::{Progress, QuorumTracker};
use crate::{
    Config, ConflictHint, Effect, EffectId, EffectOutcome, Entry, Event, FatalError, HardState,
    InvariantViolation, LogError, LogIndex, Message, PersistBatch, ProposalId, ProposalResult,
    RaftLog, ReadId, RecoveredState, SnapshotMetadata, SnapshotRecord, SnapshotRef, StoppedReason,
    Term, invariant,
    raft::{
        LocalSnapshotState, PrefixDecision, ReadRound, SnapshotReceiver, SnapshotSender,
        is_log_up_to_date, quorum_commit, rejected_next, validate_prefix,
    },
};

/// The local node role in the fixed-membership Raft election protocol.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Role {
    /// Passively receives AppendEntries and responds to vote requests.
    Follower,
    /// Solicits pre-votes for the next term without incrementing the durable term.
    /// PreVote prevents a partitioned node from disrupting an active cluster.
    PreCandidate,
    /// Solicits durable votes in the current (already incremented) term.
    Candidate,
    /// Accepts proposals, replicates log entries, and drives the commit index.
    Leader,
}

/// A copyable view of externally observable volatile Raft state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Status {
    local_id: crate::NodeId,
    term: Term,
    role: Role,
    stopped: bool,
    stopped_reason: Option<StoppedReason>,
}

impl Status {
    /// Returns the local node identifier.
    pub const fn local_id(&self) -> crate::NodeId {
        self.local_id
    }
    /// Returns the current durable term.
    pub const fn term(&self) -> Term {
        self.term
    }
    /// Returns the current election role.
    pub const fn role(&self) -> Role {
        self.role
    }
    /// Returns whether the core has permanently stopped.
    pub const fn is_stopped(&self) -> bool {
        self.stopped
    }
    /// Returns the first reason this core entered its stopped state.
    pub fn stopped_reason(&self) -> Option<StoppedReason> {
        self.stopped_reason.clone()
    }
}

/// A rejected host input that does not change core state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InputError {
    /// The message was addressed to a different node.
    WrongRecipient,
    /// The message belongs to a different Raft group.
    WrongCluster,
    /// The sender is not a member of this Raft group.
    UnknownSender,
    /// The effect belongs to a previous core generation (post-recovery).
    StaleEffectGeneration,
    /// The effect identifier does not match any outstanding effect.
    UnknownEffect,
    /// This effect has already been completed (duplicate report).
    AlreadyCompleted,
    /// A different outcome was previously reported for this effect.
    ConflictingEffectOutcome,
    /// The outcome variant does not match the effect type.
    InvalidEffectOutcome,
}

/// A failure while processing one serial Raft event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StepError {
    /// The host input was rejected without changing state.
    Input(InputError),
    /// A protocol counter would overflow.
    Arithmetic(crate::ArithmeticError),
    /// A log operation violated continuity or durability rules.
    Log(LogError),
    /// An entry's structural fields are invalid.
    Entry(crate::EntryError),
    /// The host state machine failed to apply committed entries.
    ApplyFailed,
    /// A snapshot operation produced invalid data.
    InvalidSnapshot,
    /// The host failed to build a local snapshot.
    SnapshotFailed,
    /// The host failed to compact the durable log.
    CompactionFailed,
    /// The host failed to persist a durability batch.
    PersistenceFailed,
    /// A correctness invariant was violated; the core has stopped.
    Fatal(FatalError),
    /// The core has already stopped due to a previous error or shutdown.
    Stopped(StoppedReason),
}

/// Effects and volatile-state notification emitted for one input event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StepOutput<C> {
    /// Effects the host must execute before the next event.
    pub effects: Vec<Effect<C>>,
    /// Whether the election role changed during this step.
    /// The host may use this to reset timers or update observability.
    pub soft_state_changed: bool,
}

// ── Private helper types ──────────────────────────────────────────────────

/// Aggregates the fields of an AppendEntries response for deferred sending
/// after persistence confirms the follower's state change.
struct AppendResponse {
    term: Term,
    success: bool,
    conflict: Option<ConflictHint>,
    match_index: LogIndex,
    /// Echoed read-round context from the leader's heartbeat.
    read_context: Option<Vec<u8>>,
}

/// What the core must do after a [`Persist`](Effect::Persist) completes.
///
/// This enum is the key to understanding the async effect model: every
/// `queue_persist` call pairs a durability batch with a continuation that
/// runs once the host reports `EffectOutcome::Persisted`. Different protocol
/// paths queue different continuations.
enum PersistContinuation {
    /// After persisting the new term and self-vote, broadcast RequestVote
    /// RPCs to all members.
    BroadcastVoteRequests,
    /// After persisting an updated term and granted vote, send the
    /// VoteResponse to the candidate.
    SendVoteResponse {
        to: crate::NodeId,
        term: Term,
        granted: bool,
    },
    /// After the leader no-op entry is durable, transition to Leader role
    /// and initialize per-follower progress trackers.
    ActivateLeader,
    /// After persisting entries from an AppendEntries, send the response
    /// back to the leader.
    SendAppendEntriesResponse {
        to: crate::NodeId,
        response: AppendResponse,
    },
    /// After persisting a client proposal, replicate it to all followers
    /// and check whether the commit index can advance.
    ReplicateProposal,
    /// After persisting an updated commit index, issue an Apply effect
    /// so newly committed entries reach the state machine.
    ApplyCommitted,
    /// No follow-up action is needed (for example, stepping down to a
    /// higher term without voting).
    None,
}

/// Tracks an in-flight [`Effect::StoreSnapshotChunk`] so the core can
/// validate the completion outcome and continue the snapshot pipeline.
struct PendingChunkStore {
    id: EffectId,
    /// Whether this was the final chunk of the snapshot transfer.
    done: bool,
    /// The leader that sent this snapshot.
    from: crate::NodeId,
    /// The term at the time the chunk was received.
    term: Term,
}

/// Post-receive pipeline for incoming snapshots.
///
/// After all chunks arrive and are stored, the snapshot record is first
/// persisted (with updated hard state), then installed into the state
/// machine. These two steps are serialised through distinct effects.
enum IncomingSnapshotState {
    /// The snapshot record and updated hard state are being persisted.
    Persisting {
        id: EffectId,
        record: SnapshotRecord,
        from: crate::NodeId,
        term: Term,
    },
    /// The durable snapshot is being installed into the state machine.
    Installing {
        id: EffectId,
        record: SnapshotRecord,
        from: crate::NodeId,
        term: Term,
    },
}

/// Tracks an in-flight [`Effect::Apply`] so only one apply is outstanding.
struct PendingApply {
    id: EffectId,
    /// The highest index this apply will cover.
    through: LogIndex,
}

/// Tracks an in-flight [`Effect::Persist`] and what to do after it completes.
struct PendingPersist {
    id: EffectId,
    /// The highest log index that will become stable after this persist
    /// (the last entry in the batch, if any).
    stable_through: Option<LogIndex>,
    /// The protocol step to execute once durability is confirmed.
    continuation: PersistContinuation,
}

// ── RaftCore ──────────────────────────────────────────────────────────────

/// A single-threaded Raft protocol core for a fixed member set.
///
/// `RaftCore` is generic over the command type `C`. Commands need not
/// implement `Clone`, `Serialize`, or `Send` — the host decides how to
/// encode, transport, and apply them. The core only compares entries for
/// equality during log conflict detection.
///
/// # Lifecycle
///
/// 1. Construct via [`new`](RaftCore::new) with validated [`Config`] and
///    [`RecoveredState`].
/// 2. Drive with [`step`](RaftCore::step) in a serial event loop.
/// 3. When [`is_stopped`](RaftCore::is_stopped) returns `true`, discard
///    the core and recover from durable state with a new generation.
pub struct RaftCore<C> {
    // ── Configuration and durable state ──
    /// Immutable validated settings (membership, limits, timing).
    config: Config,
    /// Durable term, vote, and commit index. Every mutation is persisted
    /// before dependent protocol steps proceed.
    hard_state: HardState,

    // ── Election state ──
    /// Current election role.
    role: Role,
    /// PreVote round tracker. `Some` only while in PreCandidate role.
    pre_votes: Option<QuorumTracker>,
    /// Durable vote round tracker. `Some` only while in Candidate role.
    votes: Option<QuorumTracker>,

    // ── Log and replication state ──
    /// In-memory logical log spanning the compacted snapshot boundary
    /// through the unstable suffix.
    log: RaftLog<C>,
    /// Cached last log index, kept in sync with `log.last_index()`.
    last_log_index: LogIndex,
    /// Cached term of the last log entry, kept in sync with the log.
    last_log_term: Term,
    /// Nodes that have responded since the last heartbeat tick. The leader
    /// uses this for CheckQuorum — if fewer than a quorum respond within
    /// `check_quorum_ticks`, the leader steps down.
    active_members: BTreeSet<crate::NodeId>,
    /// Per-follower replication state (match/next index, inflight window,
    /// state machine). Empty when not leader.
    progress: BTreeMap<crate::NodeId, Progress>,

    // ── Asynchronous barriers ──
    /// In-flight persistence barrier. At most one persist can be outstanding.
    /// While set, the core will not issue new persists, replicate proposals,
    /// activate as leader, or apply entries.
    pending_persist: Option<PendingPersist>,
    /// In-flight apply barrier. At most one apply can be outstanding.
    /// While set, the core will not issue new applies.
    pending_apply: Option<PendingApply>,

    // ── Application state ──
    /// Highest log index applied to the host state machine.
    applied_index: LogIndex,
    /// Maps log index → proposal IDs awaiting application. When entries
    /// through an index are applied, all proposals at or below that index
    /// are reported as `Applied`.
    proposals: BTreeMap<LogIndex, Vec<ProposalId>>,

    // ── Leader bookkeeping ──
    /// The index of the no-op entry the current leader appended on taking
    /// office. Reads are deferred until this entry commits (Raft paper
    /// §6.4 — the no-op proves the leader knows the current commit index).
    leader_noop_index: Option<LogIndex>,

    // ── Read-index state ──
    /// Read requests deferred because the leader no-op has not yet committed.
    pending_reads: Vec<ReadId>,
    /// The current in-flight linearizable read round, if any.
    read_round: Option<ReadRound>,
    /// Monotonic counter that makes each read-round context unique within
    /// a leader term. Combined with the current term to form the context.
    next_read_context: u64,

    // ── Local snapshot pipeline (build → persist → compact) ──
    local_snapshot: LocalSnapshotState,

    // ── Incoming snapshot pipeline (receive → persist → install) ──
    /// Active incoming snapshot receiver. Validates chunk ordering, size,
    /// and the final SHA-256 digest.
    incoming_snapshot: Option<SnapshotReceiver>,
    /// In-flight StoreSnapshotChunk effect for the current chunk.
    pending_chunk_store: Option<PendingChunkStore>,
    /// Post-receive state: first persist the record, then install.
    incoming_snapshot_state: Option<IncomingSnapshotState>,

    // ── Outgoing snapshot pipeline (read → send chunk → response) ──
    /// Per-follower cursors for streaming snapshot chunks.
    snapshot_senders: BTreeMap<crate::NodeId, SnapshotSender>,
    /// Maps in-flight ReadSnapshotChunk effect IDs to the target follower.
    /// At most one outstanding read per follower.
    pending_snapshot_reads: BTreeMap<EffectId, crate::NodeId>,

    // ── Snapshot boundary ──
    /// Highest log index covered by a durable snapshot. Used to suppress
    /// redundant local snapshot requests (don't build if nothing new).
    snapshot_index: LogIndex,

    // ── Effect tracking ──
    /// Monotonic sequence number for unique [`EffectId`] values within this
    /// core generation.
    next_effect_sequence: u64,

    // ── Lifecycle ──
    /// Whether the core has permanently stopped processing protocol work.
    stopped: bool,
    /// Why the core stopped (Shutdown or Fatal). The first reason is
    /// preserved; subsequent stop attempts are ignored.
    stopped_reason: Option<StoppedReason>,

    // ── Completion deduplication ──
    /// The highest contiguous completed effect sequence. Effects complete
    /// in order except for gaps tracked in `completed_sparse`.
    completed_frontier: u64,
    /// Sequence numbers of completed effects that arrived out of order.
    /// Fills gaps as the frontier advances.
    completed_sparse: BTreeSet<u64>,
    /// Recently completed outcomes, keyed by [`EffectId`], for deduplication
    /// and diagnostics. Bounded by `config.completion_history()`.
    completed_outcomes: BTreeMap<EffectId, EffectOutcome>,
    /// FIFO tracking of completion age. When the queue exceeds the history
    /// limit, the oldest entry is evicted from `completed_outcomes`.
    completion_order: std::collections::VecDeque<EffectId>,

    /// Phantom data so `C` is part of the type even though the core never
    /// owns a `C` value directly (commands live in `EntryPayload::Command`).
    _command: core::marker::PhantomData<C>,
}

// ── Public API ────────────────────────────────────────────────────────────

impl<C> RaftCore<C> {
    /// Restores a core from state that has already passed recovery validation.
    ///
    /// The new core starts as a [`Role::Follower`] regardless of its previous
    /// role — it will discover the current leader through heartbeats or start
    /// a new election when its timer fires.
    ///
    /// `applied_index` and `snapshot_index` are initialised from the recovered
    /// snapshot boundary (or zero if none exists). `last_log_index` and
    /// `last_log_term` reflect the last recovered entry or the snapshot
    /// boundary.
    pub fn new(config: Config, recovered: RecoveredState<C>) -> Result<Self, crate::InitError> {
        let hard_state = recovered.hard_state().clone();
        let log = RaftLog::from_recovered(&recovered);
        let (last_log_index, last_log_term) = recovered.entries().last().map_or_else(
            || {
                recovered
                    .snapshot()
                    .map_or((LogIndex::new(0), Term::new(0)), |snapshot| {
                        (snapshot.metadata().index(), snapshot.metadata().term())
                    })
            },
            |entry| (entry.index(), entry.term()),
        );
        Ok(Self {
            config,
            hard_state,
            role: Role::Follower,
            log,
            last_log_index,
            last_log_term,
            pre_votes: None,
            votes: None,
            active_members: BTreeSet::new(),
            progress: BTreeMap::new(),
            pending_persist: None,
            pending_apply: None,
            applied_index: recovered
                .snapshot()
                .map_or(LogIndex::new(0), |snapshot| snapshot.metadata().index()),
            proposals: BTreeMap::new(),
            leader_noop_index: None,
            pending_reads: Vec::new(),
            read_round: None,
            next_read_context: 0,
            local_snapshot: LocalSnapshotState::Idle,
            incoming_snapshot: None,
            pending_chunk_store: None,
            incoming_snapshot_state: None,
            snapshot_senders: BTreeMap::new(),
            pending_snapshot_reads: BTreeMap::new(),
            snapshot_index: recovered
                .snapshot()
                .map_or(LogIndex::new(0), |snapshot| snapshot.metadata().index()),
            next_effect_sequence: 0,
            stopped: false,
            stopped_reason: None,
            completed_frontier: 0,
            completed_sparse: BTreeSet::new(),
            completed_outcomes: BTreeMap::new(),
            completion_order: std::collections::VecDeque::new(),
            _command: core::marker::PhantomData,
        })
    }

    /// Applies one event and returns host work required by the transition.
    ///
    /// This is the sole entry point for driving the protocol state machine.
    /// The method:
    ///
    /// 1. If stopped, silently swallows ticks, messages, and shutdown; rejects
    ///    all other events with the stop reason.
    /// 2. Dispatches the event to the appropriate handler.
    /// 3. After the handler returns, checks whether the role changed from
    ///    Leader to non-Leader — if so, reports `LeadershipLost` for all
    ///    uncommitted proposals.
    /// 4. Validates Raft safety invariants. A violation is a fatal error.
    /// 5. Returns the accumulated effects and a flag indicating whether the
    ///    externally visible role changed.
    ///
    /// # Errors
    ///
    /// Host-failure step errors (persist, apply, snapshot, compaction) are
    /// converted to [`FatalError`] and stop the core. Other errors (input
    /// rejection, log conflicts) are returned to the host for handling.
    pub fn step(&mut self, event: Event<C>) -> Result<StepOutput<C>, StepError> {
        // Stopped cores silently accept ticks, messages, and shutdown so the
        // host can drain its event queue without special-casing. All other
        // events are rejected with the stop reason.
        if self.stopped {
            return match event {
                Event::Shutdown | Event::Tick(_) | Event::MessageReceived(_) => Ok(StepOutput {
                    effects: Vec::new(),
                    soft_state_changed: false,
                }),
                _ => Err(StepError::Stopped(
                    self.stopped_reason
                        .clone()
                        .expect("stopped cores have a reason"),
                )),
            };
        }

        let previous_role = self.role;
        let mut effects = Vec::new();
        let result = (|| {
            match event {
                Event::Tick(crate::TickKind::Election) => self.start_pre_vote(&mut effects)?,
                Event::Tick(crate::TickKind::Heartbeat) => self.on_heartbeat_tick(&mut effects)?,
                Event::MessageReceived(envelope) => {
                    self.validate_envelope(&envelope)?;
                    // Record the sender as active for CheckQuorum.
                    if self.role == Role::Leader {
                        self.active_members.insert(envelope.from());
                    }
                    self.on_message(envelope.from(), envelope.message(), &mut effects)?;
                }
                Event::EffectCompleted { id, outcome } => {
                    self.validate_completion(id, &outcome)?;
                    self.on_effect_completed(id, outcome.clone(), &mut effects)?;
                    self.record_completion(id, outcome);
                }
                Event::Shutdown => self.stop(StoppedReason::Shutdown),
                Event::Propose {
                    proposal_id,
                    command,
                    encoded_len,
                } => {
                    self.on_propose(proposal_id, command, encoded_len, &mut effects)?;
                }
                Event::Read { read_id, .. } => self.on_read(read_id, &mut effects)?,
                Event::SnapshotRequested => self.request_local_snapshot(&mut effects)?,
            }
            // After processing the event, check whether newly committed
            // entries can be applied (may have been unblocked by a persist
            // completion or commit advancement).
            self.emit_apply_if_needed(&mut effects)?;
            Ok(())
        })();
        if let Err(error) = result {
            return Err(self.fatalize(error));
        }
        // On losing leadership, fail all proposals that haven't committed.
        // This is done after the handler so proposals appended in this step
        // are also covered.
        if previous_role == Role::Leader && self.role != Role::Leader {
            self.fail_uncommitted_proposals(&mut effects);
        }
        // Guard: every state transition must preserve Raft safety invariants.
        if let Err(violation) = self.validate_invariants() {
            return Err(self.fatal(FatalError::Invariant(violation)));
        }
        Ok(StepOutput {
            effects,
            soft_state_changed: previous_role != self.role,
        })
    }

    /// Returns the first log index still available as an entry.
    ///
    /// This is the snapshot boundary + 1, or 1 if no snapshot exists. Entries
    /// at or below `first_log_index - 1` have been compacted.
    pub fn first_log_index(&self) -> LogIndex {
        self.log.first_index()
    }

    /// Returns the highest index applied to the local state machine.
    pub const fn applied_index(&self) -> LogIndex {
        self.applied_index
    }

    /// Returns leader-side progress for one remote member, when this node is
    /// leader. Returns `None` if this node is not the leader.
    pub fn progress(&self, node: crate::NodeId) -> Option<&Progress> {
        self.progress.get(&node)
    }

    /// Returns a snapshot of local volatile election state.
    pub fn status(&self) -> Status {
        Status {
            local_id: self.config.local_id(),
            term: self.hard_state.current_term(),
            role: self.role,
            stopped: self.stopped,
            stopped_reason: self.stopped_reason.clone(),
        }
    }

    /// Returns whether this instance has stopped processing protocol work.
    ///
    /// Once stopped, the core should be discarded. Recovery requires
    /// validating durable state and constructing a new core with a fresh
    /// effect generation.
    pub const fn is_stopped(&self) -> bool {
        self.stopped
    }

    // ── Invariants and lifecycle ──────────────────────────────────────────

    /// Verifies Raft safety invariants that must hold after every accepted
    /// state transition. Violations indicate a correctness bug and are fatal.
    fn validate_invariants(&self) -> Result<(), InvariantViolation> {
        invariant::validate(
            &self.hard_state,
            &self.log,
            self.applied_index,
            self.last_log_index,
            self.last_log_term,
        )
    }

    /// Marks the core permanently stopped. The first reason is preserved;
    /// subsequent calls have no effect.
    fn stop(&mut self, reason: StoppedReason) {
        if self.stopped_reason.is_none() {
            self.stopped_reason = Some(reason);
        }
        self.stopped = true;
    }

    /// Records a fatal error, stops the core, and returns the corresponding
    /// [`StepError::Fatal`].
    fn fatal(&mut self, error: FatalError) -> StepError {
        self.stop(StoppedReason::Fatal(error.clone()));
        StepError::Fatal(error)
    }

    /// Converts host-failure step errors into fatal errors.
    ///
    /// Storage failures (persist, compaction) become [`FatalError::Storage`].
    /// State-machine failures (apply, snapshot build/install) become
    /// [`FatalError::StateMachine`]. All other errors pass through unchanged.
    fn fatalize(&mut self, error: StepError) -> StepError {
        match error {
            StepError::PersistenceFailed | StepError::CompactionFailed => {
                self.fatal(FatalError::Storage)
            }
            StepError::ApplyFailed | StepError::SnapshotFailed => {
                self.fatal(FatalError::StateMachine)
            }
            error => error,
        }
    }

    // ── Effect completion tracking ────────────────────────────────────────

    /// Validates that an effect completion is legitimate.
    ///
    /// Rejects completions that are:
    /// - From a previous core generation (stale after recovery),
    /// - Already completed (duplicate),
    /// - Conflicting with a previously reported outcome for the same effect.
    fn validate_completion(&self, id: EffectId, outcome: &EffectOutcome) -> Result<(), StepError> {
        if id.generation() != self.config.generation() {
            return Err(StepError::Input(InputError::StaleEffectGeneration));
        }
        if let Some(previous) = self.completed_outcomes.get(&id) {
            return Err(StepError::Input(if previous == outcome {
                InputError::AlreadyCompleted
            } else {
                InputError::ConflictingEffectOutcome
            }));
        }
        if id.sequence() <= self.completed_frontier
            || self.completed_sparse.contains(&id.sequence())
        {
            return Err(StepError::Input(InputError::AlreadyCompleted));
        }
        Ok(())
    }

    /// Records a validated completion and advances the completion frontier.
    ///
    /// Uses a frontier + sparse-set data structure: the frontier tracks the
    /// highest *contiguous* completed sequence. Out-of-order completions are
    /// stored in `completed_sparse` and merged into the frontier as gaps are
    /// filled. This gives O(1) frontier queries and O(log n) sparse lookups.
    fn record_completion(&mut self, id: EffectId, outcome: EffectOutcome) {
        self.completed_outcomes.insert(id, outcome);
        self.completion_order.push_back(id);
        // If this sequence is exactly frontier + 1, advance the frontier.
        // Then check whether the next sequence is in the sparse set — if so,
        // merge it into the frontier and repeat.
        if id.sequence() == self.completed_frontier.saturating_add(1) {
            self.completed_frontier = id.sequence();
            while self
                .completed_sparse
                .remove(&self.completed_frontier.saturating_add(1))
            {
                self.completed_frontier = self.completed_frontier.saturating_add(1);
            }
        } else {
            self.completed_sparse.insert(id.sequence());
        }
        // Evict the oldest outcomes when the history exceeds the configured
        // limit, preventing unbounded memory growth.
        while self.completion_order.len() > self.config.completion_history() {
            let expired = self
                .completion_order
                .pop_front()
                .expect("history is nonempty");
            self.completed_outcomes.remove(&expired);
        }
    }

    // ── Local snapshot pipeline ───────────────────────────────────────────

    /// Requests a local snapshot at the current applied index.
    ///
    /// No-op if a snapshot build is already in progress or if the applied
    /// index hasn't advanced past the current snapshot boundary.
    fn request_local_snapshot(&mut self, effects: &mut Vec<Effect<C>>) -> Result<(), StepError> {
        if !self.local_snapshot.is_idle() || self.applied_index <= self.snapshot_index {
            return Ok(());
        }
        let through = self.applied_index;
        let id = self.next_effect_id()?;
        self.local_snapshot = LocalSnapshotState::Building { id, through };
        effects.push(Effect::BuildSnapshot { id, through });
        Ok(())
    }

    /// Triggers a local snapshot if the entry-count threshold has been
    /// reached since the last snapshot.
    fn maybe_request_local_snapshot(
        &mut self,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        let applied_since_snapshot = self
            .applied_index
            .get()
            .saturating_sub(self.snapshot_index.get());
        if applied_since_snapshot >= self.config.snapshot_after_entries() as u64 {
            self.request_local_snapshot(effects)?;
        }
        Ok(())
    }

    /// Checks that a host-built snapshot matches the log at its boundary.
    ///
    /// The snapshot must target the requested index, have the correct term at
    /// that index, and contain the current member set. Any mismatch indicates
    /// a host bug or a race in snapshot construction.
    fn validate_local_snapshot(
        &self,
        metadata: SnapshotMetadata,
        snapshot_ref: SnapshotRef,
        through: LogIndex,
    ) -> Result<SnapshotRecord, StepError> {
        if metadata.index() != through
            || metadata.term() != self.log.term(through).map_err(StepError::Log)?
            || metadata.members() != self.config.members()
        {
            return Err(StepError::InvalidSnapshot);
        }
        Ok(SnapshotRecord::new(metadata, snapshot_ref))
    }

    // ── Proposal handling ─────────────────────────────────────────────────

    /// Handles a client proposal.
    ///
    /// If leader, appends one entry at `last_log_index + 1` in the current
    /// term, maps the proposal ID to that index, and persists the batch.
    /// Non-leaders reject immediately with [`ProposalResult::LeadershipLost`].
    /// Rejects if a persistence barrier is in flight (avoid unbounded
    /// buffering) or the command exceeds the per-RPC byte limit.
    fn on_propose(
        &mut self,
        proposal_id: ProposalId,
        command: C,
        encoded_len: usize,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        if self.role != Role::Leader {
            effects.push(Effect::ProposalResult {
                proposal_id,
                result: ProposalResult::LeadershipLost,
            });
            return Ok(());
        }
        // Reject if a persist is in flight (to bound in-memory buffering)
        // or if the command alone exceeds the per-RPC byte budget.
        if self.pending_persist.is_some() || encoded_len > self.config.max_bytes_per_rpc() {
            effects.push(Effect::ProposalResult {
                proposal_id,
                result: ProposalResult::Rejected,
            });
            return Ok(());
        }
        let index = self
            .last_log_index
            .checked_next()
            .map_err(StepError::Arithmetic)?;
        let entry = Entry::command(index, self.hard_state.current_term(), command, encoded_len)
            .map_err(StepError::Entry)?;
        self.log
            .append(vec![entry.clone()])
            .map_err(StepError::Log)?;
        self.last_log_index = index;
        self.last_log_term = self.hard_state.current_term();
        self.proposals.entry(index).or_default().push(proposal_id);
        // Persist before replicating: the entry must be durable on the leader
        // before it can appear in followers' logs via replication.
        self.queue_persist(
            PersistBatch {
                hard_state: None,
                entries: vec![entry],
                snapshot: None,
            },
            PersistContinuation::ReplicateProposal,
            effects,
        )
    }

    // ── Read-index handling ────────────────────────────────────────────────

    /// Initiates or queues a linearizable read request.
    ///
    /// Reads are deferred until the leader's no-op entry commits (Raft §6.4).
    /// Once the no-op is committed, the read either joins the current
    /// in-flight read round (batching) or starts a new one.
    fn on_read(&mut self, read_id: ReadId, effects: &mut Vec<Effect<C>>) -> Result<(), StepError> {
        if self.role != Role::Leader {
            return Ok(());
        }
        // Reject if the pending-read queue is full.
        if self.pending_reads.len()
            + self
                .read_round
                .as_ref()
                .map_or(0, |round| round.request_count())
            >= self.config.max_pending_reads()
        {
            return Ok(());
        }
        // Defer reads until the leader's no-op entry is committed.
        // The no-op proves the leader knows all previously committed entries
        // and can safely assert a read index.
        if !self.current_leader_noop_is_committed() {
            self.pending_reads.push(read_id);
            return Ok(());
        }
        // Batch reads into the current round when one is already in flight.
        if let Some(round) = self.read_round.as_mut() {
            round.push(read_id);
            return Ok(());
        }
        self.start_read_round(read_id, effects)
    }

    /// Returns true only when the current leader's no-op entry has committed.
    /// This is the precondition for serving linearizable reads.
    fn current_leader_noop_is_committed(&self) -> bool {
        self.leader_noop_index
            .is_some_and(|index| index <= self.hard_state.commit_index())
    }

    /// Begins a new [`ReadRound`] with a unique context.
    ///
    /// The context is `current_term || counter`, making it unique per
    /// leader term. If the leader already holds a quorum (single-node
    /// cluster), the read is released immediately.
    fn start_read_round(
        &mut self,
        read_id: ReadId,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        self.next_read_context = self
            .next_read_context
            .checked_add(1)
            .ok_or(StepError::Arithmetic(crate::ArithmeticError::Overflow))?;
        let mut context = Vec::with_capacity(16);
        context.extend_from_slice(&self.hard_state.current_term().get().to_be_bytes());
        context.extend_from_slice(&self.next_read_context.to_be_bytes());
        self.read_round = Some(ReadRound::new(
            context,
            self.config.members(),
            self.config.local_id(),
            read_id,
        ));
        // Single-node cluster: the local vote already forms a quorum.
        if self.read_round.as_ref().is_some_and(ReadRound::has_quorum) {
            let round = self.read_round.as_mut().expect("round was just created");
            round.set_safe_index(self.hard_state.commit_index());
            self.release_read_round(effects);
            self.start_pending_reads(effects)?;
            return Ok(());
        }
        // Send heartbeat probes carrying the read context to all followers.
        self.send_read_heartbeats(effects)
    }

    /// Drains one pending read into a new read round when preconditions are met.
    fn start_pending_reads(&mut self, effects: &mut Vec<Effect<C>>) -> Result<(), StepError> {
        if self.role != Role::Leader
            || !self.current_leader_noop_is_committed()
            || self.read_round.is_some()
            || self.pending_reads.is_empty()
        {
            return Ok(());
        }
        let read_id = self.pending_reads.remove(0);
        self.start_read_round(read_id, effects)
    }

    /// Records a follower's read-index acknowledgement.
    ///
    /// Only acknowledgements matching the current round's context count.
    /// Once quorum is reached, the current commit index is captured as the
    /// safe read index. Reads are released once the local applied index
    /// reaches that safe index.
    fn acknowledge_read_context(
        &mut self,
        from: crate::NodeId,
        context: &[u8],
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        let Some(round) = self.read_round.as_mut() else {
            return Ok(());
        };
        // Ignore acknowledgements from a previous read round.
        if round.context() != context {
            return Ok(());
        }
        round.acknowledge(from);
        // Capture the commit index when quorum is first reached. This is the
        // lowest index at which all reads in this round are safe to serve.
        if round.has_quorum() && round.safe_index().is_none() {
            round.set_safe_index(self.hard_state.commit_index());
        }
        self.release_read_round(effects);
        self.start_pending_reads(effects)
    }

    /// Releases ready reads once the local applied index has reached the
    /// round's safe index. This ensures reads observe all entries that were
    /// committed when the read was confirmed.
    fn release_read_round(&mut self, effects: &mut Vec<Effect<C>>) {
        let ready = self
            .read_round
            .as_ref()
            .and_then(ReadRound::safe_index)
            .is_some_and(|index| self.applied_index >= index);
        if !ready {
            return;
        }
        let round = self.read_round.take().expect("ready round exists");
        let index = round.safe_index().expect("ready round has safe index");
        for read_id in round.into_requests() {
            effects.push(Effect::ReadReady {
                read_id,
                read_index: index,
            });
        }
    }

    // ── Message dispatch ──────────────────────────────────────────────────

    /// Routes one validated RPC to the appropriate protocol handler.
    fn on_message(
        &mut self,
        from: crate::NodeId,
        message: &Message<C>,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        match message {
            Message::PreVote {
                term,
                last_log_index,
                last_log_term,
            } => {
                // PreVote uses the same log-completeness check as RequestVote
                // but does not persist a term change or clear voted_for.
                let granted = *term > self.hard_state.current_term()
                    && is_log_up_to_date(
                        *last_log_index,
                        *last_log_term,
                        self.last_log_index,
                        self.last_log_term,
                    );
                self.send_pre_vote_response(from, granted, effects);
            }
            Message::RequestVote {
                term,
                last_log_index,
                last_log_term,
            } => {
                self.on_vote_request(from, *term, *last_log_index, *last_log_term, effects)?;
            }
            Message::PreVoteResponse { term, granted } => {
                self.on_pre_vote_response(from, *term, *granted, effects)?
            }
            Message::VoteResponse { term, granted } => {
                self.on_vote_response(from, *term, *granted, effects)?
            }
            Message::AppendEntries { .. } => self.on_append_entries(from, message, effects)?,
            Message::InstallSnapshot { .. } => {
                self.on_install_snapshot_chunk(from, message, effects)?
            }
            Message::InstallSnapshotResponse { term, success } => {
                self.on_install_snapshot_response(from, *term, *success, effects)?
            }
            Message::AppendEntriesResponse { .. } => {
                self.on_append_entries_response(from, message, effects)?
            }
            // Heartbeat messages require no response. The leader sent an
            // empty AppendEntries to assert leadership; the follower has
            // already processed the term check via AppendEntries handling.
            Message::Heartbeat => {}
        }
        Ok(())
    }

    // ── Incoming snapshot handling ────────────────────────────────────────

    /// Handles a follower's InstallSnapshot response.
    ///
    /// On success, sets the follower's progress to the snapshot boundary + 1
    /// (in Probe state) and resumes replication. On a higher term, steps down.
    fn on_install_snapshot_response(
        &mut self,
        from: crate::NodeId,
        term: Term,
        success: bool,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        if term > self.hard_state.current_term() {
            return self.begin_term_transition(term, PersistContinuation::None, effects);
        }
        if self.role != Role::Leader || term != self.hard_state.current_term() || !success {
            return Ok(());
        }
        let Some(sender) = self.snapshot_senders.remove(&from) else {
            return Ok(());
        };
        // After snapshot install, the follower's log starts at the snapshot
        // boundary + 1. Reset progress to Probe at that point.
        let next = sender
            .snapshot()
            .metadata()
            .index()
            .checked_next()
            .map_err(StepError::Arithmetic)?;
        if let Some(progress) = self.progress.get_mut(&from) {
            progress.restore_probe(next);
        }
        self.replicate_to(from, effects)
    }

    /// Handles one incoming InstallSnapshot chunk.
    ///
    /// Validates chunk ordering, size, membership, and idempotent retries.
    /// The last chunk triggers the persist→install pipeline. Chunks from a
    /// lower term are rejected; chunks with a mismatched member set or an
    /// already-installed snapshot boundary are silently ignored (idempotent).
    fn on_install_snapshot_chunk(
        &mut self,
        from: crate::NodeId,
        message: &Message<C>,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        let Message::InstallSnapshot {
            term,
            metadata,
            offset,
            bytes,
            done,
        } = message
        else {
            return Ok(());
        };
        let term = *term;
        let offset = *offset;
        let done = *done;
        // Reject with our current term so a stale leader can step down.
        if term < self.hard_state.current_term() {
            effects.push(Effect::SendMessage {
                to: from,
                message: Message::InstallSnapshotResponse {
                    term: self.hard_state.current_term(),
                    success: false,
                },
            });
            return Ok(());
        }
        // Guard: chunk size must respect the configured limit.
        if bytes.len() > self.config.snapshot_chunk_bytes() {
            return Err(StepError::InvalidSnapshot);
        }
        // Silently ignore snapshots with a mismatched member set or an
        // already-installed boundary (idempotent retry from a leader that
        // hasn't yet received our InstallSnapshotResponse).
        if metadata.members() != self.config.members() || metadata.index() <= self.snapshot_index {
            return Ok(());
        }
        // Start a new receiver for a new snapshot ID, or validate the chunk
        // against the existing receiver. A nonzero offset without a receiver
        // indicates a lost first chunk.
        if self
            .incoming_snapshot
            .as_ref()
            .is_none_or(|receiver| receiver.metadata().id() != metadata.id())
        {
            if offset != 0 || self.pending_chunk_store.is_some() {
                return Err(StepError::InvalidSnapshot);
            }
            self.incoming_snapshot = Some(SnapshotReceiver::new(metadata.clone()));
        }
        let receiver = self
            .incoming_snapshot
            .as_mut()
            .expect("receiver was initialized");
        if receiver.metadata() != metadata {
            return Err(StepError::InvalidSnapshot);
        }
        // `accept` returns `Ok(true)` for a new chunk, `Ok(false)` for an
        // idempotent retry of the last chunk (network retransmission), and
        // `Err(())` for an out-of-order or oversized chunk.
        if !receiver
            .accept(offset, bytes, done)
            .map_err(|_| StepError::InvalidSnapshot)?
        {
            return Ok(());
        }
        let id = self.next_effect_id()?;
        self.pending_chunk_store = Some(PendingChunkStore {
            id,
            done,
            from,
            term,
        });
        effects.push(Effect::StoreSnapshotChunk {
            id,
            metadata: metadata.clone(),
            offset,
            bytes: bytes.to_vec(),
        });
        Ok(())
    }

    // ── AppendEntries handling ────────────────────────────────────────────

    /// Handles an AppendEntries request (follower side).
    ///
    /// 1. **Term check**: if the leader's term is lower, reject immediately.
    /// 2. **Become follower**: a valid leader at the current or higher term
    ///    resets the election timer.
    /// 3. **Log consistency**: validate the previous-log proof. On match,
    ///    merge the leader's entries; on conflict, report a `ConflictHint`.
    /// 4. **Commit**: advance the local commit to `min(leader_commit, last_index)`.
    ///    We cannot commit beyond our own log.
    /// 5. **Persist**: if anything changed (term, commit, or entries), persist
    ///    before responding. Otherwise respond immediately.
    fn on_append_entries(
        &mut self,
        from: crate::NodeId,
        message: &Message<C>,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        let Message::AppendEntries {
            term,
            prev_log_index,
            prev_log_term,
            leader_commit,
            read_context,
            entries,
        } = message
        else {
            return Ok(());
        };
        let term = *term;
        let prev_log_index = *prev_log_index;
        let prev_log_term = *prev_log_term;
        let leader_commit = *leader_commit;
        let current_term = self.hard_state.current_term();
        // Reject if the leader is in a lower term. The real leader will
        // step down on receiving this response.
        if term < current_term {
            self.send_append_entries_response(
                from,
                AppendResponse {
                    term: current_term,
                    success: false,
                    conflict: None,
                    match_index: prev_log_index,
                    read_context: read_context.clone(),
                },
                effects,
            );
            return Ok(());
        }

        // A valid AppendEntries from a current or higher term resets the
        // election timer by reaffirming follower status.
        self.become_follower();
        let term_changed = term > current_term;
        let decision =
            validate_prefix(&self.log, prev_log_index, prev_log_term).map_err(StepError::Log)?;
        let (success, conflict, appended) = match decision {
            PrefixDecision::Reject(conflict) => (false, Some(conflict), Vec::new()),
            PrefixDecision::Match => {
                // Log prefix matches. Merge the leader's suffix and advance
                // the local commit index (capped at our last log index — we
                // cannot commit entries we don't yet have).
                let appended = self
                    .log
                    .merge_from_leader(entries)
                    .map_err(StepError::Log)?;
                let commit = core::cmp::min(leader_commit, self.log.last_index());
                self.log.commit_to(commit).map_err(StepError::Log)?;
                self.last_log_index = self.log.last_index();
                self.last_log_term = self.log.term(self.last_log_index).map_err(StepError::Log)?;
                (true, None, appended)
            }
        };

        // Update hard state if term or commit changed. Clear voted_for when
        // adopting a higher term.
        let commit_changed = self.hard_state.commit_index() != self.log.committed_index();
        if term_changed || commit_changed {
            self.hard_state = HardState::new(
                term,
                if term_changed {
                    None
                } else {
                    self.hard_state.voted_for()
                },
                self.log.committed_index(),
            );
        }
        // Persist before responding when state changed. Defer the response
        // via PersistContinuation so durability is confirmed first.
        if term_changed || commit_changed || !appended.is_empty() {
            self.queue_persist(
                PersistBatch {
                    hard_state: (term_changed || commit_changed).then(|| self.hard_state.clone()),
                    entries: appended,
                    snapshot: None,
                },
                PersistContinuation::SendAppendEntriesResponse {
                    to: from,
                    response: AppendResponse {
                        term: self.hard_state.current_term(),
                        success,
                        conflict,
                        match_index: if success {
                            self.log.last_index()
                        } else {
                            prev_log_index
                        },
                        read_context: read_context.clone(),
                    },
                },
                effects,
            )?;
        } else {
            // Nothing to persist — respond immediately.
            self.send_append_entries_response(
                from,
                AppendResponse {
                    term: self.hard_state.current_term(),
                    success,
                    conflict,
                    match_index: if success {
                        self.log.last_index()
                    } else {
                        prev_log_index
                    },
                    read_context: read_context.clone(),
                },
                effects,
            );
        }
        Ok(())
    }

    /// Handles an AppendEntries response (leader side).
    ///
    /// On success, advances the follower's match index and transitions from
    /// Probe to Replicate. On rejection, uses the `ConflictHint` to jump
    /// backward efficiently (skipping entire conflicting terms). On a higher
    /// term, steps down.
    fn on_append_entries_response(
        &mut self,
        from: crate::NodeId,
        message: &Message<C>,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        let Message::AppendEntriesResponse {
            term,
            success,
            match_index,
            conflict,
            read_context,
        } = message
        else {
            return Ok(());
        };
        let term = *term;
        let success = *success;
        let match_index = *match_index;
        let conflict = *conflict;
        let read_context = read_context.as_deref();
        // A higher term in the response means this leader is stale.
        // Step down and persist the new term.
        if term > self.hard_state.current_term() {
            return self.begin_term_transition(term, PersistContinuation::None, effects);
        }
        if self.role != Role::Leader || term != self.hard_state.current_term() {
            return Ok(());
        }
        // On rejection, compute the next probe index using the conflict hint.
        // `rejected_next` implements the Raft log-conflict optimization:
        // skip all entries of the conflicting term in one step.
        let next = if success {
            None
        } else {
            Some(
                rejected_next(
                    &self.log,
                    conflict.unwrap_or(ConflictHint::new(match_index, None)),
                )
                .map_err(StepError::Log)?,
            )
        };
        let Some(progress) = self.progress.get_mut(&from) else {
            return Ok(());
        };
        let changed = match next {
            None => progress.acknowledged(core::cmp::min(match_index, self.log.last_index())),
            Some(next_index) => progress.reject(match_index, next_index),
        };
        // If the follower echoed our read context in a successful response,
        // count it toward read-index quorum.
        if let Some(context) = read_context {
            if success {
                self.acknowledge_read_context(from, context, effects)?;
            }
        }
        // Only continue replicating and check commit if progress changed.
        if changed {
            self.replicate_to(from, effects)?;
            self.advance_commit(effects)?;
        }
        Ok(())
    }

    /// Calculates a new commit index via quorum match of current-term entries.
    ///
    /// Collects every follower's `match_index` plus the leader's own
    /// `last_index`, sorts them, and takes the element at position
    /// `n - quorum` (the Nth largest). An index commits only if its term
    /// equals the current term — Raft never commits entries from prior terms
    /// directly; they commit indirectly when a current-term entry commits
    /// (Raft paper §5.4.2).
    fn advance_commit(&mut self, effects: &mut Vec<Effect<C>>) -> Result<(), StepError> {
        if self.pending_persist.is_some() {
            return Ok(());
        }
        let Some(commit) = quorum_commit(
            &self.log,
            &self.progress,
            self.config.quorum(),
            self.hard_state.current_term(),
        )
        .map_err(StepError::Log)?
        else {
            return Ok(());
        };
        self.log.commit_to(commit).map_err(StepError::Log)?;
        self.hard_state = HardState::new(
            self.hard_state.current_term(),
            self.hard_state.voted_for(),
            commit,
        );
        // Persist the updated commit index before applying newly committed
        // entries. The commit point must be durable before the state machine
        // acts on it.
        self.queue_persist(
            PersistBatch {
                hard_state: Some(self.hard_state.clone()),
                entries: Vec::new(),
                snapshot: None,
            },
            PersistContinuation::ApplyCommitted,
            effects,
        )
    }

    // ── Vote handling ─────────────────────────────────────────────────────

    /// Handles a RequestVote RPC.
    ///
    /// Implements at-most-one-vote-per-term and the leader completeness
    /// property (log must be at least as up-to-date). Three cases:
    ///
    /// 1. `term < current`: reject immediately.
    /// 2. `term > current`: become follower, adopt the higher term, grant vote
    ///    if log is current, and persist before responding.
    /// 3. `term == current`: grant only if we haven't voted for another node
    ///    in this term, and only if the candidate's log is current.
    fn on_vote_request(
        &mut self,
        from: crate::NodeId,
        term: Term,
        index: LogIndex,
        log_term: Term,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        let current = self.hard_state.current_term();
        // Reject votes from lower-term candidates.
        if term < current {
            self.send_vote_response(from, current, false, effects);
            return Ok(());
        }
        // Check the leader completeness property: the candidate's log must
        // be at least as up-to-date as ours.
        let log_is_current =
            is_log_up_to_date(index, log_term, self.last_log_index, self.last_log_term);
        if term > current {
            // Higher term: reset to follower, adopt the term, and vote.
            self.role = Role::Follower;
            self.pre_votes = None;
            self.votes = None;
            let voted_for = log_is_current.then_some(from);
            self.hard_state = HardState::new(term, voted_for, self.hard_state.commit_index());
            self.queue_persist(
                PersistBatch {
                    hard_state: Some(self.hard_state.clone()),
                    entries: Vec::new(),
                    snapshot: None,
                },
                PersistContinuation::SendVoteResponse {
                    to: from,
                    term,
                    granted: log_is_current,
                },
                effects,
            )?;
            return Ok(());
        }
        // Same term: grant only if we haven't already voted for another node.
        let granted = log_is_current && self.hard_state.voted_for().is_none_or(|node| node == from);
        if granted && self.hard_state.voted_for().is_none() {
            // First vote in this term — persist before responding.
            self.hard_state = HardState::new(term, Some(from), self.hard_state.commit_index());
            self.queue_persist(
                PersistBatch {
                    hard_state: Some(self.hard_state.clone()),
                    entries: Vec::new(),
                    snapshot: None,
                },
                PersistContinuation::SendVoteResponse {
                    to: from,
                    term,
                    granted,
                },
                effects,
            )?;
        } else {
            self.send_vote_response(from, term, granted, effects);
        }
        Ok(())
    }

    // ── PreVote response handling ─────────────────────────────────────────

    /// Handles a PreVote response.
    ///
    /// Records the grant in the PreVote quorum tracker. On quorum, advances
    /// to Candidate. If the round cannot win, returns to Follower. Unlike
    /// VoteResponse, this does not touch durable state.
    fn on_pre_vote_response(
        &mut self,
        from: crate::NodeId,
        term: Term,
        granted: bool,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        if term > self.hard_state.current_term() {
            return self.begin_term_transition(term, PersistContinuation::None, effects);
        }
        if self.role != Role::PreCandidate {
            return Ok(());
        }
        let Some(votes) = self.pre_votes.as_mut() else {
            return Ok(());
        };
        votes.record(from, granted);
        if votes.has_quorum() {
            // PreVote quorum achieved — advance to Candidate. This is the
            // first step that increments the durable term.
            self.become_candidate(effects)?;
        } else if votes.cannot_win() {
            // The round is hopeless — return to Follower and wait for the
            // next election timeout.
            self.role = Role::Follower;
            self.pre_votes = None;
        }
        Ok(())
    }

    /// Handles a VoteResponse.
    ///
    /// On quorum, appends and persists the leader no-op entry. If the round
    /// cannot win, returns to Follower.
    fn on_vote_response(
        &mut self,
        from: crate::NodeId,
        term: Term,
        granted: bool,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        if term > self.hard_state.current_term() {
            return self.begin_term_transition(term, PersistContinuation::None, effects);
        }
        if term != self.hard_state.current_term() || self.role != Role::Candidate {
            return Ok(());
        }
        let Some(votes) = self.votes.as_mut() else {
            return Ok(());
        };
        votes.record(from, granted);
        if votes.has_quorum() {
            // Vote quorum achieved. Append the leader no-op to establish
            // a current-term entry before activating as leader.
            self.persist_leader_noop(effects)?;
        } else if votes.cannot_win() {
            self.role = Role::Follower;
            self.votes = None;
        }
        Ok(())
    }

    // ── Timer-driven actions ──────────────────────────────────────────────

    /// Runs CheckQuorum and sends heartbeats on each heartbeat tick.
    ///
    /// Resets per-follower activity markers. If fewer than a quorum of
    /// followers have responded since the last tick, steps down to Follower
    /// (CheckQuorum). Sends empty AppendEntries to all followers.
    fn on_heartbeat_tick(&mut self, effects: &mut Vec<Effect<C>>) -> Result<(), StepError> {
        if self.role != Role::Leader || self.pending_persist.is_some() {
            return Ok(());
        }
        // CheckQuorum: step down if fewer than a quorum of members have
        // been active since the last heartbeat tick (Raft paper §6.2).
        if self.active_members.len() < self.config.quorum() {
            self.become_follower();
            return Ok(());
        }
        // Reset activity tracking for the new heartbeat round.
        self.active_members.clear();
        for progress in self.progress.values_mut() {
            progress.reset_activity();
        }
        self.active_members.insert(self.config.local_id());
        self.replicate_all(effects)
    }

    // ── Election state transitions ────────────────────────────────────────

    /// Initiates a pre-election (PreVote).
    ///
    /// PreVote uses the *prospective* next term (`current_term + 1`) so that
    /// a partitioned node does not increment its durable term and disrupt an
    /// active cluster. Only if a quorum of followers grants the pre-vote does
    /// the node advance to Candidate and increment the durable term.
    fn start_pre_vote(&mut self, effects: &mut Vec<Effect<C>>) -> Result<(), StepError> {
        if self.role == Role::Leader || self.pending_persist.is_some() {
            return Ok(());
        }
        // Prospective term = current + 1. Not yet persisted.
        let prospective = self
            .hard_state
            .current_term()
            .checked_next()
            .map_err(StepError::Arithmetic)?;
        self.role = Role::PreCandidate;
        self.votes = None;
        self.pre_votes = Some(QuorumTracker::with_local_vote(
            self.config.members(),
            self.config.local_id(),
        ));
        // Single-node cluster: the self-vote already forms a quorum.
        if self
            .pre_votes
            .as_ref()
            .is_some_and(QuorumTracker::has_quorum)
        {
            self.become_candidate(effects)?;
        } else {
            // Broadcast PreVote RPCs with the prospective term and our
            // last-log metadata for the completeness check.
            self.broadcast_pre_vote(prospective, effects);
        }
        Ok(())
    }

    /// Transitions to Candidate.
    ///
    /// Increments the durable term, votes for self, and persists before
    /// broadcasting RequestVote RPCs. This is the first step that touches
    /// durable state in the election path.
    fn become_candidate(&mut self, effects: &mut Vec<Effect<C>>) -> Result<(), StepError> {
        let term = self
            .hard_state
            .current_term()
            .checked_next()
            .map_err(StepError::Arithmetic)?;
        self.role = Role::Candidate;
        self.pre_votes = None;
        self.votes = Some(QuorumTracker::with_local_vote(
            self.config.members(),
            self.config.local_id(),
        ));
        self.hard_state = HardState::new(
            term,
            Some(self.config.local_id()),
            self.hard_state.commit_index(),
        );
        // Persist the new term and self-vote before asking others to vote.
        self.queue_persist(
            PersistBatch {
                hard_state: Some(self.hard_state.clone()),
                entries: Vec::new(),
                snapshot: None,
            },
            PersistContinuation::BroadcastVoteRequests,
            effects,
        )
    }

    /// Appends and persists a no-op entry after winning an election.
    ///
    /// The leader no-op (Raft paper §6.4) serves two purposes:
    /// 1. It establishes a current-term entry so that prior-term entries can
    ///    be committed (Raft's commit rule only commits current-term entries).
    /// 2. It proves to the new leader that it knows the commit index —
    ///    linearizable reads are deferred until this entry commits.
    fn persist_leader_noop(&mut self, effects: &mut Vec<Effect<C>>) -> Result<(), StepError> {
        let index = self
            .last_log_index
            .checked_next()
            .map_err(StepError::Arithmetic)?;
        let entry = Entry::leader_noop(index, self.hard_state.current_term())
            .expect("checked Raft index and nonzero elected term");
        self.log
            .append(vec![entry.clone()])
            .map_err(StepError::Log)?;
        self.last_log_index = index;
        self.last_log_term = self.hard_state.current_term();
        self.leader_noop_index = Some(index);
        // Persist the no-op before activating as leader. Until the no-op is
        // durable, the leader cannot serve reads or commit prior entries.
        self.queue_persist(
            PersistBatch {
                hard_state: None,
                entries: vec![entry],
                snapshot: None,
            },
            PersistContinuation::ActivateLeader,
            effects,
        )
    }

    /// Steps down to Follower on detecting a higher term.
    ///
    /// Clears `voted_for` (the higher term releases any prior vote promise),
    /// persists the new term, and optionally sends a response after durability.
    fn begin_term_transition(
        &mut self,
        term: Term,
        continuation: PersistContinuation,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        self.become_follower();
        self.hard_state = HardState::new(term, None, self.hard_state.commit_index());
        self.queue_persist(
            PersistBatch {
                hard_state: Some(self.hard_state.clone()),
                entries: Vec::new(),
                snapshot: None,
            },
            continuation,
            effects,
        )
    }

    /// Resets all leader and candidate volatile state.
    ///
    /// Called when stepping down from Leader or Candidate back to Follower.
    /// Clears vote trackers, active members, per-follower progress, the read
    /// round, pending reads, and the leader no-op index.
    fn become_follower(&mut self) {
        self.role = Role::Follower;
        self.pre_votes = None;
        self.votes = None;
        self.active_members.clear();
        self.progress.clear();
        self.leader_noop_index = None;
        self.pending_reads.clear();
        self.read_round = None;
    }

    // ── Effect emission helpers ───────────────────────────────────────────

    /// Creates a [`Effect::Persist`] for a batch and records the continuation.
    ///
    /// At most one persist can be outstanding — the `pending_persist` field
    /// acts as a lock preventing concurrent durability operations.
    fn queue_persist(
        &mut self,
        batch: PersistBatch<C>,
        continuation: PersistContinuation,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        let id = self.next_effect_id()?;
        let stable_through = batch.entries.last().map(Entry::index);
        self.pending_persist = Some(PendingPersist {
            id,
            stable_through,
            continuation,
        });
        effects.push(Effect::Persist { id, batch });
        Ok(())
    }

    /// Issues an [`Effect::Apply`] for the range `(applied_index, commit_index]`.
    ///
    /// Only one apply can be outstanding at a time. No-op if a persist or
    /// apply barrier is already in flight, or if there is nothing new to apply.
    fn emit_apply_if_needed(&mut self, effects: &mut Vec<Effect<C>>) -> Result<(), StepError> {
        if self.pending_persist.is_some()
            || self.pending_apply.is_some()
            || self.applied_index >= self.hard_state.commit_index()
        {
            return Ok(());
        }
        let from = self
            .applied_index
            .checked_next()
            .map_err(StepError::Arithmetic)?;
        let through = self.hard_state.commit_index();
        let entries = self
            .log
            .entries(from..=through)
            .map_err(StepError::Log)?
            .to_vec();
        let id = self.next_effect_id()?;
        self.pending_apply = Some(PendingApply { id, through });
        effects.push(Effect::Apply { id, entries });
        Ok(())
    }

    /// Reports [`ProposalResult::Applied`] for every proposal whose log index
    /// is at or below the applied-through point.
    fn finish_apply(&mut self, through: LogIndex, effects: &mut Vec<Effect<C>>) {
        let indexes: Vec<_> = self
            .proposals
            .range(..=through)
            .map(|(index, _)| *index)
            .collect();
        for index in indexes {
            if let Some(ids) = self.proposals.remove(&index) {
                for proposal_id in ids {
                    effects.push(Effect::ProposalResult {
                        proposal_id,
                        result: ProposalResult::Applied { index },
                    });
                }
            }
        }
    }

    /// Reports [`ProposalResult::LeadershipLost`] for every proposal above
    /// the committed index when the leader steps down.
    fn fail_uncommitted_proposals(&mut self, effects: &mut Vec<Effect<C>>) {
        let committed = self.hard_state.commit_index();
        // Only proposals *above* the committed index are lost. Proposals at
        // or below the committed index will eventually be applied (they are
        // safe even if leadership changes).
        let indexes: Vec<_> = self
            .proposals
            .range((
                core::ops::Bound::Excluded(committed),
                core::ops::Bound::Unbounded,
            ))
            .map(|(index, _)| *index)
            .collect();
        for index in indexes {
            if let Some(ids) = self.proposals.remove(&index) {
                for proposal_id in ids {
                    effects.push(Effect::ProposalResult {
                        proposal_id,
                        result: ProposalResult::LeadershipLost,
                    });
                }
            }
        }
    }

    // ── Incoming snapshot completion ──────────────────────────────────────

    /// Handles completion events for the inbound snapshot pipeline.
    ///
    /// The pipeline has two phases after the last chunk is stored:
    /// 1. **Persist**: the snapshot record and updated hard state are persisted.
    /// 2. **Install**: the durable snapshot is installed into the state machine.
    ///
    /// Returns `Ok(true)` if the outcome was consumed, `Ok(false)` if it
    /// didn't match any snapshot-related pending state (caller should try
    /// other handlers).
    fn on_incoming_snapshot_completion(
        &mut self,
        id: EffectId,
        outcome: &EffectOutcome,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<bool, StepError> {
        // ── Chunk store completion ──
        if let Some(pending) = self.pending_chunk_store.take() {
            if pending.id != id {
                self.pending_chunk_store = Some(pending);
            } else {
                match outcome {
                    EffectOutcome::SnapshotChunkStored {
                        snapshot_id,
                        next_offset,
                        snapshot_ref,
                    } => {
                        let receiver = self
                            .incoming_snapshot
                            .as_ref()
                            .expect("chunk store has a receiver");
                        // Validate the host stored exactly what we requested.
                        if *snapshot_id != receiver.metadata().id()
                            || *next_offset != receiver.next_offset()
                        {
                            self.pending_chunk_store = Some(pending);
                            return Err(StepError::Input(InputError::InvalidEffectOutcome));
                        }
                        if pending.done {
                            // Final chunk stored. Validate the complete
                            // transfer (SHA-256 digest check), then persist
                            // the snapshot record.
                            if !receiver.is_complete_and_valid() {
                                self.pending_chunk_store = Some(pending);
                                return Err(StepError::InvalidSnapshot);
                            }
                            let Some(snapshot_ref) = snapshot_ref.clone() else {
                                self.pending_chunk_store = Some(pending);
                                return Err(StepError::InvalidSnapshot);
                            };
                            let record = receiver.record(snapshot_ref);
                            let persist_id = self.next_effect_id()?;
                            self.incoming_snapshot_state =
                                Some(IncomingSnapshotState::Persisting {
                                    id: persist_id,
                                    record: record.clone(),
                                    from: pending.from,
                                    term: pending.term,
                                });
                            // Persist the snapshot record atomically with
                            // updated hard state (commit index advances to
                            // at least the snapshot boundary).
                            effects.push(Effect::Persist {
                                id: persist_id,
                                batch: PersistBatch {
                                    hard_state: Some(HardState::new(
                                        pending.term,
                                        None,
                                        self.hard_state
                                            .commit_index()
                                            .max(record.metadata().index()),
                                    )),
                                    entries: Vec::new(),
                                    snapshot: Some(record),
                                },
                            });
                        }
                        return Ok(true);
                    }
                    EffectOutcome::Failed => {
                        self.stopped = true;
                        return Err(StepError::PersistenceFailed);
                    }
                    _ => {
                        self.pending_chunk_store = Some(pending);
                        return Err(StepError::Input(InputError::InvalidEffectOutcome));
                    }
                }
            }
        }

        // ── Persist / Install completion ──
        let Some(state) = self.incoming_snapshot_state.take() else {
            return Ok(false);
        };
        match state {
            IncomingSnapshotState::Persisting {
                id: expected,
                record,
                from,
                term,
            } if expected == id => match outcome {
                EffectOutcome::Persisted => {
                    // Snapshot record is durable. Now install it into the
                    // state machine.
                    let install_id = self.next_effect_id()?;
                    self.incoming_snapshot_state = Some(IncomingSnapshotState::Installing {
                        id: install_id,
                        record: record.clone(),
                        from,
                        term,
                    });
                    effects.push(Effect::InstallSnapshot {
                        id: install_id,
                        record,
                    });
                    Ok(true)
                }
                EffectOutcome::Failed => {
                    self.stopped = true;
                    Err(StepError::PersistenceFailed)
                }
                _ => {
                    self.incoming_snapshot_state = Some(IncomingSnapshotState::Persisting {
                        id: expected,
                        record,
                        from,
                        term,
                    });
                    Err(StepError::Input(InputError::InvalidEffectOutcome))
                }
            },
            IncomingSnapshotState::Installing {
                id: expected,
                record,
                from,
                term,
            } if expected == id => match outcome {
                EffectOutcome::SnapshotInstalled { snapshot_id }
                    if *snapshot_id == record.metadata().id() =>
                {
                    // Snapshot installed. Update the logical log, applied
                    // index, snapshot boundary, and hard state. Then respond
                    // to the leader so it can resume replication.
                    self.log
                        .install_snapshot(record.clone())
                        .map_err(StepError::Log)?;
                    self.snapshot_index = record.metadata().index();
                    self.applied_index = self.applied_index.max(self.snapshot_index);
                    self.hard_state = HardState::new(
                        self.hard_state.current_term().max(term),
                        None,
                        self.hard_state.commit_index().max(self.snapshot_index),
                    );
                    self.last_log_index = self.log.last_index();
                    self.last_log_term =
                        self.log.term(self.last_log_index).map_err(StepError::Log)?;
                    self.incoming_snapshot = None;
                    effects.push(Effect::SendMessage {
                        to: from,
                        message: Message::InstallSnapshotResponse {
                            term: self.hard_state.current_term(),
                            success: true,
                        },
                    });
                    Ok(true)
                }
                EffectOutcome::Failed => {
                    self.stopped = true;
                    Err(StepError::ApplyFailed)
                }
                _ => {
                    self.incoming_snapshot_state = Some(IncomingSnapshotState::Installing {
                        id: expected,
                        record,
                        from,
                        term,
                    });
                    Err(StepError::Input(InputError::InvalidEffectOutcome))
                }
            },
            state => {
                self.incoming_snapshot_state = Some(state);
                Ok(false)
            }
        }
    }

    // ── Local snapshot completion ─────────────────────────────────────────

    /// Handles completion events for the local snapshot pipeline.
    ///
    /// Three phases: Build → Persist → Compact. Each phase validates its
    /// outcome before advancing.
    fn on_snapshot_completion(
        &mut self,
        id: EffectId,
        outcome: &EffectOutcome,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<bool, StepError> {
        let state = core::mem::replace(&mut self.local_snapshot, LocalSnapshotState::Idle);
        match state {
            LocalSnapshotState::Idle => Ok(false),
            // ── Build phase ──
            LocalSnapshotState::Building {
                id: expected,
                through,
            } if expected == id => match outcome {
                EffectOutcome::SnapshotBuilt {
                    metadata,
                    snapshot_ref,
                } => {
                    let record = match self.validate_local_snapshot(
                        metadata.clone(),
                        snapshot_ref.clone(),
                        through,
                    ) {
                        Ok(record) => record,
                        Err(error) => {
                            self.local_snapshot = LocalSnapshotState::Building {
                                id: expected,
                                through,
                            };
                            return Err(error);
                        }
                    };
                    // Host built the snapshot. Persist the record before
                    // compacting the log.
                    let persist_id = self.next_effect_id()?;
                    self.local_snapshot = LocalSnapshotState::Persisting {
                        id: persist_id,
                        record: record.clone(),
                    };
                    effects.push(Effect::Persist {
                        id: persist_id,
                        batch: PersistBatch {
                            hard_state: None,
                            entries: Vec::new(),
                            snapshot: Some(record),
                        },
                    });
                    Ok(true)
                }
                EffectOutcome::Failed => {
                    self.stopped = true;
                    Err(StepError::SnapshotFailed)
                }
                _ => {
                    self.local_snapshot = LocalSnapshotState::Building {
                        id: expected,
                        through,
                    };
                    Err(StepError::Input(InputError::InvalidEffectOutcome))
                }
            },
            // ── Persist phase ──
            LocalSnapshotState::Persisting {
                id: expected,
                record,
            } if expected == id => match outcome {
                EffectOutcome::Persisted => {
                    // Snapshot record is durable. Now compact the log prefix
                    // through the snapshot index.
                    let compact_id = self.next_effect_id()?;
                    let through = record.metadata().index();
                    self.local_snapshot = LocalSnapshotState::Compacting {
                        id: compact_id,
                        record,
                    };
                    effects.push(Effect::CompactLog {
                        id: compact_id,
                        through,
                    });
                    Ok(true)
                }
                EffectOutcome::Failed => {
                    self.stopped = true;
                    Err(StepError::PersistenceFailed)
                }
                _ => {
                    self.local_snapshot = LocalSnapshotState::Persisting {
                        id: expected,
                        record,
                    };
                    Err(StepError::Input(InputError::InvalidEffectOutcome))
                }
            },
            // ── Compact phase ──
            LocalSnapshotState::Compacting {
                id: expected,
                record,
            } if expected == id => match outcome {
                EffectOutcome::Compacted { through } if *through == record.metadata().index() => {
                    // Log prefix compacted. Update the in-memory log and
                    // snapshot boundary.
                    self.log.compact(record).map_err(StepError::Log)?;
                    self.snapshot_index = *through;
                    Ok(true)
                }
                EffectOutcome::Failed => {
                    self.stopped = true;
                    Err(StepError::CompactionFailed)
                }
                _ => {
                    self.local_snapshot = LocalSnapshotState::Compacting {
                        id: expected,
                        record,
                    };
                    Err(StepError::Input(InputError::InvalidEffectOutcome))
                }
            },
            state => {
                self.local_snapshot = state;
                Ok(false)
            }
        }
    }

    /// Master completion dispatcher.
    ///
    /// Routes a completed effect to the correct handler in priority order:
    /// 1. Outgoing snapshot chunk reads
    /// 2. Incoming snapshot (chunk store → persist → install)
    /// 3. Local snapshot (build → persist → compact)
    /// 4. Apply (committed entries → state machine)
    /// 5. Persist (durability barrier → continuation)
    ///
    /// Each handler returns `Ok(true)` if it consumed the outcome, `Ok(false)`
    /// if it didn't match. The first match wins.
    fn on_effect_completed(
        &mut self,
        id: EffectId,
        outcome: EffectOutcome,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        if id.generation() != self.config.generation() {
            return Err(StepError::Input(InputError::StaleEffectGeneration));
        }
        // Try each handler in priority order. The ordering prevents one
        // handler from accidentally consuming another handler's outcome.
        if self.on_outgoing_snapshot_completion(id, &outcome, effects)? {
            return Ok(());
        }
        if self.on_incoming_snapshot_completion(id, &outcome, effects)? {
            return Ok(());
        }
        if self.on_snapshot_completion(id, &outcome, effects)? {
            return Ok(());
        }
        // ── Apply completion ──
        if let Some(pending) = self.pending_apply.take() {
            if pending.id != id {
                self.pending_apply = Some(pending);
                return Err(StepError::Input(InputError::UnknownEffect));
            }
            match outcome {
                EffectOutcome::Applied { through } if through == pending.through => {
                    self.applied_index = through;
                    self.finish_apply(through, effects);
                    // Applying entries may unblock pending reads and may
                    // trigger another apply batch.
                    self.release_read_round(effects);
                    self.start_pending_reads(effects)?;
                    self.emit_apply_if_needed(effects)?;
                    // Check whether enough entries have accumulated since
                    // the last snapshot to trigger a new one.
                    self.maybe_request_local_snapshot(effects)?;
                    return Ok(());
                }
                EffectOutcome::Failed => {
                    self.stopped = true;
                    return Err(StepError::ApplyFailed);
                }
                _ => {
                    self.pending_apply = Some(pending);
                    return Err(StepError::Input(InputError::InvalidEffectOutcome));
                }
            }
        }
        // ── Persist completion ──
        let Some(pending) = self.pending_persist.take() else {
            return Err(StepError::Input(InputError::UnknownEffect));
        };
        if pending.id != id {
            self.pending_persist = Some(pending);
            return Err(StepError::Input(InputError::UnknownEffect));
        }
        if matches!(outcome, EffectOutcome::Failed) {
            self.stopped = true;
            return Err(StepError::PersistenceFailed);
        }
        if !matches!(outcome, EffectOutcome::Persisted) {
            self.pending_persist = Some(pending);
            return Err(StepError::Input(InputError::InvalidEffectOutcome));
        }
        // Mark entries as durable so the log knows they are safe to compact.
        if let Some(index) = pending.stable_through {
            self.log.mark_stable(index).map_err(StepError::Log)?;
        }
        // Execute the continuation that was queued with the persist.
        // This is the key to the async effect model: the persist barrier
        // is lifted and the deferred protocol step runs now.
        match pending.continuation {
            PersistContinuation::BroadcastVoteRequests => {
                if self.votes.as_ref().is_some_and(QuorumTracker::has_quorum) {
                    // Single-node cluster: self-vote already forms quorum.
                    self.persist_leader_noop(effects)?;
                } else {
                    self.broadcast_vote_request(self.hard_state.current_term(), effects);
                }
            }
            PersistContinuation::SendVoteResponse { to, term, granted } => {
                self.send_vote_response(to, term, granted, effects)
            }
            PersistContinuation::SendAppendEntriesResponse { to, response } => {
                self.send_append_entries_response(to, response, effects);
                // Commit may have advanced — check whether new entries can
                // be applied.
                self.emit_apply_if_needed(effects)?;
            }
            PersistContinuation::ReplicateProposal => {
                // The proposal entry is durable. Replicate to followers
                // and check for immediate commit (single-node cluster).
                self.replicate_all(effects)?;
                self.advance_commit(effects)?;
                self.start_pending_reads(effects)?;
            }
            PersistContinuation::ApplyCommitted => {
                // Commit index is durable. Apply newly committed entries.
                self.emit_apply_if_needed(effects)?;
                self.start_pending_reads(effects)?;
            }
            PersistContinuation::ActivateLeader => {
                // No-op entry is durable. Transition to Leader and begin
                // replication.
                self.role = Role::Leader;
                self.votes = None;
                self.active_members.clear();
                self.active_members.insert(self.config.local_id());
                self.initialize_progress();
                self.replicate_all(effects)?;
                self.advance_commit(effects)?;
            }
            PersistContinuation::None => {}
        }
        Ok(())
    }

    // ── Envelope validation ───────────────────────────────────────────────

    /// Rejects messages for the wrong cluster, wrong recipient, or from an
    /// unknown sender. The host is responsible for authenticating the sender
    /// identity before constructing the envelope.
    fn validate_envelope(&self, envelope: &crate::Envelope<C>) -> Result<(), StepError> {
        if envelope.cluster_id() != self.config.cluster_id() {
            return Err(StepError::Input(InputError::WrongCluster));
        }
        if envelope.to() != self.config.local_id() {
            return Err(StepError::Input(InputError::WrongRecipient));
        }
        if !self.config.members().contains(&envelope.from()) {
            return Err(StepError::Input(InputError::UnknownSender));
        }
        Ok(())
    }

    // ── RPC broadcast helpers ─────────────────────────────────────────────

    /// Sends PreVote RPCs to all members except self.
    fn broadcast_pre_vote(&self, term: Term, effects: &mut Vec<Effect<C>>) {
        for node in self.config.members() {
            if *node != self.config.local_id() {
                effects.push(Effect::SendMessage {
                    to: *node,
                    message: Message::PreVote {
                        term,
                        last_log_index: self.last_log_index,
                        last_log_term: self.last_log_term,
                    },
                });
            }
        }
    }

    /// Sends RequestVote RPCs to all members except self.
    fn broadcast_vote_request(&self, term: Term, effects: &mut Vec<Effect<C>>) {
        for node in self.config.members() {
            if *node != self.config.local_id() {
                effects.push(Effect::SendMessage {
                    to: *node,
                    message: Message::RequestVote {
                        term,
                        last_log_index: self.last_log_index,
                        last_log_term: self.last_log_term,
                    },
                });
            }
        }
    }

    // ── Replication ───────────────────────────────────────────────────────

    /// Creates per-follower [`Progress`] entries in Probe state, each starting
    /// at `last_index + 1`.
    fn initialize_progress(&mut self) {
        let next = self
            .log
            .last_index()
            .checked_next()
            .expect("validated log index cannot overflow");
        self.progress.clear();
        for node in self.config.members() {
            if *node != self.config.local_id() {
                self.progress.insert(
                    *node,
                    Progress::new(next, self.config.max_inflight_appends()),
                );
            }
        }
    }

    /// Sends an AppendEntries RPC to every follower that can accept more work.
    fn replicate_all(&mut self, effects: &mut Vec<Effect<C>>) -> Result<(), StepError> {
        let peers: Vec<_> = self.progress.keys().copied().collect();
        for peer in peers {
            self.replicate_to(peer, effects)?;
        }
        Ok(())
    }

    /// Sends one AppendEntries batch to a specific follower.
    ///
    /// If the follower's `next_index` is before the first available log entry
    /// (already compacted into a snapshot), falls back to sending a snapshot
    /// chunk instead. Otherwise, builds a batch bounded by per-RPC entry and
    /// byte limits.
    fn replicate_to(
        &mut self,
        to: crate::NodeId,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        let Some(progress) = self.progress.get(&to) else {
            return Ok(());
        };
        if !progress.can_send() {
            return Ok(());
        }
        let next = progress.next_index();
        // If the follower needs entries that have been compacted into a
        // snapshot, fall back to snapshot transfer.
        if next < self.log.first_index() {
            self.progress
                .get_mut(&to)
                .expect("progress entry was checked")
                .enter_snapshot();
            return self.read_snapshot_chunk(to, effects);
        }
        // Build a batch starting at `next`, respecting the per-RPC entry
        // count and byte limits.
        let prev_log_index = LogIndex::new(next.get() - 1);
        let prev_log_term = self.log.term(prev_log_index).map_err(StepError::Log)?;
        let entries = self.replication_batch(next)?;
        let end_index = entries.last().map_or(prev_log_index, Entry::index);
        let progress = self
            .progress
            .get_mut(&to)
            .expect("progress entry was checked");
        progress.sent(end_index);
        effects.push(Effect::SendMessage {
            to,
            message: Message::AppendEntries {
                term: self.hard_state.current_term(),
                prev_log_index,
                prev_log_term,
                leader_commit: self.hard_state.commit_index(),
                read_context: None,
                entries,
            },
        });
        Ok(())
    }

    /// Reads one snapshot chunk for transmission to a lagging follower.
    ///
    /// At most one outstanding chunk read per follower. Creates a
    /// [`SnapshotSender`] cursor on first call; subsequent calls advance
    /// through the snapshot body.
    fn read_snapshot_chunk(
        &mut self,
        to: crate::NodeId,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        if self.pending_snapshot_reads.values().any(|peer| *peer == to) {
            return Ok(());
        }
        if let std::collections::btree_map::Entry::Vacant(entry) = self.snapshot_senders.entry(to) {
            let snapshot = self
                .log
                .snapshot()
                .cloned()
                .ok_or(StepError::InvalidSnapshot)?;
            entry.insert(SnapshotSender::new(snapshot));
        }
        let (snapshot, offset) = {
            let sender = self
                .snapshot_senders
                .get(&to)
                .expect("sender was initialized");
            (sender.snapshot().clone(), sender.offset())
        };
        let id = self.next_effect_id()?;
        self.pending_snapshot_reads.insert(id, to);
        effects.push(Effect::ReadSnapshotChunk {
            id,
            snapshot,
            offset,
            max_len: self.config.snapshot_chunk_bytes(),
        });
        Ok(())
    }

    /// Handles completion of a [`ReadSnapshotChunk`](Effect::ReadSnapshotChunk)
    /// effect.
    ///
    /// Sends the chunk to the follower as an InstallSnapshot message. If the
    /// chunk was not the last, reads the next chunk. Validates that the host
    /// returned data at the expected offset and for the expected snapshot.
    fn on_outgoing_snapshot_completion(
        &mut self,
        id: EffectId,
        outcome: &EffectOutcome,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<bool, StepError> {
        let Some(to) = self.pending_snapshot_reads.remove(&id) else {
            return Ok(false);
        };
        let sender = self
            .snapshot_senders
            .get_mut(&to)
            .expect("read has a sender");
        match outcome {
            EffectOutcome::SnapshotChunkRead {
                snapshot_id,
                offset,
                bytes,
                done,
            } if *snapshot_id == sender.snapshot().metadata().id()
                && *offset == sender.offset() =>
            {
                if bytes.len() > self.config.snapshot_chunk_bytes() {
                    self.pending_snapshot_reads.insert(id, to);
                    return Err(StepError::Input(InputError::InvalidEffectOutcome));
                }
                let metadata = sender.snapshot().metadata().clone();
                if sender.advance(bytes.len()).is_err() {
                    self.pending_snapshot_reads.insert(id, to);
                    return Err(StepError::Input(InputError::InvalidEffectOutcome));
                }
                effects.push(Effect::SendMessage {
                    to,
                    message: Message::InstallSnapshot {
                        term: self.hard_state.current_term(),
                        metadata,
                        offset: *offset,
                        bytes: bytes.clone(),
                        done: *done,
                    },
                });
                // If not done, queue the next chunk read.
                if !done {
                    self.read_snapshot_chunk(to, effects)?;
                }
                Ok(true)
            }
            EffectOutcome::Failed => {
                self.stopped = true;
                Err(StepError::PersistenceFailed)
            }
            _ => {
                self.pending_snapshot_reads.insert(id, to);
                Err(StepError::Input(InputError::InvalidEffectOutcome))
            }
        }
    }

    /// Builds an AppendEntries batch starting at `next`, respecting the
    /// per-RPC entry count and byte limits.
    ///
    /// Ensures at least one entry is included when entries exist, even if it
    /// alone exceeds the byte limit (to prevent starvation).
    fn replication_batch(&self, next: LogIndex) -> Result<Vec<Entry<C>>, StepError> {
        if next > self.log.last_index() {
            return Ok(Vec::new());
        }
        let entries = self
            .log
            .entries(next..=self.log.last_index())
            .map_err(StepError::Log)?;
        let mut batch = Vec::new();
        let mut bytes = 0usize;
        for entry in entries {
            let next_bytes = bytes.saturating_add(entry.encoded_len());
            // Always include at least one entry. After that, stop when
            // either the entry-count or byte limit is reached.
            if !batch.is_empty()
                && (batch.len() == self.config.max_entries_per_rpc()
                    || next_bytes > self.config.max_bytes_per_rpc())
            {
                break;
            }
            batch.push(entry.clone());
            bytes = next_bytes;
            if batch.len() == self.config.max_entries_per_rpc() {
                break;
            }
        }
        Ok(batch)
    }

    // ── Read-index heartbeat ──────────────────────────────────────────────

    /// Sends empty AppendEntries RPCs carrying the current read round's
    /// context to all followers.
    fn send_read_heartbeats(&self, effects: &mut Vec<Effect<C>>) -> Result<(), StepError> {
        let context = self
            .read_round
            .as_ref()
            .expect("read round exists")
            .context()
            .to_vec();
        let prev_log_index = self.log.last_index();
        let prev_log_term = self.log.term(prev_log_index).map_err(StepError::Log)?;
        for node in self.config.members() {
            if *node != self.config.local_id() {
                effects.push(Effect::SendMessage {
                    to: *node,
                    message: Message::AppendEntries {
                        term: self.hard_state.current_term(),
                        prev_log_index,
                        prev_log_term,
                        leader_commit: self.hard_state.commit_index(),
                        read_context: Some(context.clone()),
                        entries: Vec::new(),
                    },
                });
            }
        }
        Ok(())
    }

    // ── Response helpers ──────────────────────────────────────────────────

    fn send_pre_vote_response(
        &self,
        to: crate::NodeId,
        granted: bool,
        effects: &mut Vec<Effect<C>>,
    ) {
        effects.push(Effect::SendMessage {
            to,
            message: Message::PreVoteResponse {
                term: self.hard_state.current_term(),
                granted,
            },
        });
    }

    fn send_append_entries_response(
        &self,
        to: crate::NodeId,
        response: AppendResponse,
        effects: &mut Vec<Effect<C>>,
    ) {
        effects.push(Effect::SendMessage {
            to,
            message: Message::AppendEntriesResponse {
                term: response.term,
                success: response.success,
                conflict: response.conflict,
                match_index: response.match_index,
                read_context: response.read_context,
            },
        });
    }

    fn send_vote_response(
        &self,
        to: crate::NodeId,
        term: Term,
        granted: bool,
        effects: &mut Vec<Effect<C>>,
    ) {
        effects.push(Effect::SendMessage {
            to,
            message: Message::VoteResponse { term, granted },
        });
    }

    /// Produces the next unique [`EffectId`] within the current recovery
    /// generation.
    fn next_effect_id(&mut self) -> Result<EffectId, StepError> {
        self.next_effect_sequence = self
            .next_effect_sequence
            .checked_add(1)
            .ok_or(StepError::Arithmetic(crate::ArithmeticError::Overflow))?;
        Ok(EffectId::new(
            self.config.generation(),
            self.next_effect_sequence,
        ))
    }
}
