//! Deterministic election state machine and effect-completion boundary.

#![allow(missing_docs)]

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
    Follower,
    PreCandidate,
    Candidate,
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
    pub const fn local_id(&self) -> crate::NodeId {
        self.local_id
    }
    pub const fn term(&self) -> Term {
        self.term
    }
    pub const fn role(&self) -> Role {
        self.role
    }
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
    WrongRecipient,
    WrongCluster,
    UnknownSender,
    StaleEffectGeneration,
    UnknownEffect,
    AlreadyCompleted,
    ConflictingEffectOutcome,
    InvalidEffectOutcome,
}

/// A failure while processing one serial Raft event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StepError {
    Input(InputError),
    Arithmetic(crate::ArithmeticError),
    Log(LogError),
    Entry(crate::EntryError),
    ApplyFailed,
    InvalidSnapshot,
    SnapshotFailed,
    CompactionFailed,
    PersistenceFailed,
    Fatal(FatalError),
    Stopped(StoppedReason),
}

/// Effects and volatile-state notification emitted for one input event.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StepOutput<C> {
    pub effects: Vec<Effect<C>>,
    pub soft_state_changed: bool,
}

struct AppendResponse {
    term: Term,
    success: bool,
    conflict: Option<ConflictHint>,
    match_index: LogIndex,
    read_context: Option<Vec<u8>>,
}

enum PersistContinuation {
    BroadcastVoteRequests,
    SendVoteResponse {
        to: crate::NodeId,
        term: Term,
        granted: bool,
    },
    ActivateLeader,
    SendAppendEntriesResponse {
        to: crate::NodeId,
        response: AppendResponse,
    },
    ReplicateProposal,
    ApplyCommitted,
    None,
}

struct PendingChunkStore {
    id: EffectId,
    done: bool,
    from: crate::NodeId,
    term: Term,
}

enum IncomingSnapshotState {
    Persisting {
        id: EffectId,
        record: SnapshotRecord,
        from: crate::NodeId,
        term: Term,
    },
    Installing {
        id: EffectId,
        record: SnapshotRecord,
        from: crate::NodeId,
        term: Term,
    },
}

struct PendingApply {
    id: EffectId,
    through: LogIndex,
}

struct PendingPersist {
    id: EffectId,
    stable_through: Option<LogIndex>,
    continuation: PersistContinuation,
}

/// A single-threaded Raft protocol core for a fixed member set.
pub struct RaftCore<C> {
    config: Config,
    hard_state: HardState,
    role: Role,
    log: RaftLog<C>,
    last_log_index: LogIndex,
    last_log_term: Term,
    pre_votes: Option<QuorumTracker>,
    votes: Option<QuorumTracker>,
    active_members: BTreeSet<crate::NodeId>,
    progress: BTreeMap<crate::NodeId, Progress>,
    pending_persist: Option<PendingPersist>,
    pending_apply: Option<PendingApply>,
    applied_index: LogIndex,
    proposals: BTreeMap<LogIndex, Vec<ProposalId>>,
    leader_noop_index: Option<LogIndex>,
    pending_reads: Vec<ReadId>,
    read_round: Option<ReadRound>,
    next_read_context: u64,
    local_snapshot: LocalSnapshotState,
    incoming_snapshot: Option<SnapshotReceiver>,
    pending_chunk_store: Option<PendingChunkStore>,
    incoming_snapshot_state: Option<IncomingSnapshotState>,
    snapshot_senders: BTreeMap<crate::NodeId, SnapshotSender>,
    pending_snapshot_reads: BTreeMap<EffectId, crate::NodeId>,
    snapshot_index: LogIndex,
    next_effect_sequence: u64,
    stopped: bool,
    stopped_reason: Option<StoppedReason>,
    completed_frontier: u64,
    completed_sparse: BTreeSet<u64>,
    completed_outcomes: BTreeMap<EffectId, EffectOutcome>,
    completion_order: std::collections::VecDeque<EffectId>,
    _command: core::marker::PhantomData<C>,
}

impl<C> RaftCore<C> {
    /// Restores a core from state that has already passed recovery validation.
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
    pub fn step(&mut self, event: Event<C>) -> Result<StepOutput<C>, StepError> {
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
            self.emit_apply_if_needed(&mut effects)?;
            Ok(())
        })();
        if let Err(error) = result {
            return Err(self.fatalize(error));
        }
        if previous_role == Role::Leader && self.role != Role::Leader {
            self.fail_uncommitted_proposals(&mut effects);
        }
        if let Err(violation) = self.validate_invariants() {
            return Err(self.fatal(FatalError::Invariant(violation)));
        }
        Ok(StepOutput {
            effects,
            soft_state_changed: previous_role != self.role,
        })
    }

    /// Returns the first log index still available as an entry.
    pub fn first_log_index(&self) -> LogIndex {
        self.log.first_index()
    }

    /// Returns the highest index applied to the local state machine.
    pub const fn applied_index(&self) -> LogIndex {
        self.applied_index
    }

    /// Returns leader-side progress for one remote member, when this node is leader.
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
    pub const fn is_stopped(&self) -> bool {
        self.stopped
    }

    fn validate_invariants(&self) -> Result<(), InvariantViolation> {
        invariant::validate(
            &self.hard_state,
            &self.log,
            self.applied_index,
            self.last_log_index,
            self.last_log_term,
        )
    }

    fn stop(&mut self, reason: StoppedReason) {
        if self.stopped_reason.is_none() {
            self.stopped_reason = Some(reason);
        }
        self.stopped = true;
    }

    fn fatal(&mut self, error: FatalError) -> StepError {
        self.stop(StoppedReason::Fatal(error.clone()));
        StepError::Fatal(error)
    }

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

    fn record_completion(&mut self, id: EffectId, outcome: EffectOutcome) {
        self.completed_outcomes.insert(id, outcome);
        self.completion_order.push_back(id);
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
        while self.completion_order.len() > self.config.completion_history() {
            let expired = self
                .completion_order
                .pop_front()
                .expect("history is nonempty");
            self.completed_outcomes.remove(&expired);
        }
    }

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

    fn on_read(&mut self, read_id: ReadId, effects: &mut Vec<Effect<C>>) -> Result<(), StepError> {
        if self.role != Role::Leader {
            return Ok(());
        }
        if self.pending_reads.len()
            + self
                .read_round
                .as_ref()
                .map_or(0, |round| round.request_count())
            >= self.config.max_pending_reads()
        {
            return Ok(());
        }
        if !self.current_leader_noop_is_committed() {
            self.pending_reads.push(read_id);
            return Ok(());
        }
        if let Some(round) = self.read_round.as_mut() {
            round.push(read_id);
            return Ok(());
        }
        self.start_read_round(read_id, effects)
    }

    fn current_leader_noop_is_committed(&self) -> bool {
        self.leader_noop_index
            .is_some_and(|index| index <= self.hard_state.commit_index())
    }

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
        if self.read_round.as_ref().is_some_and(ReadRound::has_quorum) {
            let round = self.read_round.as_mut().expect("round was just created");
            round.set_safe_index(self.hard_state.commit_index());
            self.release_read_round(effects);
            self.start_pending_reads(effects)?;
            return Ok(());
        }
        self.send_read_heartbeats(effects)
    }

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

    fn acknowledge_read_context(
        &mut self,
        from: crate::NodeId,
        context: &[u8],
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        let Some(round) = self.read_round.as_mut() else {
            return Ok(());
        };
        if round.context() != context {
            return Ok(());
        }
        round.acknowledge(from);
        if round.has_quorum() && round.safe_index().is_none() {
            round.set_safe_index(self.hard_state.commit_index());
        }
        self.release_read_round(effects);
        self.start_pending_reads(effects)
    }

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
            Message::Heartbeat => {}
        }
        Ok(())
    }

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
        if bytes.len() > self.config.snapshot_chunk_bytes() {
            return Err(StepError::InvalidSnapshot);
        }
        if metadata.members() != self.config.members() || metadata.index() <= self.snapshot_index {
            return Ok(());
        }
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

        self.become_follower();
        let term_changed = term > current_term;
        let decision =
            validate_prefix(&self.log, prev_log_index, prev_log_term).map_err(StepError::Log)?;
        let (success, conflict, appended) = match decision {
            PrefixDecision::Reject(conflict) => (false, Some(conflict), Vec::new()),
            PrefixDecision::Match => {
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
        if term > self.hard_state.current_term() {
            return self.begin_term_transition(term, PersistContinuation::None, effects);
        }
        if self.role != Role::Leader || term != self.hard_state.current_term() {
            return Ok(());
        }
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
        if let Some(context) = read_context {
            if success {
                self.acknowledge_read_context(from, context, effects)?;
            }
        }
        if changed {
            self.replicate_to(from, effects)?;
            self.advance_commit(effects)?;
        }
        Ok(())
    }

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

    fn on_vote_request(
        &mut self,
        from: crate::NodeId,
        term: Term,
        index: LogIndex,
        log_term: Term,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        let current = self.hard_state.current_term();
        if term < current {
            self.send_vote_response(from, current, false, effects);
            return Ok(());
        }
        let log_is_current =
            is_log_up_to_date(index, log_term, self.last_log_index, self.last_log_term);
        if term > current {
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
        let granted = log_is_current && self.hard_state.voted_for().is_none_or(|node| node == from);
        if granted && self.hard_state.voted_for().is_none() {
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
            self.become_candidate(effects)?;
        } else if votes.cannot_win() {
            self.role = Role::Follower;
            self.pre_votes = None;
        }
        Ok(())
    }

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
            self.persist_leader_noop(effects)?;
        } else if votes.cannot_win() {
            self.role = Role::Follower;
            self.votes = None;
        }
        Ok(())
    }

    fn on_heartbeat_tick(&mut self, effects: &mut Vec<Effect<C>>) -> Result<(), StepError> {
        if self.role != Role::Leader || self.pending_persist.is_some() {
            return Ok(());
        }
        if self.active_members.len() < self.config.quorum() {
            self.become_follower();
            return Ok(());
        }
        self.active_members.clear();
        for progress in self.progress.values_mut() {
            progress.reset_activity();
        }
        self.active_members.insert(self.config.local_id());
        self.replicate_all(effects)
    }

    fn start_pre_vote(&mut self, effects: &mut Vec<Effect<C>>) -> Result<(), StepError> {
        if self.role == Role::Leader || self.pending_persist.is_some() {
            return Ok(());
        }
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
        if self
            .pre_votes
            .as_ref()
            .is_some_and(QuorumTracker::has_quorum)
        {
            self.become_candidate(effects)?;
        } else {
            self.broadcast_pre_vote(prospective, effects);
        }
        Ok(())
    }

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

    fn fail_uncommitted_proposals(&mut self, effects: &mut Vec<Effect<C>>) {
        let committed = self.hard_state.commit_index();
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

    fn on_incoming_snapshot_completion(
        &mut self,
        id: EffectId,
        outcome: &EffectOutcome,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<bool, StepError> {
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
                        if *snapshot_id != receiver.metadata().id()
                            || *next_offset != receiver.next_offset()
                        {
                            self.pending_chunk_store = Some(pending);
                            return Err(StepError::Input(InputError::InvalidEffectOutcome));
                        }
                        if pending.done {
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

    fn on_snapshot_completion(
        &mut self,
        id: EffectId,
        outcome: &EffectOutcome,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<bool, StepError> {
        let state = core::mem::replace(&mut self.local_snapshot, LocalSnapshotState::Idle);
        match state {
            LocalSnapshotState::Idle => Ok(false),
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
            LocalSnapshotState::Persisting {
                id: expected,
                record,
            } if expected == id => match outcome {
                EffectOutcome::Persisted => {
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
            LocalSnapshotState::Compacting {
                id: expected,
                record,
            } if expected == id => match outcome {
                EffectOutcome::Compacted { through } if *through == record.metadata().index() => {
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

    fn on_effect_completed(
        &mut self,
        id: EffectId,
        outcome: EffectOutcome,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        if id.generation() != self.config.generation() {
            return Err(StepError::Input(InputError::StaleEffectGeneration));
        }
        if self.on_outgoing_snapshot_completion(id, &outcome, effects)? {
            return Ok(());
        }
        if self.on_incoming_snapshot_completion(id, &outcome, effects)? {
            return Ok(());
        }
        if self.on_snapshot_completion(id, &outcome, effects)? {
            return Ok(());
        }
        if let Some(pending) = self.pending_apply.take() {
            if pending.id != id {
                self.pending_apply = Some(pending);
                return Err(StepError::Input(InputError::UnknownEffect));
            }
            match outcome {
                EffectOutcome::Applied { through } if through == pending.through => {
                    self.applied_index = through;
                    self.finish_apply(through, effects);
                    self.release_read_round(effects);
                    self.start_pending_reads(effects)?;
                    self.emit_apply_if_needed(effects)?;
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
        if let Some(index) = pending.stable_through {
            self.log.mark_stable(index).map_err(StepError::Log)?;
        }
        match pending.continuation {
            PersistContinuation::BroadcastVoteRequests => {
                if self.votes.as_ref().is_some_and(QuorumTracker::has_quorum) {
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
                self.emit_apply_if_needed(effects)?;
            }
            PersistContinuation::ReplicateProposal => {
                self.replicate_all(effects)?;
                self.advance_commit(effects)?;
                self.start_pending_reads(effects)?;
            }
            PersistContinuation::ApplyCommitted => {
                self.emit_apply_if_needed(effects)?;
                self.start_pending_reads(effects)?;
            }
            PersistContinuation::ActivateLeader => {
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

    fn replicate_all(&mut self, effects: &mut Vec<Effect<C>>) -> Result<(), StepError> {
        let peers: Vec<_> = self.progress.keys().copied().collect();
        for peer in peers {
            self.replicate_to(peer, effects)?;
        }
        Ok(())
    }

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
        if next < self.log.first_index() {
            self.progress
                .get_mut(&to)
                .expect("progress entry was checked")
                .enter_snapshot();
            return self.read_snapshot_chunk(to, effects);
        }
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
