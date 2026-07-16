//! Deterministic election state machine and effect-completion boundary.

#![allow(missing_docs)]

use std::collections::{BTreeMap, BTreeSet};

use crate::progress::{Progress, QuorumTracker};
use crate::{
    Config, ConflictHint, Effect, EffectId, EffectOutcome, Entry, Event, HardState, LogError,
    LogIndex, Message, PersistBatch, ProposalId, ProposalResult, RaftLog, ReadId, RecoveredState,
    Term,
    raft::{
        PrefixDecision, ReadRound, is_log_up_to_date, quorum_commit, rejected_next, validate_prefix,
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
}

/// A rejected host input that does not change core state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InputError {
    WrongRecipient,
    WrongCluster,
    UnknownSender,
    StaleEffectGeneration,
    UnknownEffect,
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
    PersistenceFailed,
    Stopped,
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
    next_effect_sequence: u64,
    stopped: bool,
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
            next_effect_sequence: 0,
            stopped: false,
            _command: core::marker::PhantomData,
        })
    }

    /// Applies one event and returns host work required by the transition.
    pub fn step(&mut self, event: Event<C>) -> Result<StepOutput<C>, StepError> {
        if self.stopped && !matches!(event, Event::Shutdown) {
            return Err(StepError::Stopped);
        }
        let previous_role = self.role;
        let mut effects = Vec::new();
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
                self.on_effect_completed(id, outcome, &mut effects)?
            }
            Event::Shutdown => self.stopped = true,
            Event::Propose {
                proposal_id,
                command,
                encoded_len,
            } => self.on_propose(proposal_id, command, encoded_len, &mut effects)?,
            Event::Read { read_id, .. } => self.on_read(read_id, &mut effects)?,
        }
        self.emit_apply_if_needed(&mut effects)?;
        if previous_role == Role::Leader && self.role != Role::Leader {
            self.fail_uncommitted_proposals(&mut effects);
        }
        Ok(StepOutput {
            effects,
            soft_state_changed: previous_role != self.role,
        })
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
        }
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
            Message::InstallSnapshot { term, .. } => {
                if *term > self.hard_state.current_term() {
                    self.begin_term_transition(*term, PersistContinuation::None, effects)?;
                }
            }
            Message::AppendEntriesResponse { .. } => {
                self.on_append_entries_response(from, message, effects)?
            }
            Message::Heartbeat => {}
        }
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

    fn on_effect_completed(
        &mut self,
        id: EffectId,
        outcome: EffectOutcome,
        effects: &mut Vec<Effect<C>>,
    ) -> Result<(), StepError> {
        if id.generation() != self.config.generation() {
            return Err(StepError::Input(InputError::StaleEffectGeneration));
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
            return Ok(());
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
