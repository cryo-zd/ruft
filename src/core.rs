//! Deterministic event dispatcher and effect-completion boundary.

#![allow(missing_docs)]

use crate::{Config, Effect, Event, RecoveredState};

/// A read-only summary of the core's externally relevant state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Status {
    local_id: crate::NodeId,
    stopped: bool,
}

impl Status {
    /// Returns the node hosted by this core.
    pub const fn local_id(&self) -> crate::NodeId {
        self.local_id
    }
    /// Reports whether the host requested shutdown.
    pub const fn is_stopped(&self) -> bool {
        self.stopped
    }
}

/// Input validation failures that leave protocol state unchanged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum InputError {
    WrongRecipient,
    WrongCluster,
    UnknownSender,
    StaleEffectGeneration,
    UnknownEffect,
}

/// Failure returned by one `step` call.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StepError {
    Input(InputError),
    Stopped,
}

/// Effects produced by a successful state transition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StepOutput<C> {
    pub effects: Vec<Effect<C>>,
    pub soft_state_changed: bool,
}

/// Runtime-neutral core state. Protocol transitions are added by subsequent tasks.
pub struct RaftCore<C> {
    config: Config,
    _recovered: RecoveredState<C>,
    stopped: bool,
}

impl<C> RaftCore<C> {
    /// Creates a follower core from validated configuration and recovered state.
    pub fn new(config: Config, recovered: RecoveredState<C>) -> Result<Self, crate::InitError> {
        Ok(Self {
            config,
            _recovered: recovered,
            stopped: false,
        })
    }
    /// Handles one serial event without performing I/O.
    pub fn step(&mut self, event: Event<C>) -> Result<StepOutput<C>, StepError> {
        match event {
            Event::MessageReceived(envelope) => {
                if envelope.cluster_id() != self.config.cluster_id() {
                    return Err(StepError::Input(InputError::WrongCluster));
                }
                if envelope.to() != self.config.local_id() {
                    return Err(StepError::Input(InputError::WrongRecipient));
                }
                if !self.config.members().contains(&envelope.from()) {
                    return Err(StepError::Input(InputError::UnknownSender));
                }
            }
            Event::EffectCompleted { id, .. } => {
                if id.generation() != self.config.generation() {
                    return Err(StepError::Input(InputError::StaleEffectGeneration));
                }
                return Err(StepError::Input(InputError::UnknownEffect));
            }
            Event::Shutdown => {
                self.stopped = true;
            }
            _ if self.stopped => return Err(StepError::Stopped),
            _ => {}
        }
        Ok(StepOutput {
            effects: Vec::new(),
            soft_state_changed: false,
        })
    }
    /// Returns a snapshot suitable for diagnostics.
    pub fn status(&self) -> Status {
        Status {
            local_id: self.config.local_id(),
            stopped: self.stopped,
        }
    }
}
