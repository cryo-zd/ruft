//! A deterministic, runtime-agnostic Raft consensus core.
//!
//! Ruft separates protocol state transitions from networking, storage, timers,
//! and application state. This first module set defines the stable identifiers
//! and validated configuration used by the rest of the implementation.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod config;
mod core;
mod effect;
mod error;
mod event;
mod log;
mod progress;
mod protocol;
mod raft;
mod recovery_error;
mod state;
mod types;

pub use config::{Config, ConfigBuilder};
pub use core::{InputError, RaftCore, Role, Status, StepError, StepOutput};
pub use effect::{Effect, EffectOutcome, PersistBatch, ProposalResult};
pub use error::{ArithmeticError, InitError};
pub use event::{Event, TickKind};
pub use log::{Entry, EntryPayload, RaftLog};
pub use progress::{Progress, ProgressState};
pub use protocol::{ConflictHint, Envelope, Message};
pub use recovery_error::{EntryError, LogError, RecoveryError};
pub use state::{HardState, RecoveredState, SnapshotRecord};
pub use types::{
    ClusterId, EffectId, LogIndex, NodeId, ProposalId, ReadId, SnapshotDigest, SnapshotId,
    SnapshotMetadata, SnapshotRef, Term,
};
