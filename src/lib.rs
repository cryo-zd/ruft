//! A deterministic, runtime-agnostic Raft consensus core.
//!
//! Ruft separates protocol state transitions from networking, storage, timers,
//! and application state. The host drives [`RaftCore`] with [`Event`] values and
//! executes each returned [`Effect`] before reporting its completion.
//!
//! Correctness-sensitive effects have explicit barriers: persistence must be
//! atomic and durable before its completion, committed entries must be applied
//! in order, and received snapshots must be installed before they are acknowledged.
//! Storage or state-machine failure stops the running core; recover by validating
//! durable state and constructing a new core with a new effect generation.
//!
//! Membership is fixed for the lifetime of a core. See the repository README and
//! `examples/minimal_host.rs` for a complete in-memory host loop.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod config;
mod core;
mod effect;
mod error;
mod event;
mod invariant;
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
pub use error::{ArithmeticError, FatalError, InitError, InvariantViolation, StoppedReason};
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
