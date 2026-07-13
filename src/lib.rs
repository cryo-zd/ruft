//! A deterministic, runtime-agnostic Raft consensus core.
//!
//! Ruft separates protocol state transitions from networking, storage, timers,
//! and application state. This first module set defines the stable identifiers
//! and validated configuration used by the rest of the implementation.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod config;
mod error;
mod log;
mod recovery_error;
mod state;
mod types;

pub use config::{Config, ConfigBuilder};
pub use error::{ArithmeticError, InitError};
pub use log::{Entry, EntryPayload, RaftLog};
pub use recovery_error::{EntryError, LogError, RecoveryError};
pub use state::{HardState, RecoveredState, SnapshotRecord};
pub use types::{
    ClusterId, EffectId, LogIndex, NodeId, ProposalId, ReadId, SnapshotDigest, SnapshotId,
    SnapshotMetadata, SnapshotRef, Term,
};
