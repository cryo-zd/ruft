use std::sync::Arc;

use crate::{EntryError, LogIndex, Term};

/// The replicated payload stored at a single log index.
///
/// Commands are wrapped in [`Arc`] so the same entry can be shared across
/// the persistence batch, replication batch, and apply batch without cloning
/// the command value. The host decides the command type `C` — it need not
/// implement `Clone`, `Serialize`, or `Send`.
#[derive(Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EntryPayload<C> {
    /// An application command owned jointly by persistence, replication, and
    /// apply work through an [`Arc`].
    Command(Arc<C>),
    /// The no-op appended by a newly elected leader in its current term.
    ///
    /// Raft requires each leader to commit at least one entry from its own
    /// term before entries from prior terms can be considered committed
    /// (Raft paper §5.4.2). This no-op entry satisfies that requirement and
    /// also gates linearizable reads — they are deferred until the no-op
    /// commits, proving the leader knows the current commit index (§6.4).
    LeaderNoop,
}

impl<C> Clone for EntryPayload<C> {
    fn clone(&self) -> Self {
        match self {
            Self::Command(command) => Self::Command(Arc::clone(command)),
            Self::LeaderNoop => Self::LeaderNoop,
        }
    }
}

/// An immutable Raft log entry.
///
/// Each entry occupies exactly one log index and is created in exactly one
/// term. Once created, an entry is never mutated — conflict resolution
/// replaces entire suffixes rather than modifying individual entries.
#[derive(Debug, Eq, PartialEq)]
pub struct Entry<C> {
    index: LogIndex,
    term: Term,
    /// The host-supplied encoded size of the command, used for per-RPC byte
    /// budgeting during replication. Zero for leader no-ops.
    encoded_len: usize,
    payload: EntryPayload<C>,
}

impl<C> Clone for Entry<C> {
    fn clone(&self) -> Self {
        Self {
            index: self.index,
            term: self.term,
            encoded_len: self.encoded_len,
            payload: self.payload.clone(),
        }
    }
}

impl<C> Entry<C> {
    /// Creates a replicated application command with a host-supplied encoded
    /// size. The `encoded_len` must be nonzero — it is used to enforce
    /// per-RPC byte limits during replication.
    pub fn command(
        index: LogIndex,
        term: Term,
        command: C,
        encoded_len: usize,
    ) -> Result<Self, EntryError> {
        Self::validate_position(index, term)?;
        if encoded_len == 0 {
            return Err(EntryError::ZeroEncodedLength);
        }

        Ok(Self {
            index,
            term,
            encoded_len,
            payload: EntryPayload::Command(Arc::new(command)),
        })
    }

    /// Creates the no-op entry required after becoming leader.
    ///
    /// The no-op has `encoded_len` of 0 because it carries no command data.
    /// It does not count against per-RPC byte limits during replication.
    pub fn leader_noop(index: LogIndex, term: Term) -> Result<Self, EntryError> {
        Self::validate_position(index, term)?;

        Ok(Self {
            index,
            term,
            encoded_len: 0,
            payload: EntryPayload::LeaderNoop,
        })
    }

    /// Rejects entries at index 0 (virtual origin) or term 0 (uninitialized).
    fn validate_position(index: LogIndex, term: Term) -> Result<(), EntryError> {
        if index == LogIndex::new(0) {
            return Err(EntryError::ZeroLogIndex);
        }
        if term == Term::new(0) {
            return Err(EntryError::ZeroTerm);
        }
        Ok(())
    }

    /// Returns the one-based position of this entry.
    pub const fn index(&self) -> LogIndex {
        self.index
    }

    /// Returns the term that created this entry.
    pub const fn term(&self) -> Term {
        self.term
    }

    /// Returns the host-supplied encoded command size.
    pub const fn encoded_len(&self) -> usize {
        self.encoded_len
    }

    /// Returns the immutable payload.
    pub const fn payload(&self) -> &EntryPayload<C> {
        &self.payload
    }
}
