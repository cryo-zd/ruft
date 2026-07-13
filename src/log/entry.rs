use std::sync::Arc;

use crate::{EntryError, LogIndex, Term};

/// The replicated payload stored at a single log index.
#[derive(Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum EntryPayload<C> {
    /// An application command owned jointly by persistence, replication, and apply work.
    Command(Arc<C>),
    /// The no-op appended by a newly elected leader in its current term.
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
#[derive(Debug, Eq, PartialEq)]
pub struct Entry<C> {
    index: LogIndex,
    term: Term,
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
    /// Creates a replicated application command with a host-supplied encoded size.
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
    pub fn leader_noop(index: LogIndex, term: Term) -> Result<Self, EntryError> {
        Self::validate_position(index, term)?;

        Ok(Self {
            index,
            term,
            encoded_len: 0,
            payload: EntryPayload::LeaderNoop,
        })
    }

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
