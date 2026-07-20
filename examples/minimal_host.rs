//! A complete single-node host loop using in-memory, non-production adapters.

use std::{collections::VecDeque, error::Error};

use ruft::{
    ClusterId, Config, Effect, EffectOutcome, EntryPayload, Event, HardState, LogIndex, NodeId,
    ProposalId, RaftCore, RecoveredState, Term, TickKind,
};

/// Executes only the effects exercised by this example. A production host must
/// durably store every `PersistBatch`, route messages, and implement snapshot effects.
#[derive(Default)]
struct InMemoryHost {
    applied: Vec<String>,
}

impl InMemoryHost {
    fn execute(&mut self, effect: Effect<String>) -> Result<Option<Event<String>>, Box<dyn Error>> {
        match effect {
            Effect::Persist { id, .. } => Ok(Some(Event::EffectCompleted {
                id,
                outcome: EffectOutcome::Persisted,
            })),
            Effect::Apply { id, entries } => {
                let through = entries
                    .last()
                    .map_or(LogIndex::new(0), |entry| entry.index());
                for entry in entries {
                    if let EntryPayload::Command(command) = entry.payload() {
                        self.applied.push(command.to_string());
                    }
                }
                Ok(Some(Event::EffectCompleted {
                    id,
                    outcome: EffectOutcome::Applied { through },
                }))
            }
            Effect::SendMessage { .. } => Ok(None),
            Effect::ProposalResult {
                proposal_id,
                result,
            } => {
                println!("proposal {proposal_id:?}: {result:?}");
                Ok(None)
            }
            Effect::ReadReady {
                read_id,
                read_index,
            } => {
                println!("read {read_id:?} is safe through {read_index:?}");
                Ok(None)
            }
            unsupported => Err(format!("example host does not implement {unsupported:?}").into()),
        }
    }
}

fn drive(
    core: &mut RaftCore<String>,
    event: Event<String>,
    host: &mut InMemoryHost,
) -> Result<(), Box<dyn Error>> {
    let mut pending = VecDeque::from([event]);
    while let Some(event) = pending.pop_front() {
        for effect in core
            .step(event)
            .map_err(|error| std::io::Error::other(format!("Raft step failed: {error:?}")))?
            .effects
        {
            if let Some(completion) = host.execute(effect)? {
                pending.push_back(completion);
            }
        }
    }
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    let local = NodeId::new(1);
    let config = Config::builder(local, [local])
        .cluster_id(ClusterId::new(1))
        .heartbeat_ticks(1)
        .election_ticks(3..=6)
        .check_quorum_ticks(3)
        .build()?;
    let recovered = RecoveredState::new(
        HardState::new(Term::new(0), None, LogIndex::new(0)),
        None,
        Vec::new(),
    )?;
    let mut core = RaftCore::new(config, recovered)?;
    let mut host = InMemoryHost::default();

    drive(&mut core, Event::Tick(TickKind::Election), &mut host)?;
    drive(
        &mut core,
        Event::Propose {
            proposal_id: ProposalId::new(1),
            command: "set answer=42".to_owned(),
            encoded_len: "set answer=42".len(),
        },
        &mut host,
    )?;

    assert_eq!(host.applied, ["set answer=42"]);
    Ok(())
}
