# Building a Runtime-Agnostic Raft Core in Rust

> This doc walks through the design philosophy, host interaction model, and protocol optimisations behind **Ruft**, a deterministic, runtime-agnostic implementation of the Raft consensus algorithm.

## 1. What Problem Does Ruft Solve?

Most Raft implementations bundle protocol logic with networking, storage, and
an async runtime. That makes them hard to test, hard to embed, and hard to
reason about. Ruft takes the opposite approach: it owns **only** the protocol
state machine and emits **effects** — pure data describing what the host must
do next. The host (your application) owns the runtime, the transport, the
storage engine, and the state machine.

```
┌──────────────────────────────────────────────────────────┐
│                      YOUR APPLICATION                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐  │
│  │  Network │  │  Storage │  │  Timers  │  │  State   │  │
│  │  (gRPC,  │  │  (Rocks, │  │  (tokio, │  │  Machine │  │
│  │   HTTP)  │  │   File)  │  │   smol)  │  │  (SQL,   │  │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  │  KV)     │  │
│       │             │             │        └────┬─────┘  │
│       │    ┌────────┴─────────────┴─────────────┘        │
│       │    │                                             │
│       ▼    ▼                                             │
│  ┌──────────────────────────────────────┐                │
│  │             RaftCore<C>              │                │
│  │     (pure protocol state machine)    │                │
│  └──────────────────────────────────────┘                │
└──────────────────────────────────────────────────────────┘
```

**Ruft has zero dependencies on**: an async runtime, a network stack, a codec,
a storage engine, or a wall clock. Commands (`C`) need not implement `Clone`,
`Serialize`, or `Send`. The host decides everything.

---

## 2. The Host Contract: Event → Effect Loop

The host drives the core through a single-threaded, serialised loop:

```
  ┌───────┐      ┌──────────────┐      ┌─────────┐
  │ EVENT │─────▶│ RaftCore::   │─────▶│ EFFECTS │
  │       │      │   step()     │      │  vec![] │
  └───────┘      └──────────────┘      └────┬────┘
       ▲                                    │
       │          ┌──────────┐              │  host executes
       │          │ Effect   │◀─────────────┘  each effect
       └──────────│ Completed│
                  └──────────┘
```

- **Event** — a serial input: `Tick(Election)`, `MessageReceived(…)`, `Propose {…}`, `EffectCompleted {…}`, `Shutdown`.
- **RaftCore::step()** — the transition function. Applies the event, mutates internal state, returns a `Vec<Effect>`.
- **Effect** — a task for the host: `Persist(batch)`, `Apply(entries)`, `SendMessage { to, msg }`, `BuildSnapshot {…}`, `InstallSnapshot {…}`, `ProposalResult {…}`, `ReadReady {…}`, etc.
- **EffectCompleted** — the host's confirmation that the effect was executed. For correctness-critical effects (persist, apply, snapshot install), this is the **only** way the core learns the work is done.

### Correctness barriers

Some effects are *barriers* — the core will not advance past them until the
host confirms completion:

| Effect | Barrier semantics |
|--------|-------------------|
| `Persist` | Blocks replication, commit advancement, and apply until durability is confirmed. |
| `Apply` | At most one apply in flight. Blocks the next apply batch until the state machine catches up. |
| `InstallSnapshot` | Blocks responding to the leader until the snapshot is loaded into the state machine. |

Storage or state-machine failure is reported as `EffectOutcome::Failed`. The
core stops permanently — there is no online recovery. The host must validate
durable state and construct a new core with a fresh effect generation. This
**fail-stop** design avoids the complexity of in-place recovery.

---

## 3. Internal Architecture

The core is organised into six layers, each with a single responsibility:

```
┌─────────────────────────────────────────────────┐
│  6. Core Orchestration  (core.rs, invariant.rs) │  step(), message dispatch,
│     RaftCore struct, PersistContinuation,       │  effect completion, invariants
│     election state machine                      │
├─────────────────────────────────────────────────┤
│  5. Protocol Helpers  (raft/)                   │  is_log_up_to_date,
│     election, commit, replication, read_index,  │  quorum_commit, validate_prefix,
│     snapshot                                    │  ReadRound, SnapshotReceiver
├─────────────────────────────────────────────────┤
│  4. Host Contract  (effect.rs, event.rs)        │  Effect enum, Event enum,
│     Effect-Event loop boundary                  │  EffectOutcome, PersistBatch
├─────────────────────────────────────────────────┤
│  3. Core Data Structures                        │
│     log/ (RaftLog, Entry, Unstable)             │  Logical log across snapshot
│     progress/ (Progress, Inflights, Quorum)     │  boundary, per-follower state
│     protocol/ (Envelope, Message)               │  wire-independent DTOs
├─────────────────────────────────────────────────┤
│  2. Durable State  (state.rs)                   │  HardState, SnapshotRecord,
│     Recovery validation                         │  RecoveredState
├─────────────────────────────────────────────────┤
│  1. Foundation  (types.rs, config.rs, error.rs) │  NodeId, Term, LogIndex, …
│     Newtype IDs, validated config, error types  │  ConfigBuilder, FatalError
└─────────────────────────────────────────────────┘
```

### Type-safe identifiers

Every protocol counter is a distinct newtype. The compiler prevents
accidental mixing:

| Type | Wraps | Semantics |
|------|-------|-----------|
| `NodeId` | `u64` | A node in the cluster |
| `Term` | `u64` | A Raft election term |
| `LogIndex` | `u64` | A position in the replicated log |
| `ProposalId` | `u64` | A client write request |
| `ReadId` | `u64` | A client linearizable read |
| `SnapshotId` | `u128` | An immutable snapshot body |

```rust
fn vote(term: Term, candidate: NodeId) { … }
// vote(LogIndex::new(5), Term::new(3));  // compile error
```

### The `PersistContinuation` pattern

Persistence is asynchronous from the core's perspective. When the core queues
a `Persist` effect, it stores a **continuation** — a closure over what happens
next. The continuations form a small enumerated state machine:

```
PersistContinuation:
  BroadcastVoteRequests      → broadcast RequestVote after self-vote is durable
  SendVoteResponse           → send VoteResponse after the vote grant is durable
  ActivateLeader             → become Leader after the no-op entry is durable
  SendAppendEntriesResponse  → respond to AppendEntries after new entries are durable
  ReplicateProposal          → replicate a proposal after it is durable locally
  ApplyCommitted             → issue Apply after the commit index is durable
  None                       → no follow-up (e.g., stepping down to a higher term)
```

This is the most important pattern in the codebase for understanding how the
async effect model works: **persist, then continue**.

---

## 4. Raft Protocol Walkthrough

### 4.1 Leader Election with PreVote

```
 Follower             PreCandidate             Candidate              Leader
    │                     │                       │                     │
    │  election timeout   │                       │                     │
    │────────────────────▶│                       │                     │
    │                     │  PreVote(term+1)      │                     │
    │                     │──────────────────────▶│ (peers)             │
    │                     │                       │                     │
    │                     │◀─ PreVoteResponse ─ ─ │                     │
    │                     │  (quorum?)            │                     │
    │                     │                       │                     │
    │                     │  become_candidate()   │                     │
    │                     │──────────────────────▶│                     │
    │                     │                       │  persist(term,      │
    │                     │                       │    voted_for=self)  │
    │                     │                       │                     │
    │                     │                       │  RequestVote(term)  │
    │                     │                       │────────────────────▶│ (peers)
    │                     │                       │                     │
    │                     │                       │◀─ VoteResponse ─ ─ -│
    │                     │                       │  (quorum?)          │
    │                     │                       │                     │
    │                     │                       │  persist(noop)      │
    │                     │                       │────────────────────▶│
    │                     │                       │                     │  ActivateLeader
```

**Why PreVote?** Without PreVote, a node partitioned from the cluster would
increment its term on every election timeout, forcing a new election when it
reconnects. Even if it can't win (its log is stale), it disrupts the active
leader. PreVote asks "would you vote for me *if* I incremented my term?"
without actually incrementing it. A partitioned node never gets past PreVote,
and the working cluster is undisturbed.

### 4.2 Log Replication with Conflict Optimisation

When a follower rejects an `AppendEntries` because of a log inconsistency, it
returns a `ConflictHint`:

```
Leader                          Follower
  │                                │
  │  AppendEntries(prev=10,        │
  │    prev_term=3, entries=[…])   │
  │───────────────────────────────▶│
  │                                │  term at index 10 is 4, not 3!
  │                                │  first_index_of_term(4) = 7
  │                                │
  │◀──── AppendEntriesResponse ────│
  │     conflict={index:7,         │
  │              term:Some(4)}     │
  │                                │
  │  The leader now probes at      │
  │  last_index_of_term(4) + 1     │
  │  instead of decrementing       │
  │  next_index one by one.        │
```

**The naive approach** decrements `next_index` by one and retries — O(N)
round trips in the worst case. **The conflict-hint optimisation** (Raft paper
§5.3) skips entire conflicting term ranges in one step. The follower reports
the first index of the conflicting term, and the leader jumps to right after
the last index of that term in its own log.

### 4.3 ReadIndex: Linearizable Reads Without Writing

Raft can serve linearizable reads without appending a log entry:

```
Leader                              Followers
  │                                     │
  │  Heartbeat(context=term||counter)   │
  │────────────────────────────────────▶│  (broadcast)
  │                                     │
  │◀── AppendEntriesResponse ───────────│  (echoes context back)
  │         read_context=…              │
  │                                     │
  │  Quorum acknowledged?               │
  │  capture commit_index ─── safe_idx  │
  │                                     │
  │  Wait for local apply ≥ safe_idx    │
  │                                     │
  │  Emit ReadReady { read_index }      │
  │                                     │
```

**Step 1 — Heartbeat confirmation.** The leader sends empty `AppendEntries`
carrying a unique context (`term || counter`). A quorum of followers echoing
that context back proves the leader still holds its leadership.

**Step 2 — Capture the safe index.** The commit index at the moment quorum is
reached becomes the *read index* — any state at or beyond this index is
guaranteed linearizable.

**Step 3 — Apply barrier.** The read is released only after the local state
machine has applied through the safe index. This ensures the read observes
all entries that were committed when leadership was confirmed.

Multiple concurrent reads share a single `ReadRound` (batching), and the
leader's initial no-op entry gates reads — they are deferred until the no-op
commits (§6.4: proving the leader knows the commit index).

### 4.4 Snapshot Streaming

Snapshots are transferred in bounded chunks to avoid exceeding message size
limits:

```
Leader                                          Follower
  │                                                │
  │  InstallSnapshot(offset=0, done=false, …)      │
  │───────────────────────────────────────────────▶│ StoreSnapshotChunk
  │  InstallSnapshot(offset=64KB, done=false, …)   │
  │───────────────────────────────────────────────▶│ StoreSnapshotChunk
  │                      …                         │
  │  InstallSnapshot(offset=N, done=true, …)       │
  │───────────────────────────────────────────────▶│ StoreSnapshotChunk
  │                                                │──▶ SHA-256 check
  │                                                │──▶ Persist(snapshot record)
  │                                                │──▶ InstallSnapshot (state machine)
  │◀── InstallSnapshotResponse(success=true) ──────│
  │                                                │
```

The receiver maintains a running SHA-256 hash of all received bytes and
verifies the final digest against the snapshot metadata. Each chunk is
idempotent — if the leader resends the last chunk after a lost response,
the receiver accepts the exact duplicate rather than failing the transfer.

The leader side (`SnapshotSender`) tracks the current byte offset and paces
chunks through the host storage layer, one chunk at a time, per follower.

### 4.5 CheckQuorum: Leader Self-Preservation

A leader that loses connectivity must not believe it is still the leader
indefinitely. On each heartbeat tick the leader checks:

```rust
if active_members.len() < quorum {
    become_follower();  // step down immediately
}
```

`active_members` is cleared at the start of each heartbeat round and
repopulated as followers respond. If fewer than a quorum respond within
`check_quorum_ticks`, the leader steps down without waiting for an election
timeout. This bounds the window of a "disconnected leader" serving stale reads.

---

## 5. Design Decisions Worth Highlighting

### 5.1 Fail-Stop, Not Fail-Recover

When storage or the state machine fails, the core stops permanently. There is
no retry loop, no in-place recovery. The host must:

1. Validate durable state through `RecoveredState::from_parts()`.
2. Restore the state machine from the last snapshot.
3. Construct a new `RaftCore` with a fresh effect generation.

This keeps the core simple and avoids the complexity of online recovery. All
effects from the old generation are rejected by the new core (generation
mismatch).

### 5.2 Commands Through `Arc<C>`

`EntryPayload::Command(Arc<C>)` means the same command value can appear
simultaneously in three places — the persistence batch, the replication batch,
and the apply batch — without cloning. The host picks `C`; the core never
inspects or deserialises it.

### 5.3 Defensive Validation at Every Layer

| Layer | What it checks |
|-------|---------------|
| `ConfigBuilder::build()` | Membership constraints, capacity relationships, timer ordering |
| `RecoveredState::from_parts()` | Format version, log continuity, term monotonicity, commit/snapshot relationship |
| `invariant::validate()` | Applied ≤ committed, no log gaps, cache consistency — **after every step** |
| `checked_next()` / `checked_add()` | Integer overflow prevention at every index increment |

### 5.4 Fixed Membership

Membership is validated once at construction and stored immutably in the
`Config`. Dynamic membership changes (joint consensus) are explicitly out of
scope. This eliminates an entire class of bugs and simplifies the state space
considerably.

---

## 6. Summary

Ruft is a library, not a framework. It implements the Raft protocol as a pure
state machine and leaves every operational concern to the host. The design
favours:

| Principle | Mechanism |
|-----------|-----------|
| **Determinism** | No wall clock, no randomness in the core; PRNG seed from config |
| **Type safety** | Newtype wrappers for every protocol counter |
| **Explicit barriers** | `Persist`, `Apply`, and snapshot effects block until confirmed |
| **Fail-stop** | Storage/state-machine failures terminate the core; no online recovery |
| **Optimisations** | PreVote, CheckQuorum, ReadIndex, conflict-hint term-skipping, pipelined replication, snapshot streaming |

