# Ruft

A deterministic, runtime-agnostic [Raft] consensus core for Rust — it owns
protocol transitions and emits **effects** for the host to perform. No timers,
no networking, no storage engine, no async runtime.

```rust
let mut core = RaftCore::new(config, recovered_state)?;
for event in host_events {
    let output = core.step(event)?;
    for effect in output.effects {
        host.execute(effect);          // persist, apply, send, snapshot, …
    }
}
```

## Features

- **PreVote** — prevents partitioned nodes from disrupting active clusters
- **CheckQuorum** — leader steps down when it loses contact with a majority
- **ReadIndex** — linearizable reads without appending to the log
- **Conflict-hint optimisation** — skips entire conflicting term ranges on rejection
- **Pipelined replication** — bounded in-flight AppendEntries per follower
- **Snapshot streaming** — chunked transfer with SHA-256 verification and idempotent retry
- **Type-safe identifiers** — `NodeId`, `Term`, `LogIndex`, etc. are distinct newtypes
- **Fail-stop safety** — storage or state-machine failure stops the core permanently
- **Zero command bounds** — commands need not implement `Clone`, `Serialize`, or `Send`

Fixed-membership Raft only; dynamic membership changes are out of scope.

## Quick Start

```bash
cargo run --example minimal_host
```

The example boots a single-node cluster in memory, elects a leader, persists
Raft state, and commits one command. It is deliberately minimal — a production
host adds a real transport, storage, and state machine.

## How It Works

The host drives the core in a serial event loop:

```
Event ──▶ RaftCore::step() ──▶ Vec<Effect> ──▶ host executes ──▶ EffectCompleted ──▶ …
```

- **Event** — a tick, an RPC, a proposal, a read request, or an effect completion.
- **Effect** — a host-side task: `Persist`, `Apply`, `SendMessage`, `BuildSnapshot`, …
- **EffectCompleted** — the host reports back after executing the effect.

Correctness-critical effects (`Persist`, `Apply`, snapshot operations) act as
**asynchronous barriers** — the core will not advance past them until the host
confirms completion.

For a deep dive into the architecture, protocol walkthroughs, and design
decisions, see **[Building a Runtime-Agnostic Raft Core](docs/building-a-runtime-agnostic-raft-core.md)**.

## Host Contract (Summary)

- Drive each `RaftCore` instance **serially** — one event at a time.
- Persist every `PersistBatch` **atomically**; report `Persisted` only after the
  durability boundary is reached.
- Apply entries **in index order**, without gaps.
- Bind authenticated transport identity to `Envelope::from`.
- Treat `Status::is_stopped()` as terminal — recover by validating durable state
  and constructing a new core with a fresh effect generation.

Network failures are retryable (drop the send). Storage and state-machine
failures are fatal — report `EffectOutcome::Failed` and the core stops.

## License

[MIT](LICENSE)

[Raft]: https://raft.github.io/raft.pdf
