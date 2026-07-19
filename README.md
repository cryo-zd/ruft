# Ruft

Ruft is a deterministic, runtime-agnostic Raft consensus core for Rust. It owns
protocol transitions and emits effects for the host to perform; it does not own
timers, networking, storage, or the application state machine.

The current release implements fixed-membership Raft: PreVote leader election,
CheckQuorum, durable log replication, pipelined follower progress, commit and
ordered apply, quorum-confirmed ReadIndex, local snapshot compaction, streaming
snapshot installation, and fail-stop fault handling. Cluster membership changes
are intentionally out of scope.

## Design

A host serially calls `RaftCore::step` with an `Event` and executes the returned
`Effect`s. Work that affects correctness is asynchronous from the core’s point
of view: the host must report the matching `EffectCompleted` event only after
that work has crossed its required durability or state-machine boundary.

```text
Event -> RaftCore::step -> Effects -> host executes work -> EffectCompleted
```

The core has no runtime, networking, codec, or storage dependency. Commands do
not require `Clone`, serialization, or `Send` bounds.

## Minimal Host

Run the in-memory demonstration with:

```bash
cargo run --example minimal_host
```

It elects a single-node leader, persists the required Raft state in a mock host,
and commits one command. The example deliberately keeps its storage and state
machine in memory; it is not a production storage adapter.

## Host Contract

- Execute events serially for each `RaftCore` instance.
- Persist every `PersistBatch` atomically and in issued order. Report `Persisted`
  only after the selected durability boundary is reached.
- Apply `Apply` entries in order and without gaps. Application commands should
  be idempotent when replay could repeat an externally visible side effect.
- Build and store snapshot bodies before reporting `SnapshotBuilt`; persist
  snapshot metadata before compacting the log; install received snapshots before
  acknowledging them to a leader.
- Bind authenticated transport identity to `Envelope::from`, validate message
  sizes before delivery, and discard network-send failures locally. Transport
  failure is retryable; storage and state-machine failure must be completed as
  `EffectOutcome::Failed` and stops the core.
- Treat `Status::is_stopped()` as terminal for the running instance. Recover by
  validating durable state, restoring the referenced snapshot into the state
  machine, and constructing a new core with a new effect generation.

Fixed membership is validated at construction and stored in snapshot metadata.
No event changes the member set.

## Reads and Snapshots

`ReadReady` is emitted only after a current-term leader has quorum confirmation
and the local state machine has applied the read index. A read must therefore be
served from state at or beyond `read_index`.

Snapshots are externally stored. The core coordinates metadata, opaque snapshot
references, chunk bounds, checksums, persistence ordering, compaction, and
installation, while the host owns body bytes and lifecycle cleanup.

## License

Ruft is distributed under the [MIT License](LICENSE).
