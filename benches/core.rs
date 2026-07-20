use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use ruft::{
    ClusterId, Config, Entry, Envelope, Event, HardState, LogIndex, Message, NodeId, RaftCore,
    RecoveredState, Term,
};

fn follower() -> RaftCore<String> {
    let config = Config::builder(NodeId::new(2), [NodeId::new(1), NodeId::new(2)])
        .cluster_id(ClusterId::new(1))
        .heartbeat_ticks(2)
        .election_ticks(10..=20)
        .check_quorum_ticks(10)
        .build()
        .unwrap();
    let recovered = RecoveredState::new(
        HardState::new(Term::new(1), None, LogIndex::new(0)),
        None,
        Vec::new(),
    )
    .unwrap();
    RaftCore::new(config, recovered).unwrap()
}

fn append_event(count: usize) -> Event<String> {
    let entries = (1..=count)
        .map(|index| Entry::command(LogIndex::new(index as u64), Term::new(1), "x".into(), 1))
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    Event::MessageReceived(Envelope::new(
        ClusterId::new(1),
        NodeId::new(1),
        NodeId::new(2),
        Message::AppendEntries {
            term: Term::new(1),
            prev_log_index: LogIndex::new(0),
            prev_log_term: Term::new(0),
            leader_commit: LogIndex::new(0),
            read_context: None,
            entries,
        },
    ))
}

fn bench_append_entries(criterion: &mut Criterion) {
    let mut group = criterion.benchmark_group("replication");
    for count in [1usize, 16, 256] {
        group.bench_with_input(
            BenchmarkId::new("append_entries", count),
            &count,
            |bench, &count| {
                bench.iter_batched(
                    follower,
                    |mut core| core.step(append_event(count)).unwrap(),
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(core, bench_append_entries);
criterion_main!(core);
