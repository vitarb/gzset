use criterion::{black_box, criterion_group, criterion_main, BatchSize, Criterion, Throughput};

mod support;

const REMOVE_SIZE: usize = 150_000;
const REMOVE_COUNT: usize = 25_000;

fn bench_remove(c: &mut Criterion) {
    let base_entries = support::unique_increasing(REMOVE_SIZE);
    let mut shuffled_members: Vec<String> = base_entries
        .iter()
        .map(|(_, member)| member.clone())
        .collect();
    support::shuffle_members(&mut shuffled_members);
    let random_targets = shuffled_members[..REMOVE_COUNT.min(shuffled_members.len())].to_vec();
    let front_targets: Vec<String> = base_entries
        .iter()
        .take(REMOVE_COUNT)
        .map(|(_, m)| m.clone())
        .collect();
    let back_targets: Vec<String> = base_entries
        .iter()
        .rev()
        .take(REMOVE_COUNT)
        .map(|(_, m)| m.clone())
        .collect();

    record_remove_delta("random_existing", &base_entries, &random_targets);
    record_remove_delta("cluster_front", &base_entries, &front_targets);
    record_remove_delta("cluster_back", &base_entries, &back_targets);

    let mut group = c.benchmark_group("remove");
    group.throughput(Throughput::Elements(random_targets.len() as u64));
    group.bench_function("random_existing", |b| {
        b.iter_batched(
            || support::build_set(&base_entries),
            |mut set| {
                let before = support::mem_usage_bytes(&set);
                for member in &random_targets {
                    let removed = set.remove(member);
                    black_box(removed);
                }
                let after = support::mem_usage_bytes(&set);
                black_box(before.saturating_sub(after));
            },
            BatchSize::SmallInput,
        );
    });
    group.throughput(Throughput::Elements(front_targets.len() as u64));
    group.bench_function("cluster_front", |b| {
        b.iter_batched(
            || support::build_set(&base_entries),
            |mut set| {
                let before = support::mem_usage_bytes(&set);
                for member in &front_targets {
                    let removed = set.remove(member);
                    black_box(removed);
                }
                let after = support::mem_usage_bytes(&set);
                black_box(before.saturating_sub(after));
            },
            BatchSize::SmallInput,
        );
    });
    group.throughput(Throughput::Elements(back_targets.len() as u64));
    group.bench_function("cluster_back", |b| {
        b.iter_batched(
            || support::build_set(&base_entries),
            |mut set| {
                let before = support::mem_usage_bytes(&set);
                for member in &back_targets {
                    let removed = set.remove(member);
                    black_box(removed);
                }
                let after = support::mem_usage_bytes(&set);
                black_box(before.saturating_sub(after));
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn record_remove_delta(name: &str, entries: &[(f64, String)], removals: &[String]) {
    let mut set = support::build_set(entries);
    let before = support::mem_usage_bytes(&set);
    for member in removals {
        let _ = set.remove(member);
    }
    let after = support::mem_usage_bytes(&set);
    support::record_memory_csv("remove", name, before.saturating_sub(after));
}

criterion_group!(benches, bench_remove);
criterion_main!(benches);
