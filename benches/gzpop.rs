use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use gzset::ScoreSet;

mod support;

const DEFAULT_ENTRY_COUNT: usize = 100_000;
const DEFAULT_REPEAT_POPS: usize = 50;

fn entry_count() -> usize {
    std::env::var("BENCH_ENTRY_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_ENTRY_COUNT)
}

fn repeat_pops() -> usize {
    std::env::var("BENCH_REPEAT_POPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_REPEAT_POPS)
}

fn bench_pop(c: &mut Criterion) {
    let entry_count = entry_count();
    let repeat_pops = repeat_pops();
    let members: Vec<String> = (0..entry_count).map(|i| format!("member:{i}")).collect();

    let mut group = c.benchmark_group("pop_loop_vs_baseline");
    let measurement = support::duration_env("GZSET_BENCH_MEASUREMENT_SECS", 8.0);
    let warmup = support::duration_env("GZSET_BENCH_WARMUP_SECS", 2.0);
    let sample_size = support::usize_env("GZSET_BENCH_SAMPLE_SIZE", 10);
    group.measurement_time(measurement);
    group.warm_up_time(warmup);
    group.sample_size(sample_size);

    let entry_count_throughput = Throughput::Elements(entry_count as u64);

    group.throughput(entry_count_throughput.clone());
    group.bench_function("pop_min", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for (idx, member) in members.iter().enumerate() {
                set.insert(idx as f64, member);
            }
            black_box(pop_all_members(&mut set, true));
        })
    });

    group.throughput(entry_count_throughput.clone());
    group.bench_function("pop_min_same_score", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for member in &members {
                set.insert(0.0, member);
            }
            black_box(pop_all_members(&mut set, true));
        })
    });

    group.throughput(entry_count_throughput.clone());
    group.bench_function("pop_max", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for (idx, member) in members.iter().enumerate() {
                set.insert(idx as f64, member);
            }
            black_box(pop_all_members(&mut set, false));
        })
    });

    group.throughput(entry_count_throughput);
    group.bench_function("pop_max_same_score", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for member in &members {
                set.insert(0.0, member);
            }
            black_box(pop_all_members(&mut set, false));
        })
    });

    let repeat_throughput = Throughput::Elements(repeat_pops as u64);

    group.throughput(repeat_throughput.clone());
    group.bench_function("pop_min_n1_same_score", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for member in &members {
                set.insert(0.0, member);
            }
            for _ in 0..repeat_pops {
                let popped = set.pop_n(true, 1);
                black_box(&popped);
            }
        })
    });

    group.throughput(repeat_throughput.clone());
    group.bench_function("pop_max_n1_same_score", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for member in &members {
                set.insert(0.0, member);
            }
            for _ in 0..repeat_pops {
                let popped = set.pop_n(false, 1);
                black_box(&popped);
            }
        })
    });

    group.throughput(repeat_throughput.clone());
    group.bench_function("pop_min_one_same_score", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for member in &members {
                set.insert(0.0, member);
            }
            for _ in 0..repeat_pops {
                let popped = set.pop_one(true);
                black_box(&popped);
            }
        })
    });

    group.throughput(repeat_throughput);
    group.bench_function("pop_max_one_same_score", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for member in &members {
                set.insert(0.0, member);
            }
            for _ in 0..repeat_pops {
                let popped = set.pop_one(false);
                black_box(&popped);
            }
        })
    });

    group.finish();
}

fn pop_all_members(set: &mut ScoreSet, min: bool) -> Vec<String> {
    set.pop_n(min, set.len())
        .into_iter()
        .map(|(member, _)| member)
        .collect()
}

criterion_group!(benches, bench_pop);
criterion_main!(benches);
