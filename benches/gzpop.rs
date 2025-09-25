use criterion::{black_box, criterion_group, criterion_main, Criterion};
use gzset::ScoreSet;

const ENTRY_COUNT: usize = 100_000;
const REPEAT_POPS: usize = 50;

fn bench_pop(c: &mut Criterion) {
    let members: Vec<String> = (0..ENTRY_COUNT).map(|i| format!("member:{i}")).collect();

    let mut group = c.benchmark_group("pop_loop_vs_baseline");
    group.sample_size(10);

    group.bench_function("pop_min", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for (idx, member) in members.iter().enumerate() {
                set.insert(idx as f64, member);
            }
            black_box(set.pop_all(true));
        })
    });

    group.bench_function("pop_min_same_score", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for member in &members {
                set.insert(0.0, member);
            }
            black_box(set.pop_all(true));
        })
    });

    group.bench_function("pop_max", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for (idx, member) in members.iter().enumerate() {
                set.insert(idx as f64, member);
            }
            black_box(set.pop_all(false));
        })
    });

    group.bench_function("pop_max_same_score", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for member in &members {
                set.insert(0.0, member);
            }
            black_box(set.pop_all(false));
        })
    });

    group.bench_function("pop_min_n1_same_score", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for member in &members {
                set.insert(0.0, member);
            }
            for _ in 0..REPEAT_POPS {
                let popped = set.pop_n(true, 1);
                black_box(&popped);
            }
        })
    });

    group.bench_function("pop_max_n1_same_score", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for member in &members {
                set.insert(0.0, member);
            }
            for _ in 0..REPEAT_POPS {
                let popped = set.pop_n(false, 1);
                black_box(&popped);
            }
        })
    });

    group.bench_function("pop_min_one_same_score", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for member in &members {
                set.insert(0.0, member);
            }
            for _ in 0..REPEAT_POPS {
                let popped = set.pop_one(true);
                black_box(&popped);
            }
        })
    });

    group.bench_function("pop_max_one_same_score", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for member in &members {
                set.insert(0.0, member);
            }
            for _ in 0..REPEAT_POPS {
                let popped = set.pop_one(false);
                black_box(&popped);
            }
        })
    });

    group.finish();
}

criterion_group!(benches, bench_pop);
criterion_main!(benches);
