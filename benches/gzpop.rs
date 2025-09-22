use criterion::{black_box, criterion_group, criterion_main, Criterion};
use gzset::ScoreSet;

fn bench_pop(c: &mut Criterion) {
    let entries: Vec<(f64, String)> = (0..1_000_000).map(|i| (i as f64, i.to_string())).collect();
    let mut group = c.benchmark_group("pop_loop_vs_baseline");
    group.bench_function("pop_min", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for (s, m) in &entries {
                set.insert(*s, m);
            }
            let _ = set.pop_all(true);
        })
    });
    group.bench_function("pop_min_same_score", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for (_, m) in &entries {
                set.insert(0.0, m);
            }
            let _ = set.pop_all(true);
        })
    });
    group.bench_function("pop_max", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for (s, m) in &entries {
                set.insert(*s, m);
            }
            let _ = set.pop_all(false);
        })
    });
    group.bench_function("pop_max_same_score", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for (_, m) in &entries {
                set.insert(0.0, m);
            }
            let _ = set.pop_all(false);
        })
    });
    group.bench_function("pop_min_n1_same_score", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for (_, m) in &entries {
                set.insert(0.0, m);
            }
            for _ in 0..100 {
                let popped = set.pop_n(true, 1);
                black_box(&popped);
            }
        })
    });
    group.bench_function("pop_max_n1_same_score", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for (_, m) in &entries {
                set.insert(0.0, m);
            }
            for _ in 0..100 {
                let popped = set.pop_n(false, 1);
                black_box(&popped);
            }
        })
    });
    group.finish();
}

criterion_group!(benches, bench_pop);
criterion_main!(benches);
