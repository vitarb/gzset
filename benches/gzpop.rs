use criterion::{criterion_group, criterion_main, Criterion};
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
    group.bench_function("pop_max", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for (s, m) in &entries {
                set.insert(*s, m);
            }
            let _ = set.pop_all(false);
        })
    });
    group.finish();
}

criterion_group!(benches, bench_pop);
criterion_main!(benches);
