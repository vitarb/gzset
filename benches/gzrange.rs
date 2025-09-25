use criterion::{criterion_group, criterion_main, Criterion};
use gzset::ScoreSet;
use ordered_float::OrderedFloat;

fn bench_range(c: &mut Criterion) {
    let entries: Vec<(f64, String)> = (0..1_000_000).map(|i| (i as f64, i.to_string())).collect();
    let mut group = c.benchmark_group("gzrange_iter");
    group.sample_size(10);
    group.bench_function("iter", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for (s, m) in &entries {
                set.insert(*s, m);
            }
            let mut iter = set.iter_range_fwd(0, entries.len() as isize - 1);
            for _ in &mut iter {}
        })
    });
    group.bench_function("iter_from_90pct", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for (s, m) in &entries {
                set.insert(*s, m);
            }
            let start_idx = entries.len() * 9 / 10;
            let start_score = OrderedFloat(entries[start_idx].0);
            let member = entries[start_idx].1.as_str();
            let mut iter = set.iter_from(start_score, member, false);
            for _ in &mut iter {}
        })
    });
    group.finish();
}

criterion_group!(benches, bench_range);
criterion_main!(benches);
