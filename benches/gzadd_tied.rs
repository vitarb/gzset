use criterion::{black_box, criterion_group, criterion_main, Criterion};
use gzset::ScoreSet;

fn bench_insert_many_ties(c: &mut Criterion) {
    let entries: Vec<(f64, String)> = (0..200_000)
        .map(|i| ((i % 1_024) as f64, format!("member:{i}")))
        .collect();

    let mut group = c.benchmark_group("insert_many_ties");
    group.sample_size(10);
    group.bench_function("insert_many_ties", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for (score, member) in &entries {
                set.insert(*score, member);
            }
            black_box(set.len())
        })
    });
    group.finish();
}

criterion_group!(benches, bench_insert_many_ties);
criterion_main!(benches);
