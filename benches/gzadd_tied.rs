use criterion::{black_box, criterion_group, criterion_main, Criterion};
use gzset::ScoreSet;

mod support;

fn bench_insert_many_ties(c: &mut Criterion) {
    let entries: Vec<(f64, String)> = (0..200_000)
        .map(|i| ((i % 1_024) as f64, format!("member:{i}")))
        .collect();

    let mut group = c.benchmark_group("insert_many_ties");
    let measurement = support::duration_env("GZSET_BENCH_MEASUREMENT_SECS", 10.0);
    let warmup = support::duration_env("GZSET_BENCH_WARMUP_SECS", 3.0);
    let sample_size = support::usize_env("GZSET_BENCH_SAMPLE_SIZE", 10);
    group.measurement_time(measurement);
    group.warm_up_time(warmup);
    group.sample_size(sample_size);
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

    let built = support::build_set(&entries);
    let mem = support::mem_usage_bytes(&built);
    support::record_mem("insert_many_ties", mem);
}

criterion_group!(benches, bench_insert_many_ties);
criterion_main!(benches);
