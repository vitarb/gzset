use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

mod support;

fn bench_lookup(c: &mut Criterion) {
    let lookup_size = support::usize_env("GZSET_BENCH_LOOKUP_SIZE", 200_000);
    let query_count = support::usize_env("GZSET_BENCH_QUERY_COUNT", 50_000);
    let entries = support::uniform_random(lookup_size, lookup_size as f64);
    let set = Box::leak(Box::new(support::build_set(&entries)));
    let existing = support::pick_existing(set, query_count);
    let missing: Vec<String> = (0..existing.len())
        .map(|i| format!("missing:{i}"))
        .collect();

    let mut group = c.benchmark_group("lookup");
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(3));
    group.sample_size(10);
    group.throughput(Throughput::Elements(existing.len() as u64));
    group.bench_function("rank/existing_random", |b| {
        b.iter(|| {
            for member in &existing {
                let res = set.rank(black_box(member.as_str()));
                black_box(res);
            }
        });
    });
    group.throughput(Throughput::Elements(missing.len() as u64));
    group.bench_function("rank/missing_random", |b| {
        b.iter(|| {
            for member in &missing {
                let res = set.rank(black_box(member.as_str()));
                black_box(res);
            }
        });
    });
    group.throughput(Throughput::Elements(existing.len() as u64));
    group.bench_function("score/existing_random", |b| {
        b.iter(|| {
            for member in &existing {
                let res = set.score(black_box(member.as_str()));
                black_box(res);
            }
        });
    });
    group.throughput(Throughput::Elements(missing.len() as u64));
    group.bench_function("score/missing_random", |b| {
        b.iter(|| {
            for member in &missing {
                let res = set.score(black_box(member.as_str()));
                black_box(res);
            }
        });
    });
    group.finish();
}

criterion_group!(benches, bench_lookup);
criterion_main!(benches);
