use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

mod support;

// The rank benchmarks use a separate query-count knob to keep each sample short
// enough for Criterion's default measurement window. Override via
// GZSET_BENCH_QUERY_COUNT_RANK.
fn bench_lookup(c: &mut Criterion) {
    let lookup_size = support::usize_env("GZSET_BENCH_LOOKUP_SIZE", 200_000);
    let query_count = support::usize_env("GZSET_BENCH_QUERY_COUNT", 10_000);
    let rank_existing_count = std::env::var("GZSET_BENCH_QUERY_COUNT_RANK")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(3_000);
    let entries = support::uniform_random(lookup_size, lookup_size as f64);
    let set = Box::leak(Box::new(support::build_set(&entries)));
    let existing = support::pick_existing(set, query_count);
    let rank_existing = &existing[..existing.len().min(rank_existing_count)];
    let missing: Vec<String> = (0..existing.len())
        .map(|i| format!("missing:{i}"))
        .collect();

    let mut group = c.benchmark_group("lookup");
    let measurement = support::duration_env("GZSET_BENCH_MEASUREMENT_SECS", 10.0);
    let warmup = support::duration_env("GZSET_BENCH_WARMUP_SECS", 3.0);
    let sample_size = support::usize_env("GZSET_BENCH_SAMPLE_SIZE", 10);
    group.measurement_time(measurement);
    group.warm_up_time(warmup);
    group.sample_size(sample_size);
    group.throughput(Throughput::Elements(rank_existing.len() as u64));
    group.bench_function("rank/existing_random", |b| {
        b.iter(|| {
            for member in rank_existing {
                let res = set.rank(black_box(member.as_str()));
                black_box(res);
            }
        });
    });

    #[cfg(feature = "bench-internals")]
    let existing_handles: Vec<_> = rank_existing
        .iter()
        .map(|member| {
            set.rank_find_only(member.as_str())
                .expect("existing member must resolve")
        })
        .collect();

    #[cfg(feature = "bench-internals")]
    {
        group.throughput(Throughput::Elements(rank_existing.len() as u64));
        group.bench_function("rank/find_only", |b| {
            b.iter(|| {
                for member in rank_existing {
                    let handle = set
                        .rank_find_only(black_box(member.as_str()))
                        .expect("existing member must resolve");
                    black_box(handle);
                }
            });
        });

        group.throughput(Throughput::Elements(rank_existing.len() as u64));
        group.bench_function("rank/resolve_only", |b| {
            b.iter(|| {
                for handle in &existing_handles {
                    let rank = set.rank_resolve_only(*handle);
                    black_box(rank);
                }
            });
        });
    }
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
