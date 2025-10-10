use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, SamplingMode,
    Throughput,
};
use gzset::ScoreSet;

mod support;

fn bench_string_shape(c: &mut Criterion) {
    let dataset_size = support::usize_env("GZSET_STRING_SHAPE_SIZE", 150_000);
    let measurement = support::duration_env("GZSET_BENCH_MEASUREMENT_SECS", 10.0);
    let warmup = support::duration_env("GZSET_BENCH_WARMUP_SECS", 3.0);
    let sample_size = support::usize_env("GZSET_BENCH_SAMPLE_SIZE", 10);
    let score_query_count = support::usize_env("GZSET_STRING_SCORE_QUERIES", 5_000);
    let rank_query_count = support::usize_env("GZSET_STRING_RANK_QUERIES", 1_000);

    let datasets = [
        ("short_ascii", build_short_ascii(dataset_size)),
        ("long_prefix", build_long_prefix(dataset_size)),
        ("utf8_heavy", build_utf8_heavy(dataset_size)),
    ];

    let mut group = c.benchmark_group("string_shape");
    group.measurement_time(measurement);
    group.warm_up_time(warmup);
    group.sample_size(sample_size);
    group.sampling_mode(SamplingMode::Flat);

    for (name, entries) in datasets {
        let set = Box::leak(Box::new(support::build_set(&entries)));
        let desired = std::cmp::max(score_query_count.min(entries.len()), 1);
        let score_queries = support::pick_existing(set, desired);
        let rank_count = std::cmp::max(rank_query_count.min(score_queries.len()), 1);
        let rank_queries: Vec<String> = score_queries.iter().take(rank_count).cloned().collect();

        group.throughput(Throughput::Elements(entries.len() as u64));
        group.bench_function(BenchmarkId::new("insert", name), |b| {
            b.iter_batched(
                ScoreSet::default,
                |mut set| {
                    for (score, member) in &entries {
                        set.insert(*score, member);
                    }
                    black_box(set.len());
                },
                BatchSize::LargeInput,
            );
        });

        group.throughput(Throughput::Elements(score_queries.len() as u64));
        group.bench_function(BenchmarkId::new("score", name), |b| {
            b.iter(|| {
                for member in &score_queries {
                    black_box(set.score(member));
                }
            });
        });

        group.throughput(Throughput::Elements(rank_queries.len() as u64));
        group.bench_function(BenchmarkId::new("rank", name), |b| {
            b.iter(|| {
                for member in &rank_queries {
                    black_box(set.rank(member));
                }
            });
        });
    }

    group.finish();
}

fn build_short_ascii(n: usize) -> Vec<(f64, String)> {
    (0..n).map(|i| (i as f64, format!("a{i}"))).collect()
}

fn build_long_prefix(n: usize) -> Vec<(f64, String)> {
    (0..n)
        .map(|i| {
            let member = format!(
                "commonprefix/commonprefix/branch/{:08}/leaf/{:08}/trail",
                i,
                i.rotate_left(3)
            );
            (i as f64, member)
        })
        .collect()
}

fn build_utf8_heavy(n: usize) -> Vec<(f64, String)> {
    let accents = ["公測", "пример", "canción", "データ", "δοκιμή"];
    (0..n)
        .map(|i| {
            let prefix = accents[i % accents.len()];
            let member = format!(
                "{prefix}|🌟%Δ|møøse|条目{:06}|管道{:06}",
                i,
                i.rotate_right(5)
            );
            (i as f64, member)
        })
        .collect()
}

criterion_group!(benches, bench_string_shape);
criterion_main!(benches);
