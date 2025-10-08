use criterion::{
    black_box, criterion_group, criterion_main, measurement::WallTime, BenchmarkId, Criterion,
    SamplingMode, Throughput,
};
use gzset::ScoreSet;

mod support;

fn bench_range(c: &mut Criterion) {
    let range_size = support::usize_env("GZSET_BENCH_RANGE_SIZE", 500_000);
    let datasets = [
        ("unique_increasing", support::unique_increasing(range_size)),
        ("same_score", support::same_score(range_size, 42.0)),
    ];

    let mut group = c.benchmark_group("gzrange_iter");
    let measurement = support::duration_env("GZSET_BENCH_MEASUREMENT_SECS", 10.0);
    let warmup = support::duration_env("GZSET_BENCH_WARMUP_SECS", 3.0);
    let sample_size = support::usize_env("GZSET_BENCH_SAMPLE_SIZE", 10);
    group.measurement_time(measurement);
    group.warm_up_time(warmup);
    group.sample_size(sample_size);
    group.sampling_mode(SamplingMode::Flat);

    for (name, entries) in datasets {
        let set = Box::leak(Box::new(support::build_set(&entries)));
        add_range_benches(&mut group, name, set);
    }
    group.finish();
}

fn add_range_benches(
    group: &mut criterion::BenchmarkGroup<'_, WallTime>,
    name: &str,
    set: &ScoreSet,
) {
    let len = set.len() as isize;
    let window_1k = 1_000;
    let window_10k = 10_000;
    let mid_start = len / 2 - (window_10k as isize / 2);
    let window_start = mid_start.max(0);

    group.throughput(Throughput::Elements(window_1k as u64));
    group.bench_function(BenchmarkId::new("iter/window_1k", name), |b| {
        b.iter(|| {
            let mut iter = set.iter_range_fwd(window_start, window_start + window_1k as isize - 1);
            for item in &mut iter {
                black_box(item);
            }
        });
    });

    group.throughput(Throughput::Elements(window_10k as u64));
    group.bench_function(BenchmarkId::new("iter/window_10k", name), |b| {
        b.iter(|| {
            let mut iter = set.iter_range_fwd(window_start, window_start + window_10k as isize - 1);
            for item in &mut iter {
                black_box(item);
            }
        });
    });

    group.throughput(Throughput::Elements(set.len() as u64));
    group.bench_function(BenchmarkId::new("iter/whole_set", name), |b| {
        b.iter(|| {
            let mut iter = set.iter_range_fwd(0, len - 1);
            for item in &mut iter {
                black_box(item);
            }
        });
    });
}

criterion_group!(benches, bench_range);
criterion_main!(benches);
