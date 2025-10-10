use criterion::{
    black_box, criterion_group, criterion_main, BenchmarkId, Criterion, SamplingMode, Throughput,
};
mod support;

fn bench_topk(c: &mut Criterion) {
    let base_size = support::usize_env("GZSET_TOPK_SIZE", 200_000);
    let measurement = support::duration_env("GZSET_BENCH_MEASUREMENT_SECS", 10.0);
    let warmup = support::duration_env("GZSET_BENCH_WARMUP_SECS", 3.0);
    let sample_size = support::usize_env("GZSET_BENCH_SAMPLE_SIZE", 10);

    let uniform_entries = support::uniform_random(base_size, base_size as f64 * 4.0);
    let uniform_set = Box::leak(Box::new(support::build_set(&uniform_entries)));
    let tie_entries = support::same_score(base_size, 128.0);
    let tie_set = Box::leak(Box::new(support::build_set(&tie_entries)));

    let datasets = [("uniform_random", uniform_set), ("same_score", tie_set)];
    let ks = [10usize, 1_000, 10_000];

    let mut group = c.benchmark_group("topk");
    group.measurement_time(measurement);
    group.warm_up_time(warmup);
    group.sample_size(sample_size);
    group.sampling_mode(SamplingMode::Flat);

    for (name, set) in datasets {
        let len = set.len();
        for &k in &ks {
            let k = k.min(len);
            if k == 0 {
                continue;
            }

            let top_indices: Vec<usize> = (len - k..len).collect();
            let bottom_indices: Vec<usize> = (0..k).collect();

            group.throughput(Throughput::Elements(k as u64));
            group.bench_function(BenchmarkId::new(format!("top/{k}"), name), |b| {
                let mut results = vec![0.0f64; k];
                b.iter(|| {
                    for (slot, &rank) in top_indices.iter().enumerate() {
                        let (_, score) = set.select_by_rank(rank);
                        results[slot] = score;
                    }
                    black_box(&results);
                });
            });

            group.throughput(Throughput::Elements(k as u64));
            group.bench_function(BenchmarkId::new(format!("bottom/{k}"), name), |b| {
                let mut results = vec![0.0f64; k];
                b.iter(|| {
                    for (slot, &rank) in bottom_indices.iter().enumerate() {
                        let (_, score) = set.select_by_rank(rank);
                        results[slot] = score;
                    }
                    black_box(&results);
                });
            });
        }
    }

    group.finish();
}

criterion_group!(benches, bench_topk);
criterion_main!(benches);
