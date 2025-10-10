use criterion::{
    black_box, criterion_group, criterion_main, measurement::WallTime, BenchmarkId, Criterion,
    SamplingMode, Throughput,
};
use gzset::ScoreSet;
use ordered_float::OrderedFloat;

mod support;

fn bench_range(c: &mut Criterion) {
    let range_size = support::usize_env("GZSET_BENCH_RANGE_SIZE", 500_000);

    let unique_entries = support::unique_increasing(range_size);
    let unique_set: &'static ScoreSet = Box::leak(Box::new(support::build_set(&unique_entries)));
    let same_score_entries = support::same_score(range_size, 42.0);
    let same_score_set: &'static ScoreSet =
        Box::leak(Box::new(support::build_set(&same_score_entries)));
    let clustered_entries = support::clustered(range_size, 8, 4.0);
    let clustered_set: &'static ScoreSet =
        Box::leak(Box::new(support::build_set(&clustered_entries)));

    let datasets = [
        ("unique_increasing", unique_set),
        ("same_score", same_score_set),
    ];

    let mut group = c.benchmark_group("gzrange_iter");
    let measurement = support::duration_env("GZSET_BENCH_MEASUREMENT_SECS", 10.0);
    let warmup = support::duration_env("GZSET_BENCH_WARMUP_SECS", 3.0);
    let sample_size = support::usize_env("GZSET_BENCH_SAMPLE_SIZE", 10);
    group.measurement_time(measurement);
    group.warm_up_time(warmup);
    group.sample_size(sample_size);
    group.sampling_mode(SamplingMode::Flat);

    for (name, set) in datasets {
        add_range_benches(&mut group, name, set);
    }
    group.finish();

    let score_datasets = [
        ("unique_increasing", unique_set),
        ("same_score", same_score_set),
        ("clustered", clustered_set),
    ];

    let mut score_group = c.benchmark_group("gzrange_score");
    score_group.measurement_time(measurement);
    score_group.warm_up_time(warmup);
    score_group.sample_size(sample_size);
    score_group.sampling_mode(SamplingMode::Flat);

    for (name, set) in score_datasets {
        add_score_range_benches(&mut score_group, name, set);
        add_score_range_benches_rev(&mut score_group, name, set);
    }
    score_group.finish();
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

fn add_score_range_benches(
    group: &mut criterion::BenchmarkGroup<'_, WallTime>,
    name: &str,
    set: &ScoreSet,
) {
    let mut ordered: Vec<(OrderedFloat<f64>, &str)> = set
        .iter_all()
        .map(|(member, score)| (OrderedFloat(score), member))
        .collect();
    if ordered.is_empty() {
        return;
    }
    ordered.sort_unstable();

    let len = ordered.len();
    let wide_target = ((len as f64) * 0.65).round() as usize;
    let specs = [
        ("narrow", 1_000usize),
        ("medium", 10_000usize),
        ("wide", wide_target),
    ];

    for (label, target) in specs {
        let desired = target.clamp(1, len);
        let mid = len / 2;
        let mut start_idx = if desired >= len {
            0
        } else {
            mid.saturating_sub(desired / 2)
        };
        let mut end_idx = start_idx + desired - 1;
        if end_idx >= len {
            end_idx = len - 1;
            start_idx = len - desired;
        }

        let min_score = ordered[start_idx].0 .0;
        let max_score = ordered[end_idx].0 .0;
        let start_member = ordered[start_idx].1;
        let end_member = ordered[end_idx].1;

        let min_key = OrderedFloat(min_score);
        let mut count = 0usize;
        for (member, score) in set.iter_from(min_key, start_member, true) {
            if score > max_score || (score == max_score && member > end_member) {
                break;
            }
            count += 1;
        }

        if count == 0 {
            continue;
        }

        group.throughput(Throughput::Elements(count as u64));
        group.bench_function(BenchmarkId::new(format!("score/{label}"), name), |b| {
            b.iter(|| {
                let mut yielded = 0usize;
                for (member, score) in set.iter_from(min_key, start_member, true) {
                    if score > max_score || (score == max_score && member > end_member) {
                        break;
                    }
                    black_box(member);
                    black_box(score);
                    yielded += 1;
                }
                black_box(yielded);
            });
        });
    }
}

fn add_score_range_benches_rev(
    group: &mut criterion::BenchmarkGroup<'_, WallTime>,
    name: &str,
    set: &ScoreSet,
) {
    let mut ordered: Vec<(OrderedFloat<f64>, &str)> = set
        .iter_all()
        .map(|(member, score)| (OrderedFloat(score), member))
        .collect();
    if ordered.is_empty() {
        return;
    }
    ordered.sort_unstable();

    let len = ordered.len();
    let wide_target = ((len as f64) * 0.65).round() as usize;
    let specs = [
        ("narrow", 1_000usize),
        ("medium", 10_000usize),
        ("wide", wide_target),
    ];

    for (label, target) in specs {
        let desired = target.clamp(1, len);
        let mid = len / 2;
        let mut start_idx = if desired >= len {
            0
        } else {
            mid.saturating_sub(desired / 2)
        };
        let mut end_idx = start_idx + desired - 1;
        if end_idx >= len {
            end_idx = len - 1;
            start_idx = len - desired;
        }

        let min_score = ordered[start_idx].0 .0;
        let start_member = ordered[start_idx].1;

        let start_rank = start_idx as isize;
        let end_rank = end_idx as isize;
        let mut count = 0usize;
        for (member, score) in set.iter_range(start_rank, end_rank).rev() {
            if score < min_score || (score == min_score && member < start_member) {
                break;
            }
            count += 1;
        }

        if count == 0 {
            continue;
        }

        group.throughput(Throughput::Elements(count as u64));
        group.bench_function(BenchmarkId::new(format!("score_rev/{label}"), name), |b| {
            b.iter(|| {
                let mut yielded = 0usize;
                for (member, score) in set.iter_range(start_rank, end_rank).rev() {
                    if score < min_score || (score == min_score && member < start_member) {
                        break;
                    }
                    black_box(member);
                    black_box(score);
                    yielded += 1;
                }
                black_box(yielded);
            });
        });
    }
}

criterion_group!(benches, bench_range);
criterion_main!(benches);
