use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, SamplingMode, Throughput};
use gzset::ScoreSet;
use ordered_float::OrderedFloat;

fn bench_range(c: &mut Criterion) {
    let entries: Vec<(f64, String)> = (0..1_000_000).map(|i| (i as f64, i.to_string())).collect();
    let mut group = c.benchmark_group("gzrange_iter");
    group.measurement_time(Duration::from_secs(12));
    group.warm_up_time(Duration::from_secs(3));
    group.sample_size(10);
    group.sampling_mode(SamplingMode::Flat);

    group.throughput(Throughput::Elements(entries.len() as u64));
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
    let tail_len = entries.len() - (entries.len() * 9 / 10);

    group.throughput(Throughput::Elements(tail_len as u64));
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
    group.throughput(Throughput::Elements(tail_len as u64));
    group.bench_function("iter_from_90pct_hot", |b| {
        let mut set = ScoreSet::default();
        for (s, m) in &entries {
            set.insert(*s, m);
        }
        let start_idx = entries.len() * 9 / 10;
        let start_score = OrderedFloat(entries[start_idx].0);
        let member = entries[start_idx].1.as_str();
        b.iter(|| {
            let mut iter = set.iter_from(start_score, member, false);
            for _ in &mut iter {}
        })
    });
    group.throughput(Throughput::Elements(tail_len as u64));
    group.bench_function("iter_from_gap_90pct", |b| {
        b.iter(|| {
            let mut set = ScoreSet::default();
            for (s, m) in &entries {
                set.insert(*s, m);
            }
            let start_idx = entries.len() * 9 / 10;
            let start_score = OrderedFloat(entries[start_idx].0 + 0.5);
            let mut iter = set.iter_from(start_score, "", false);
            for _ in &mut iter {}
        })
    });
    group.throughput(Throughput::Elements(tail_len as u64));
    group.bench_function("iter_from_gap_90pct_hot", |b| {
        let mut set = ScoreSet::default();
        for (s, m) in &entries {
            set.insert(*s, m);
        }
        let start_idx = entries.len() * 9 / 10;
        let start_score = OrderedFloat(entries[start_idx].0 + 0.5);
        b.iter(|| {
            let mut iter = set.iter_from(start_score, "", false);
            for _ in &mut iter {}
        })
    });
    group.finish();
}

criterion_group!(benches, bench_range);
criterion_main!(benches);
