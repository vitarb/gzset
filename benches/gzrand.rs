use std::cell::RefCell;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rand::{seq::index::sample, Rng};

mod support;
const COUNT_SMALL: usize = 64;
const INDEX_BATCHES: usize = 16;

fn bench_randmember(c: &mut Criterion) {
    let rand_size = support::usize_env("GZSET_BENCH_RAND_SIZE", 200_000);
    let entries = support::zipf_like(rand_size, 1.2);
    let set = Box::leak(Box::new(support::build_set(&entries)));
    let len = set.len();

    let mut group = c.benchmark_group("randmember");
    let measurement = support::duration_env("GZSET_BENCH_MEASUREMENT_SECS", 10.0);
    let warmup = support::duration_env("GZSET_BENCH_WARMUP_SECS", 3.0);
    let sample_size = support::usize_env("GZSET_BENCH_SAMPLE_SIZE", 10);
    group.measurement_time(measurement);
    group.warm_up_time(warmup);
    group.sample_size(sample_size);
    group.throughput(Throughput::Elements(1));
    group.bench_function("single/no_scores", |b| {
        let rng = RefCell::new(support::seeded_rng());
        b.iter(|| {
            let idx = rng.borrow_mut().gen_range(0..len);
            let (member, _) = set.select_by_rank(idx);
            black_box(member);
        });
    });
    group.bench_function("single/with_scores", |b| {
        let rng = RefCell::new(support::seeded_rng());
        b.iter(|| {
            let idx = rng.borrow_mut().gen_range(0..len);
            let (member, score) = set.select_by_rank(idx);
            black_box((member, score));
        });
    });

    let count_large = (len / 10).max(COUNT_SMALL);
    group.throughput(Throughput::Elements(COUNT_SMALL as u64));
    group.bench_function("count_pos_small", |b| {
        let mut rng = support::seeded_rng();
        let batches: Vec<Vec<usize>> = (0..INDEX_BATCHES)
            .map(|_| sample(&mut rng, len, COUNT_SMALL).into_vec())
            .collect();
        let batch_index = RefCell::new(0usize);
        b.iter(|| {
            let batch_idx = {
                let mut idx = batch_index.borrow_mut();
                let current = *idx;
                *idx = (*idx + 1) % batches.len();
                current
            };
            for &rank in &batches[batch_idx] {
                black_box(set.select_by_rank(rank));
            }
        });
    });

    group.throughput(Throughput::Elements(count_large as u64));
    group.bench_function("count_pos_large", |b| {
        let mut rng = support::seeded_rng();
        let unique_batches: Vec<Vec<usize>> = (0..INDEX_BATCHES)
            .map(|_| sample(&mut rng, len, count_large).into_vec())
            .collect();
        let sorted_batches: Vec<Vec<usize>> = unique_batches
            .iter()
            .map(|indices| {
                let mut sorted = indices.clone();
                sorted.sort_unstable();
                sorted
            })
            .collect();
        let batch_index = RefCell::new(0usize);
        b.iter(|| {
            let batch_idx = {
                let mut idx = batch_index.borrow_mut();
                let current = *idx;
                *idx = (*idx + 1) % sorted_batches.len();
                current
            };
            for &rank in &sorted_batches[batch_idx] {
                black_box(set.select_by_rank(rank));
            }
        });
    });

    group.throughput(Throughput::Elements(count_large as u64));
    group.bench_function("count_neg_with_replacement", |b| {
        let mut rng = support::seeded_rng();
        let batches: Vec<Vec<usize>> = (0..INDEX_BATCHES)
            .map(|_| (0..count_large).map(|_| rng.gen_range(0..len)).collect())
            .collect();
        let batch_index = RefCell::new(0usize);
        b.iter(|| {
            let batch_idx = {
                let mut idx = batch_index.borrow_mut();
                let current = *idx;
                *idx = (*idx + 1) % batches.len();
                current
            };
            for &rank in &batches[batch_idx] {
                black_box(set.select_by_rank(rank));
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_randmember);
criterion_main!(benches);
