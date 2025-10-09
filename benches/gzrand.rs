use std::cell::RefCell;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rand::{seq::index::sample, Rng};

mod support;
const COUNT_SMALL: usize = 64;

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
        let indices = sample(&mut rng, len, COUNT_SMALL).into_vec();
        b.iter(|| {
            for &idx in &indices {
                black_box(set.select_by_rank(idx));
            }
        });
    });

    group.throughput(Throughput::Elements(count_large as u64));
    group.bench_function("count_pos_large", |b| {
        let mut rng = support::seeded_rng();
        let indices = sample(&mut rng, len, count_large).into_vec();
        let mut sorted_indices = indices.clone();
        sorted_indices.sort_unstable();
        b.iter(|| {
            for &idx in &sorted_indices {
                black_box(set.select_by_rank(idx));
            }
        });
    });

    group.throughput(Throughput::Elements(count_large as u64));
    group.bench_function("count_neg_with_replacement", |b| {
        let mut rng = support::seeded_rng();
        let indices: Vec<usize> = (0..count_large).map(|_| rng.gen_range(0..len)).collect();
        b.iter(|| {
            for &idx in &indices {
                black_box(set.select_by_rank(idx));
            }
        });
    });

    group.finish();
}

criterion_group!(benches, bench_randmember);
criterion_main!(benches);
