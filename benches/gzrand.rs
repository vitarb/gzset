use std::cell::RefCell;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use rand::{seq::index::sample, Rng};
use rustc_hash::FxHashSet;

mod support;

const RAND_SIZE: usize = 200_000;
const COUNT_SMALL: usize = 64;

fn bench_randmember(c: &mut Criterion) {
    let entries = support::zipf_like(RAND_SIZE, 1.2);
    let set = Box::leak(Box::new(support::build_set(&entries)));
    let len = set.len();

    let mut group = c.benchmark_group("randmember");
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
        let rng = RefCell::new(support::seeded_rng());
        b.iter(|| {
            let mut seen: FxHashSet<usize> = FxHashSet::default();
            let mut taken = Vec::with_capacity(COUNT_SMALL);
            let mut guard = rng.borrow_mut();
            while taken.len() < COUNT_SMALL {
                let idx = guard.gen_range(0..len);
                if seen.insert(idx) {
                    taken.push(set.select_by_rank(idx));
                }
            }
            black_box(taken.len());
        });
    });

    group.throughput(Throughput::Elements(count_large as u64));
    group.bench_function("count_pos_large", |b| {
        let rng = RefCell::new(support::seeded_rng());
        b.iter(|| {
            let sample = sample(&mut rng.borrow_mut(), len, count_large).into_vec();
            let mut indices = sample.clone();
            indices.sort_unstable();
            let mut results = Vec::with_capacity(indices.len());
            for idx in indices {
                results.push(set.select_by_rank(idx));
            }
            black_box(results.len());
        });
    });

    group.throughput(Throughput::Elements(count_large as u64));
    group.bench_function("count_neg_with_replacement", |b| {
        let rng = RefCell::new(support::seeded_rng());
        b.iter(|| {
            let mut out = Vec::with_capacity(count_large);
            let mut guard = rng.borrow_mut();
            for _ in 0..count_large {
                let idx = guard.gen_range(0..len);
                out.push(set.select_by_rank(idx));
            }
            black_box(out.len());
        });
    });

    group.finish();
}

criterion_group!(benches, bench_randmember);
criterion_main!(benches);
