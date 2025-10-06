use std::time::Duration;

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use gzset::ScoreSet;
use rustc_hash::FxHashMap as FastHashMap;

mod support;

const SET_SIZE: usize = 120_000;

fn bench_algebra(c: &mut Criterion) {
    let overlap_cases = [
        ("0pct", 0.0),
        ("25pct", 0.25),
        ("50pct", 0.5),
        ("90pct", 0.9),
    ];

    let mut group = c.benchmark_group("algebra");
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_secs(3));
    group.sample_size(10);
    group.sampling_mode(criterion::SamplingMode::Flat);
    for (label, ratio) in overlap_cases {
        let (set_a, set_b) = two_sets_with_overlap(SET_SIZE, ratio);
        let total = (set_a.len() + set_b.len()) as u64;
        group.throughput(Throughput::Elements(total));
        group.bench_function(format!("union/2sets/{label}"), |b| {
            b.iter(|| {
                let cardinality = union_two(set_a, set_b);
                black_box(cardinality);
            });
        });
        group.bench_function(format!("inter/2sets/{label}"), |b| {
            b.iter(|| {
                let cardinality = inter_two(set_a, set_b);
                black_box(cardinality);
            });
        });
        group.bench_function(format!("diff/2sets/{label}"), |b| {
            b.iter(|| {
                let cardinality = diff_two(set_a, set_b);
                black_box(cardinality);
            });
        });
        group.bench_function(format!("intercard/2sets/{label}"), |b| {
            b.iter(|| {
                let cardinality = intercard_two(set_a, set_b, None);
                black_box(cardinality);
            });
        });
        let limit = set_a.len() / 5;
        group.bench_function(format!("intercard/2sets/{label}/limit"), |b| {
            b.iter(|| {
                let cardinality = intercard_two(set_a, set_b, Some(limit));
                black_box(cardinality);
            });
        });
    }

    let multi_sets = multi_set_family();
    group.throughput(Throughput::Elements(
        multi_sets.iter().map(|s| s.len() as u64).sum(),
    ));
    group.bench_function("union/multikey/6sets", |b| {
        b.iter(|| {
            let cardinality = union_multi(&multi_sets);
            black_box(cardinality);
        });
    });
    group.bench_function("inter/multikey/6sets", |b| {
        b.iter(|| {
            let cardinality = inter_multi(&multi_sets);
            black_box(cardinality);
        });
    });

    group.finish();
}

fn two_sets_with_overlap(size: usize, ratio: f64) -> (&'static ScoreSet, &'static ScoreSet) {
    let entries_a = support::unique_increasing(size);
    let overlap = ((size as f64) * ratio).round() as usize;
    let mut entries_b = Vec::with_capacity(size);
    for (score, member) in entries_a.iter().take(overlap) {
        entries_b.push((score + 0.5, member.clone()));
    }
    let mut extra_idx = 0usize;
    let mut next_score = size as f64;
    while entries_b.len() < size {
        entries_b.push((
            next_score + extra_idx as f64,
            format!("b_extra:{ratio:.2}:{extra_idx}"),
        ));
        extra_idx += 1;
    }
    let set_a = Box::leak(Box::new(support::build_set(&entries_a)));
    let set_b = Box::leak(Box::new(support::build_set(&entries_b)));
    (set_a, set_b)
}

fn multi_set_family() -> Vec<&'static ScoreSet> {
    let configs = [
        ("uniform", 40_000usize),
        ("cluster", 60_000usize),
        ("zipf", 80_000usize),
        ("uniform", 20_000usize),
        ("cluster", 50_000usize),
        ("zipf", 30_000usize),
    ];
    configs
        .iter()
        .map(|(kind, size)| {
            let entries = match *kind {
                "uniform" => support::uniform_random(*size, *size as f64),
                "cluster" => support::clustered(*size, 8, 4.0),
                "zipf" => support::zipf_like(*size, 1.3),
                _ => unreachable!(),
            };
            Box::leak(Box::new(support::build_set(&entries)))
        })
        .collect()
}

fn union_two(a: &ScoreSet, b: &ScoreSet) -> usize {
    let mut agg: FastHashMap<String, f64> = FastHashMap::default();
    agg.reserve(a.len() + b.len());
    for (member, score) in a.iter_all() {
        agg.insert(member.to_owned(), score);
    }
    for (member, score) in b.iter_all() {
        agg.entry(member.to_owned())
            .and_modify(|v| *v += score)
            .or_insert(score);
    }
    agg.len()
}

fn inter_two(a: &ScoreSet, b: &ScoreSet) -> usize {
    let (small, big) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    let mut count = 0usize;
    for (member, _) in small.iter_all() {
        if big.contains(member) {
            count += 1;
        }
    }
    count
}

fn diff_two(a: &ScoreSet, b: &ScoreSet) -> usize {
    let mut count = 0usize;
    for (member, _) in a.iter_all() {
        if !b.contains(member) {
            count += 1;
        }
    }
    count
}

fn intercard_two(a: &ScoreSet, b: &ScoreSet, limit: Option<usize>) -> usize {
    let (small, big) = if a.len() <= b.len() { (a, b) } else { (b, a) };
    let mut count = 0usize;
    for (member, _) in small.iter_all() {
        if big.contains(member) {
            count += 1;
            if let Some(limit) = limit {
                if count >= limit {
                    break;
                }
            }
        }
    }
    count
}

fn union_multi(sets: &[&ScoreSet]) -> usize {
    let mut agg: FastHashMap<String, f64> = FastHashMap::default();
    for set in sets {
        agg.reserve(set.len());
        for (member, score) in set.iter_all() {
            agg.entry(member.to_owned())
                .and_modify(|v| *v += score)
                .or_insert(score);
        }
    }
    agg.len()
}

fn inter_multi(sets: &[&ScoreSet]) -> usize {
    let mut iter = sets.iter();
    let Some(first) = iter.next() else { return 0 };
    let mut acc: FastHashMap<String, f64> = FastHashMap::default();
    for (member, score) in first.iter_all() {
        acc.insert(member.to_owned(), score);
    }
    for set in iter {
        acc.retain(|member, total| match set.score(member) {
            Some(score) => {
                *total += score;
                true
            }
            None => false,
        });
        if acc.is_empty() {
            break;
        }
    }
    acc.len()
}

criterion_group!(benches, bench_algebra);
criterion_main!(benches);
