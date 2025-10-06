use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};

mod support;

const LOOKUP_SIZE: usize = 200_000;
const QUERY_COUNT: usize = 50_000;

fn bench_lookup(c: &mut Criterion) {
    let entries = support::uniform_random(LOOKUP_SIZE, LOOKUP_SIZE as f64);
    let set = Box::leak(Box::new(support::build_set(&entries)));
    let existing = support::pick_existing(set, QUERY_COUNT);
    let missing: Vec<String> = (0..existing.len())
        .map(|i| format!("missing:{i}"))
        .collect();

    let mut group = c.benchmark_group("lookup");
    group.throughput(Throughput::Elements(existing.len() as u64));
    group.bench_function("rank/existing_random", |b| {
        b.iter(|| {
            for member in &existing {
                let res = set.rank(black_box(member.as_str()));
                black_box(res);
            }
        });
    });
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
