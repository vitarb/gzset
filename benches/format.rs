use criterion::{black_box, criterion_group, criterion_main, Criterion};
use gzset::fmt_f64;

fn bench_format(c: &mut Criterion) {
    let mut group = c.benchmark_group("fmt_vs_to_string");
    group.bench_function("fmt_f64", |b| {
        b.iter(|| {
            for _ in 0..1_000_000 {
                black_box(fmt_f64(black_box(42.123456)));
            }
        })
    });
    group.bench_function("to_string", |b| {
        b.iter(|| {
            for _ in 0..1_000_000 {
                black_box(black_box(42.123456).to_string());
            }
        })
    });
    group.finish();
}

criterion_group!(benches, bench_format);
criterion_main!(benches);
