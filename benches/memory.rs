use criterion::{criterion_group, criterion_main, Criterion};

mod support;

type DatasetGenerator = fn(usize) -> Vec<(f64, String)>;

fn bench_memory(_: &mut Criterion) {
    const SIZES: [usize; 5] = [10_000, 50_000, 100_000, 500_000, 1_000_000];
    let datasets: [(&str, DatasetGenerator); 3] = [
        ("unique_increasing", support::unique_increasing),
        ("same_score", same_score_dataset),
        ("uniform_random", uniform_random_dataset),
    ];

    for &(name, generator) in &datasets {
        for &n in &SIZES {
            let entries = generator(n);
            let set = support::build_set(&entries);
            let total_bytes = support::mem_usage_bytes(&set);
            let bench_id = format!("memory/{name}/{n}");
            support::record_mem(bench_id.as_str(), total_bytes);
            support::record_structural_mem(bench_id.as_str(), set.mem_bytes());
        }
    }
}

fn same_score_dataset(n: usize) -> Vec<(f64, String)> {
    support::same_score(n, 42.0)
}

fn uniform_random_dataset(n: usize) -> Vec<(f64, String)> {
    support::uniform_random(n, n as f64)
}

criterion_group!(benches, bench_memory);
criterion_main!(benches);
