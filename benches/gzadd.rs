use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use rand::{seq::SliceRandom, Rng};

mod support;

const INSERT_SIZE: usize = 200_000;
const UPDATE_SIZE: usize = 150_000;
const UPDATE_TOUCH: usize = 25_000;

fn bench_insert(c: &mut Criterion) {
    let unique_entries = support::unique_increasing(INSERT_SIZE);
    let uniform_entries = support::uniform_random(INSERT_SIZE, INSERT_SIZE as f64);
    let high_ties_entries = build_high_ties(INSERT_SIZE);

    let mut group = c.benchmark_group("insert");
    for (name, entries) in [
        ("unique_increasing", &unique_entries),
        ("uniform_random", &uniform_entries),
        ("high_ties", &high_ties_entries),
    ] {
        let dataset = BenchmarkId::new("insert", name);
        group.throughput(Throughput::Elements(entries.len() as u64));
        group.bench_with_input(dataset, entries, |b, data| {
            b.iter(|| {
                let mut set = support::build_set(data);
                black_box(set.len());
            });
        });
        let built = support::build_set(entries);
        let mem = support::mem_usage_bytes(&built);
        support::record_memory_csv("insert", name, mem);
    }
    group.finish();
}

fn bench_update(c: &mut Criterion) {
    let base_entries = support::uniform_random(UPDATE_SIZE, UPDATE_SIZE as f64);
    let mut rng = support::seeded_rng();
    let mut indices: Vec<usize> = (0..base_entries.len()).collect();
    indices.shuffle(&mut rng);
    let touch = UPDATE_TOUCH.min(base_entries.len());
    let mut nearby_updates = Vec::with_capacity(touch);
    let mut far_updates = Vec::with_capacity(touch);
    for &idx in indices.iter().take(touch) {
        let (score, member) = &base_entries[idx];
        let mut delta = rng.gen_range(-0.25..=0.25);
        if delta == 0.0 {
            delta = 0.125;
        }
        nearby_updates.push((member.clone(), score + delta));
        let far_target = rng.gen_range(0.0..(UPDATE_SIZE as f64 * 10.0));
        far_updates.push((member.clone(), far_target));
    }

    let mut group = c.benchmark_group("update");
    group.throughput(Throughput::Elements(nearby_updates.len() as u64));
    group.bench_function("score_move_nearby", |b| {
        b.iter_batched(
            || support::build_set(&base_entries),
            |mut set| {
                for (member, score) in &nearby_updates {
                    set.insert(*score, member);
                }
                black_box(set.len());
            },
            BatchSize::SmallInput,
        );
    });
    group.throughput(Throughput::Elements(far_updates.len() as u64));
    group.bench_function("score_move_far", |b| {
        b.iter_batched(
            || support::build_set(&base_entries),
            |mut set| {
                for (member, score) in &far_updates {
                    set.insert(*score, member);
                }
                black_box(set.len());
            },
            BatchSize::SmallInput,
        );
    });
    group.finish();
}

fn build_high_ties(target: usize) -> Vec<(f64, String)> {
    let mut rng = support::seeded_rng();
    let mut entries = Vec::with_capacity(target);
    let mut score = 0.0;
    while entries.len() < target {
        let ties = rng.gen_range(1_000..=4_000);
        for local in 0..ties {
            if entries.len() == target {
                break;
            }
            entries.push((score, format!("tie:{score}:{local}")));
        }
        score += 1.0;
    }
    entries
}

criterion_group!(benches, bench_insert, bench_update);
criterion_main!(benches);
