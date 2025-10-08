use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput,
};
use rand::{seq::SliceRandom, Rng};

mod support;

fn bench_insert(c: &mut Criterion) {
    let insert_size = support::usize_env("GZSET_BENCH_INSERT_SIZE", 200_000);
    let unique_entries = support::unique_increasing(insert_size);
    let uniform_entries = support::uniform_random(insert_size, insert_size as f64);
    let high_ties_entries = build_high_ties(insert_size);

    let mut group = c.benchmark_group("insert");
    let measurement = support::duration_env("GZSET_BENCH_MEASUREMENT_SECS", 10.0);
    let warmup = support::duration_env("GZSET_BENCH_WARMUP_SECS", 3.0);
    let sample_size = support::usize_env("GZSET_BENCH_SAMPLE_SIZE", 10);
    group.measurement_time(measurement);
    group.warm_up_time(warmup);
    group.sample_size(sample_size);
    for (name, entries) in [
        ("unique_increasing", &unique_entries),
        ("uniform_random", &uniform_entries),
        ("high_ties", &high_ties_entries),
    ] {
        let dataset = BenchmarkId::new("insert", name);
        group.throughput(Throughput::Elements(entries.len() as u64));
        group.bench_with_input(dataset, entries, |b, data| {
            b.iter(|| {
                let set = support::build_set(data);
                black_box(set.len());
            });
        });
        let built = support::build_set(entries);
        let mem = support::mem_usage_bytes(&built);
        support::record_mem(format!("insert/{name}"), mem);
    }
    group.finish();
}

fn bench_update(c: &mut Criterion) {
    let update_size = support::usize_env("GZSET_BENCH_UPDATE_SIZE", 150_000);
    let update_touch = support::usize_env("GZSET_BENCH_UPDATE_TOUCH", 25_000);
    let base_entries = support::uniform_random(update_size, update_size as f64);
    let mut rng = support::seeded_rng();
    let mut indices: Vec<usize> = (0..base_entries.len()).collect();
    indices.shuffle(&mut rng);
    let touch = update_touch.min(base_entries.len());
    let mut nearby_updates = Vec::with_capacity(touch);
    let mut far_updates = Vec::with_capacity(touch);
    for &idx in indices.iter().take(touch) {
        let (score, member) = &base_entries[idx];
        let mut delta = rng.gen_range(-0.25..=0.25);
        if delta == 0.0 {
            delta = 0.125;
        }
        nearby_updates.push((member.clone(), score + delta));
        let far_target = rng.gen_range(0.0..(update_size as f64 * 10.0));
        far_updates.push((member.clone(), far_target));
    }

    let mut group = c.benchmark_group("update");
    let measurement = support::duration_env("GZSET_BENCH_MEASUREMENT_SECS", 10.0);
    let warmup = support::duration_env("GZSET_BENCH_WARMUP_SECS", 3.0);
    let sample_size = support::usize_env("GZSET_BENCH_SAMPLE_SIZE", 10);
    group.measurement_time(measurement);
    group.warm_up_time(warmup);
    group.sample_size(sample_size);
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
