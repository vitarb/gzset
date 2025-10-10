use criterion::{
    black_box, criterion_group, criterion_main, BatchSize, Criterion, SamplingMode, Throughput,
};
use gzset::ScoreSet;
use rand::{seq::SliceRandom, Rng};

mod support;

fn bench_churn(c: &mut Criterion) {
    let base_size = support::usize_env("GZSET_CHURN_BASE", 100_000);
    let script_len = support::usize_env("GZSET_CHURN_SCRIPT", 50_000);
    let measurement = support::duration_env("GZSET_BENCH_MEASUREMENT_SECS", 10.0);
    let warmup = support::duration_env("GZSET_BENCH_WARMUP_SECS", 3.0);
    let sample_size = support::usize_env("GZSET_BENCH_SAMPLE_SIZE", 10);

    let base_entries = support::uniform_random(base_size, base_size as f64 * 4.0);
    let script = build_script(&base_entries, script_len);

    let mut group = c.benchmark_group("churn");
    group.measurement_time(measurement);
    group.warm_up_time(warmup);
    group.sample_size(sample_size);
    group.sampling_mode(SamplingMode::Flat);
    group.throughput(Throughput::Elements(script.len() as u64));

    group.bench_function("script_mixed", |b| {
        b.iter_batched(
            || support::build_set(&base_entries),
            |mut set| {
                apply_script(&mut set, &script);
                black_box(set.len());
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

#[derive(Clone)]
enum Operation {
    Insert { member: String, score: f64 },
    Update { member: String, score: f64 },
    Remove { member: String },
}

#[derive(Clone)]
struct MemberState {
    name: String,
    score: f64,
}

fn build_script(base_entries: &[(f64, String)], script_len: usize) -> Vec<Operation> {
    let mut rng = support::seeded_rng();
    let update_count = script_len * 50 / 100;
    let insert_count = script_len * 30 / 100;
    let remove_count = script_len - update_count - insert_count;

    let mut op_types = Vec::with_capacity(script_len);
    op_types.extend(std::iter::repeat(OpType::Update).take(update_count));
    op_types.extend(std::iter::repeat(OpType::Insert).take(insert_count));
    op_types.extend(std::iter::repeat(OpType::Remove).take(remove_count));
    op_types.shuffle(&mut rng);

    let mut existing: Vec<MemberState> = base_entries
        .iter()
        .map(|(score, member)| MemberState {
            name: member.clone(),
            score: *score,
        })
        .collect();
    let mut next_insert_id = 0usize;
    let base_score_span = (base_entries.len().max(1) as f64) * 4.0;

    let mut script = Vec::with_capacity(op_types.len());
    for op in op_types {
        match op {
            OpType::Update => {
                if existing.is_empty() {
                    continue;
                }
                let idx = rng.gen_range(0..existing.len());
                let state = &mut existing[idx];
                let delta = if rng.gen_bool(0.7) {
                    rng.gen_range(-1.0..=1.0)
                } else {
                    rng.gen_range(-750.0..=750.0)
                };
                state.score += delta;
                script.push(Operation::Update {
                    member: state.name.clone(),
                    score: state.score,
                });
            }
            OpType::Insert => {
                let member = format!("new:{next_insert_id}");
                next_insert_id += 1;
                let score = rng.gen_range(0.0..base_score_span);
                existing.push(MemberState {
                    name: member.clone(),
                    score,
                });
                script.push(Operation::Insert { member, score });
            }
            OpType::Remove => {
                if existing.is_empty() {
                    continue;
                }
                let idx = rng.gen_range(0..existing.len());
                let state = existing.swap_remove(idx);
                script.push(Operation::Remove { member: state.name });
            }
        }
    }

    script
}

fn apply_script(set: &mut ScoreSet, script: &[Operation]) {
    for op in script {
        match op {
            Operation::Insert { member, score } | Operation::Update { member, score } => {
                set.insert(*score, member);
            }
            Operation::Remove { member } => {
                set.remove(member);
            }
        }
    }
}

#[derive(Clone, Copy)]
enum OpType {
    Insert,
    Update,
    Remove,
}

criterion_group!(benches, bench_churn);
criterion_main!(benches);
