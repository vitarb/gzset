use std::{os::raw::c_void, sync::Mutex};

use gzset::ScoreSet;
use once_cell::sync::Lazy;
use rand::{rngs::StdRng, seq::SliceRandom, Rng, SeedableRng};

pub mod mem;

pub use mem::{record_mem, record_structural_mem};

static BASE_SEED: Lazy<u64> = Lazy::new(|| {
    std::env::var("GZSET_BENCH_SEED")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0x7d11_5eed_f065_cafe)
});

static RNG_COUNTER: Lazy<Mutex<u64>> = Lazy::new(|| Mutex::new(0));

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
extern "C" {
    fn gzset_mem_usage(value: *const c_void) -> usize;
}

pub fn usize_env(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

#[inline]
pub fn seeded_rng() -> StdRng {
    let mut guard = RNG_COUNTER.lock().unwrap();
    let seed = BASE_SEED.wrapping_add(*guard);
    *guard = guard.wrapping_add(1);
    StdRng::seed_from_u64(seed)
}

pub fn unique_increasing(n: usize) -> Vec<(f64, String)> {
    (0..n).map(|i| (i as f64, format!("member:{i}"))).collect()
}

pub fn uniform_random(n: usize, score_range: f64) -> Vec<(f64, String)> {
    let mut rng = seeded_rng();
    (0..n)
        .map(|i| (rng.gen_range(0.0..score_range), format!("rand:{i}")))
        .collect()
}

pub fn same_score(n: usize, score: f64) -> Vec<(f64, String)> {
    (0..n).map(|i| (score, format!("same:{i}"))).collect()
}

pub fn clustered(n: usize, clusters: usize, spread: f64) -> Vec<(f64, String)> {
    assert!(clusters > 0, "clusters must be > 0");
    let mut rng = seeded_rng();
    let mut out = Vec::with_capacity(n);
    let mut generated = 0usize;
    let base_gap = spread.max(1.0);
    for cluster_idx in 0..clusters {
        if generated >= n {
            break;
        }
        let remaining = n - generated;
        let clusters_left = clusters - cluster_idx;
        let target = (remaining + clusters_left - 1) / clusters_left;
        let center = cluster_idx as f64 * base_gap * 10.0;
        for local in 0..target {
            let delta = rng.gen_range(-spread..=spread);
            let score = center + delta;
            out.push((score, format!("cluster:{cluster_idx}:{local}")));
        }
        generated += target;
    }
    out
}

pub fn zipf_like(n: usize, s: f64) -> Vec<(f64, String)> {
    let exponent = s.max(0.5);
    (0..n)
        .map(|i| {
            let rank = (i + 1) as f64;
            let score = rank.powf(exponent);
            (score, format!("zipf:{i}"))
        })
        .collect()
}

pub fn build_set(entries: &[(f64, String)]) -> ScoreSet {
    let mut set = ScoreSet::default();
    for (score, member) in entries {
        set.insert(*score, member);
    }
    set
}

pub fn shuffle_members(members: &mut [String]) {
    let mut rng = seeded_rng();
    members.shuffle(&mut rng);
}

pub fn pick_existing(set: &ScoreSet, k: usize) -> Vec<String> {
    let mut rng = seeded_rng();
    let mut names = set.member_names();
    names.shuffle(&mut rng);
    names.truncate(names.len().min(k));
    names
}

#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
pub fn mem_usage_bytes(set: &ScoreSet) -> usize {
    unsafe {
        let f: unsafe extern "C" fn(*const c_void) -> usize = gzset_mem_usage;
        f(set as *const _ as *const c_void)
    }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub fn mem_usage_bytes(set: &ScoreSet) -> usize {
    set.mem_bytes()
}
