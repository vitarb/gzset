use gzset::ScoreSet;
use rand::{rngs::StdRng, Rng, SeedableRng};

#[test]
fn lexicographic_order_equal_scores() {
    let mut set = ScoreSet::default();
    set.insert(1.0, "b");
    set.insert(1.0, "a");
    set.insert(1.0, "c");
    let items: Vec<_> = set.range_iter(0, -1);
    let members: Vec<_> = items.into_iter().map(|(_, m)| m).collect();
    assert_eq!(members, ["a", "b", "c"]);
}

#[test]
fn lexicographic_order_reinsert_equal_scores() {
    let mut set = ScoreSet::default();
    set.insert(1.0, "b");
    set.insert(1.0, "a");
    set.remove("b");
    set.insert(1.0, "b");
    let items: Vec<_> = set.range_iter(0, -1);
    let members: Vec<_> = items.into_iter().map(|(_, m)| m).collect();
    assert_eq!(members, ["a", "b"]);
}

#[test]
fn pop_min_max_duplicates() {
    let mut set = ScoreSet::default();
    for m in ["b", "a", "c"] {
        set.insert(1.0, m);
    }
    let mut mins = Vec::new();
    for (_, m) in set.range_iter(0, -1) {
        mins.push(m.clone());
        set.remove(&m);
    }
    assert_eq!(mins, ["a", "b", "c"]);

    for m in ["b", "a", "c"] {
        set.insert(1.0, m);
    }
    let mut maxs = Vec::new();
    for (_, m) in set.range_iter(0, -1).into_iter().rev() {
        maxs.push(m.clone());
        set.remove(&m);
    }
    assert_eq!(maxs, ["c", "b", "a"]);
}

#[test]
fn insertion_order_five_members() {
    let mut set = ScoreSet::default();
    for m in ["e", "d", "c", "b", "a"] {
        set.insert(1.0, m);
    }
    let items: Vec<_> = set.range_iter(0, -1);
    let members: Vec<_> = items.into_iter().map(|(_, m)| m).collect();
    assert_eq!(members, ["a", "b", "c", "d", "e"]);
}

#[test]
fn duplicate_reject() {
    let mut set = ScoreSet::default();
    assert!(set.insert(1.0, "a"));
    assert!(!set.insert(1.0, "a"));
}

#[test]
fn grow_and_shrink_bucket() {
    const SHRINK_THRESHOLD: usize = 64;
    let total = SHRINK_THRESHOLD + 5;
    let mut set = ScoreSet::default();
    let names: Vec<String> = (0..total).map(|i| format!("member-{i}")).collect();
    for name in &names {
        assert!(set.insert(1.0, name));
    }

    let initial_cap = set
        .bucket_capacity_for_test(1.0)
        .expect("bucket should spill");
    assert!(
        initial_cap > SHRINK_THRESHOLD,
        "capacity should exceed shrink threshold after inserts",
    );

    for name in &names[..(total - SHRINK_THRESHOLD)] {
        assert!(set.remove(name));
    }

    let cap_at_threshold = set
        .bucket_capacity_for_test(1.0)
        .expect("bucket should remain after removals");
    const CAPACITY_SLOP: usize = 2;
    assert!(
        cap_at_threshold <= SHRINK_THRESHOLD + CAPACITY_SLOP,
        "capacity should shrink near threshold when remaining == threshold: {cap_at_threshold}",
    );

    let next_index = total - SHRINK_THRESHOLD;
    assert!(set.remove(&names[next_index]));

    let cap_below_threshold = set
        .bucket_capacity_for_test(1.0)
        .expect("bucket should remain while more than one member persists");
    assert!(
        cap_below_threshold <= SHRINK_THRESHOLD + CAPACITY_SLOP,
        "capacity should stay within threshold when remaining < threshold: {cap_below_threshold}",
    );

    for name in &names[(next_index + 1)..] {
        assert!(set.remove(name));
    }
    assert!(set.is_empty());
}

#[test]
fn compact_tail_when_head_small() {
    const SHRINK_THRESHOLD: usize = 64;
    let total = SHRINK_THRESHOLD + 36;
    let mut set = ScoreSet::default();
    let names: Vec<String> = (0..total).map(|i| format!("member-{i}")).collect();
    for name in &names {
        assert!(set.insert(1.0, name));
    }

    let remaining = SHRINK_THRESHOLD - 4;
    let popped = total - remaining;
    let removed = set.pop_n(true, popped);
    assert_eq!(removed.len(), popped);

    let cap_after = set
        .bucket_capacity_for_test(1.0)
        .expect("bucket should remain spilled");
    const CAPACITY_SLOP: usize = 2;
    assert!(
        cap_after <= remaining + CAPACITY_SLOP,
        "capacity should compact when tail small: {cap_after}",
    );
}

#[test]
fn rank_matches_naive_random() {
    let mut rng = StdRng::seed_from_u64(0);
    for _ in 0..10 {
        let mut set = ScoreSet::default();
        let mut members = Vec::new();
        for i in 0..100 {
            let member = format!("m{i}");
            let score: f64 = rng.gen();
            set.insert(score, &member);
            members.push(member);
        }
        let items = set.range_iter(0, -1);
        let mut naive = std::collections::HashMap::new();
        for (i, (_, m)) in items.iter().enumerate() {
            naive.insert(m.clone(), i);
        }
        for m in members {
            let r_new = set.rank(&m).expect("rank");
            let r_old = naive[&m];
            assert_eq!(r_new, r_old, "member {m}");
        }
    }
}
