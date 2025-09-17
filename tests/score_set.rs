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
    let mut set = ScoreSet::default();
    for m in ["a", "b", "c", "d", "e"] {
        set.insert(1.0, m);
    }
    assert!(set.bucket_capacity_for_test(1.0).is_some_and(|c| c > 4));
    set.remove("a");
    set.remove("b");
    assert_eq!(set.bucket_capacity_for_test(1.0), Some(3));
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
