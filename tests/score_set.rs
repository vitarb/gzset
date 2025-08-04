use gzset::ScoreSet;

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
