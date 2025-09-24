mod helpers;

use std::collections::HashSet;

#[test]
fn gzrandmember_samples() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;

    for i in 0..50 {
        redis::cmd("GZADD")
            .arg("s")
            .arg(i)
            .arg(format!("m{i}"))
            .execute(&mut con);
    }

    // single random member
    let single: String = redis::cmd("GZRANDMEMBER").arg("s").query(&mut con)?;
    assert!(single.starts_with('m'));

    // single random member with score
    let with_scores: Vec<String> = redis::cmd("GZRANDMEMBER")
        .arg("s")
        .arg("WITHSCORES")
        .query(&mut con)?;
    assert_eq!(with_scores.len(), 2);
    assert!(with_scores[0].starts_with('m'));
    let _score: f64 = with_scores[1].parse().unwrap();

    // multiple distinct members
    let items: Vec<String> = redis::cmd("GZRANDMEMBER").arg("s").arg(5).query(&mut con)?;
    assert_eq!(items.len(), 5);
    let set: HashSet<_> = items.iter().cloned().collect();
    assert_eq!(set.len(), 5);

    // repeated samples should be random order (allow a few retries before failing)
    let mut unique_order_found = false;
    for _ in 0..5 {
        let mut seen_orders: HashSet<Vec<String>> = HashSet::new();
        let mut all_unique = true;
        for _ in 0..20 {
            let sample: Vec<String> = redis::cmd("GZRANDMEMBER").arg("s").arg(5).query(&mut con)?;
            if !seen_orders.insert(sample.clone()) {
                all_unique = false;
                break;
            }
        }
        if all_unique {
            unique_order_found = true;
            break;
        }
    }
    assert!(
        unique_order_found,
        "expected to observe non-repeating random orders"
    );

    // allow duplicates
    let mut saw_duplicate = false;
    for _ in 0..20 {
        let dup_items: Vec<String> = redis::cmd("GZRANDMEMBER")
            .arg("s")
            .arg(-5)
            .query(&mut con)?;
        assert_eq!(dup_items.len(), 5);
        for item in &dup_items {
            assert!(item.starts_with('m'));
        }
        let uniq: HashSet<_> = dup_items.iter().collect();
        if uniq.len() < dup_items.len() {
            saw_duplicate = true;
            break;
        }
    }
    assert!(
        saw_duplicate,
        "expected to observe duplicates with replacement"
    );

    // large count should return all members in random order
    let bulk: Vec<String> = redis::cmd("GZRANDMEMBER")
        .arg("s")
        .arg(100)
        .query(&mut con)?;
    assert_eq!(bulk.len(), 50);
    let uniq: HashSet<_> = bulk.iter().cloned().collect();
    assert_eq!(uniq.len(), 50);
    let expected: Vec<String> = (0..50).map(|i| format!("m{i}")).collect();
    let expected_set: HashSet<_> = expected.iter().cloned().collect();
    assert_eq!(uniq, expected_set);
    assert_ne!(bulk, expected);

    Ok(())
}
