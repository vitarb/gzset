mod helpers;

use std::collections::HashSet;

#[test]
fn gzrandmember_samples() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;

    for i in 0..100 {
        redis::cmd("GZADD")
            .arg("s")
            .arg(i)
            .arg(format!("m{i}"))
            .execute(&mut con);
    }

    // single random member
    let single: String = redis::cmd("GZRANDMEMBER").arg("s").query(&mut con)?;
    assert!(single.starts_with('m'));

    // multiple distinct members
    let items: Vec<String> = redis::cmd("GZRANDMEMBER").arg("s").arg(5).query(&mut con)?;
    assert_eq!(items.len(), 5);
    let set: HashSet<_> = items.iter().cloned().collect();
    assert_eq!(set.len(), 5);

    // allow duplicates
    let dup_items: Vec<String> = redis::cmd("GZRANDMEMBER")
        .arg("s")
        .arg(-5)
        .query(&mut con)?;
    assert_eq!(dup_items.len(), 5);
    for item in dup_items {
        assert!(item.starts_with('m'));
    }

    Ok(())
}
