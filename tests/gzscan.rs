mod helpers;

#[test]
fn gzscan_exact_batches() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;

    let mut pipe = redis::pipe();
    for i in 0..20 {
        pipe.cmd("GZADD").arg("s").arg(i).arg(format!("m{i}"));
    }
    pipe.query::<()>(&mut con)?;

    let (cur1, arr1): (String, Vec<String>) =
        redis::cmd("GZSCAN").arg("s").arg("0").query(&mut con)?;
    assert_ne!(cur1, "0");
    assert_eq!(arr1.len(), 20);

    let (cur2, arr2): (String, Vec<String>) =
        redis::cmd("GZSCAN").arg("s").arg(&cur1).query(&mut con)?;
    assert_eq!(cur2, "0");
    assert_eq!(arr2.len(), 20);

    let mut members = Vec::new();
    for chunk in arr1.chunks(2).chain(arr2.chunks(2)) {
        members.push(chunk[0].clone());
    }
    let expected: Vec<String> = (0..20).map(|i| format!("m{i}")).collect();
    assert_eq!(members, expected);
    Ok(())
}

#[test]
fn gzscan_mutation_between_calls() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;

    let mut pipe = redis::pipe();
    for i in 0..15 {
        pipe.cmd("GZADD").arg("s").arg(i).arg(format!("m{i}"));
    }
    pipe.query::<()>(&mut con)?;

    let (cur1, arr1): (String, Vec<String>) =
        redis::cmd("GZSCAN").arg("s").arg("0").query(&mut con)?;
    assert_ne!(cur1, "0");

    // mutate between scans
    redis::cmd("GZREM").arg("s").arg("m10").execute(&mut con);
    redis::cmd("GZADD")
        .arg("s")
        .arg(20)
        .arg("m20")
        .execute(&mut con);

    let (cur2, arr2): (String, Vec<String>) =
        redis::cmd("GZSCAN").arg("s").arg(&cur1).query(&mut con)?;
    assert_eq!(cur2, "0");

    let mut members = Vec::new();
    for chunk in arr1.chunks(2).chain(arr2.chunks(2)) {
        members.push(chunk[0].clone());
    }
    let expected: Vec<String> = redis::cmd("GZRANGE")
        .arg("s")
        .arg(0)
        .arg(-1)
        .query(&mut con)?;
    // After mutation, the union of scanned items should match the final set.
    // As with Redis SCAN, duplicates or skips are not guaranteed across changes.
    assert_eq!(members, expected);
    Ok(())
}
