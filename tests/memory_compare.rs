mod helpers;

fn used_memory(con: &mut redis::Connection) -> redis::RedisResult<i64> {
    let info: String = redis::cmd("INFO").arg("MEMORY").query(con)?;
    for line in info.lines() {
        if let Some(rest) = line.strip_prefix("used_memory:") {
            return Ok(rest.trim().parse::<i64>().unwrap());
        }
    }
    Err((redis::ErrorKind::TypeError, "used_memory not found").into())
}

#[test]
fn memory_compare() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;

    redis::cmd("MEMORY").arg("PURGE").query::<()>(&mut con)?;
    let baseline = used_memory(&mut con)?;

    let mut pipe = redis::pipe();
    for i in 0..1000 {
        pipe.cmd("GZADD").arg("gz").arg(i).arg(i);
    }
    pipe.query::<()>(&mut con)?;

    let delta = used_memory(&mut con)? - baseline;
    let gz_usage: i64 = redis::cmd("MEMORY")
        .arg("USAGE")
        .arg("gz")
        .query(&mut con)?;
    const ALLOWANCE: i64 = 4 * 1024; // account for allocator overhead
    assert!(
        (delta - gz_usage).abs() <= ALLOWANCE,
        "delta {delta} vs usage {gz_usage}"
    );

    redis::cmd("DEL").arg("gz").query::<()>(&mut con)?;
    redis::cmd("MEMORY").arg("PURGE").query::<()>(&mut con)?;

    let mut pipe = redis::pipe();
    for i in 0..1000 {
        pipe.cmd("ZADD").arg("zs").arg(i).arg(i);
    }
    pipe.query::<()>(&mut con)?;
    redis::cmd("MEMORY").arg("PURGE").query::<()>(&mut con)?;

    let zs_usage: i64 = redis::cmd("MEMORY")
        .arg("USAGE")
        .arg("zs")
        .query(&mut con)?;

    assert!(gz_usage < zs_usage, "gz {gz_usage} vs zset {zs_usage}");
    Ok(())
}
