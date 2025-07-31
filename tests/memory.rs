mod helpers;

#[test]
fn mem_usage_reports_non_zero() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;

    redis::cmd("GZADD")
        .arg("s")
        .arg(1)
        .arg("a")
        .execute(&mut con);
    let usage: i64 = redis::cmd("MEMORY").arg("USAGE").arg("s").query(&mut con)?;
    assert!(usage > 0, "reported usage must be > 0");
    Ok(())
}

#[test]
fn mem_usage_matches_rss() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;

    let mut pipe = redis::pipe();
    for i in 0..1_000_000u32 {
        pipe.cmd("GZADD").arg("gz").arg(i).arg(i);
    }
    pipe.query::<()>(&mut con)?;

    let used: i64 = redis::cmd("MEMORY")
        .arg("USAGE")
        .arg("gz")
        .query(&mut con)?;
    let info: String = redis::cmd("INFO").arg("MEMORY").query(&mut con)?;
    let rss: i64 = info
        .lines()
        .find_map(|l| l.strip_prefix("used_memory:"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let ratio = (used as f64) / rss as f64;
    println!("used={used} rss={rss} ratio={ratio:.3}");
    assert!(ratio > 0.95);
    Ok(())
}
