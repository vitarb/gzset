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
