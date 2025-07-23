mod helpers;

#[test]
fn flush_clears_sets() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;

    redis::cmd("GZADD")
        .arg("s")
        .arg(1)
        .arg("a")
        .execute(&mut con);
    redis::cmd("FLUSHDB").query::<()>(&mut con)?;
    let len: i64 = redis::cmd("GZCARD").arg("s").query(&mut con)?;
    assert_eq!(len, 0);

    redis::cmd("GZADD")
        .arg("s")
        .arg(1)
        .arg("b")
        .execute(&mut con);
    redis::cmd("FLUSHALL").query::<()>(&mut con)?;
    let len: i64 = redis::cmd("GZCARD").arg("s").query(&mut con)?;
    assert_eq!(len, 0);
    Ok(())
}
