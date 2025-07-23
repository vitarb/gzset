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
    if redis::cmd("FLUSHDB")
        .arg("SYNC")
        .query::<()>(&mut con)
        .is_err()
    {
        redis::cmd("FLUSHDB").query::<()>(&mut con)?;
    }
    for _ in 0..40 {
        if redis::cmd("GZCARD").arg("s").query::<i64>(&mut con)? == 0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert_eq!(redis::cmd("GZCARD").arg("s").query::<i64>(&mut con)?, 0);

    redis::cmd("GZADD")
        .arg("s")
        .arg(1)
        .arg("b")
        .execute(&mut con);
    if redis::cmd("FLUSHALL")
        .arg("SYNC")
        .query::<()>(&mut con)
        .is_err()
    {
        redis::cmd("FLUSHALL").query::<()>(&mut con)?;
    }
    for _ in 0..40 {
        if redis::cmd("GZCARD").arg("s").query::<i64>(&mut con)? == 0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    assert_eq!(redis::cmd("GZCARD").arg("s").query::<i64>(&mut con)?, 0);
    Ok(())
}
