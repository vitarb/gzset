mod helpers;

#[test]
fn gzrem_deletes_empty_key() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;

    redis::cmd("GZADD")
        .arg("k")
        .arg(1)
        .arg("a")
        .execute(&mut con);
    redis::cmd("GZREM").arg("k").arg("a").execute(&mut con);
    let t: String = redis::cmd("TYPE").arg("k").query(&mut con)?;
    assert_eq!(t, "none");
    Ok(())
}
