mod helpers;

#[test]
fn gzpopmin_registered() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;

    // prepare
    redis::cmd("GZADD")
        .arg("s")
        .arg(1)
        .arg("a")
        .execute(&mut con);

    // must succeed
    let res: Vec<String> = redis::cmd("GZPOPMIN").arg("s").query(&mut con)?;
    assert_eq!(res, vec!["a".to_string(), "1".to_string()]);
    Ok(())
}
