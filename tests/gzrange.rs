mod helpers;

#[test]
fn gzrange_large_stream() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;
    let count = 1_000_000u32;
    let mut pipe = redis::pipe();
    for i in 0..count {
        pipe.cmd("GZADD").arg("s").arg(i).arg(i);
    }
    pipe.query::<()>(&mut con)?;

    let res: Vec<String> = redis::cmd("GZRANGE")
        .arg("s")
        .arg(0)
        .arg(-1)
        .query(&mut con)?;
    let expected: Vec<String> = (0..count).map(|i| i.to_string()).collect();
    assert_eq!(res, expected);
    Ok(())
}
