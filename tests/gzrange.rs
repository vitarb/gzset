mod helpers;

#[test]
fn gzrange_large_stream() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;
    let count = 100_000u32;
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

#[test]
fn gzrange_negative_indices() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;

    for i in 0..5 {
        redis::cmd("GZADD")
            .arg("s")
            .arg(i)
            .arg(format!("m{i}"))
            .query::<()>(&mut con)?;
    }

    let res: Vec<String> = redis::cmd("GZRANGE")
        .arg("s")
        .arg(-3)
        .arg(-1)
        .query(&mut con)?;
    assert_eq!(res, vec!["m2", "m3", "m4"]);
    Ok(())
}

#[test]
fn gzrange_empty_set() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;

    let res: Vec<String> = redis::cmd("GZRANGE")
        .arg("missing")
        .arg(0)
        .arg(-1)
        .query(&mut con)?;
    assert!(res.is_empty());
    Ok(())
}
