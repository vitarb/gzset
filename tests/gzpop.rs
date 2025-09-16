mod helpers;
use redis::Value;

#[test]
fn gzpopmin_updates_state_and_memory() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;

    let members = [
        (1.0, "member:a1"),
        (1.0, "member:a2"),
        (2.0, "member:b1"),
        (2.0, "member:b2"),
        (3.0, "member:c1"),
        (4.0, "member:d1"),
    ];
    let mut pipe = redis::pipe();
    for (score, member) in &members {
        pipe.cmd("GZADD").arg("gzpop").arg(*score).arg(*member);
    }
    pipe.query::<()>(&mut con)?;

    let initial_usage: i64 = redis::cmd("MEMORY")
        .arg("USAGE")
        .arg("gzpop")
        .query::<Option<i64>>(&mut con)?
        .expect("initial usage");
    const TOLERANCE: i64 = 4096;
    let max_allowed = initial_usage + TOLERANCE;

    let popped: Vec<String> = redis::cmd("GZPOPMIN").arg("gzpop").arg(3).query(&mut con)?;
    assert_eq!(
        popped,
        vec![
            "member:a1".to_string(),
            "1".to_string(),
            "member:a2".to_string(),
            "1".to_string(),
            "member:b1".to_string(),
            "2".to_string(),
        ]
    );

    let remaining_card: i64 = redis::cmd("GZCARD").arg("gzpop").query(&mut con)?;
    assert_eq!(remaining_card, 3);

    let rank_b2 = match redis::cmd("GZRANK")
        .arg("gzpop")
        .arg("member:b2")
        .query::<Value>(&mut con)?
    {
        Value::Nil => None,
        Value::Int(n) => Some(n),
        other => panic!("unexpected rank response: {other:?}"),
    };
    assert_eq!(rank_b2, Some(0));
    let rank_c1 = match redis::cmd("GZRANK")
        .arg("gzpop")
        .arg("member:c1")
        .query::<Value>(&mut con)?
    {
        Value::Nil => None,
        Value::Int(n) => Some(n),
        other => panic!("unexpected rank response: {other:?}"),
    };
    assert_eq!(rank_c1, Some(1));
    let rank_d1 = match redis::cmd("GZRANK")
        .arg("gzpop")
        .arg("member:d1")
        .query::<Value>(&mut con)?
    {
        Value::Nil => None,
        Value::Int(n) => Some(n),
        other => panic!("unexpected rank response: {other:?}"),
    };
    assert_eq!(rank_d1, Some(2));

    let after_first_usage: i64 = redis::cmd("MEMORY")
        .arg("USAGE")
        .arg("gzpop")
        .query::<Option<i64>>(&mut con)?
        .expect("usage after first pop");
    assert!(
        after_first_usage <= max_allowed,
        "usage grew too much: before={initial_usage} after={after_first_usage}"
    );

    let mut prev_usage = after_first_usage;
    let mut remaining = remaining_card as usize;
    while remaining > 0 {
        let popped_once: Vec<String> = redis::cmd("GZPOPMIN").arg("gzpop").query(&mut con)?;
        assert_eq!(popped_once.len(), 2);
        let card: i64 = redis::cmd("GZCARD").arg("gzpop").query(&mut con)?;
        assert_eq!(card as usize, remaining - 1);
        let usage = redis::cmd("MEMORY")
            .arg("USAGE")
            .arg("gzpop")
            .query::<Option<i64>>(&mut con)?
            .unwrap_or(0);
        assert!(usage <= max_allowed, "usage {usage} exceeded {max_allowed}");
        assert!(
            usage <= prev_usage + TOLERANCE,
            "usage {usage} prev {prev_usage}"
        );
        prev_usage = usage;
        remaining -= 1;
    }

    let exists: i64 = redis::cmd("EXISTS").arg("gzpop").query(&mut con)?;
    assert_eq!(exists, 0);

    let final_usage = redis::cmd("MEMORY")
        .arg("USAGE")
        .arg("gzpop")
        .query::<Option<i64>>(&mut con)?
        .unwrap_or(0);
    assert_eq!(final_usage, 0);

    Ok(())
}
