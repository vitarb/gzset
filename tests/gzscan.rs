mod helpers;

use gzset::{fmt_f64, with_fmt_buf};

#[test]
fn gzscan_iterates_with_various_counts() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;

    let mut pipe = redis::pipe();
    for i in 0..30 {
        pipe.cmd("GZADD").arg("s").arg(i).arg(format!("m{i}"));
    }
    pipe.query::<()>(&mut con)?;

    let expected: Vec<String> = (0..30).map(|i| format!("m{i}")).collect();
    let scenarios = [None, Some(1usize), Some(4), Some(9), Some(30), Some(128)];
    for count in scenarios {
        let mut cursor = "0".to_string();
        let mut seen = Vec::new();
        loop {
            let mut cmd = redis::cmd("GZSCAN");
            cmd.arg("s").arg(&cursor);
            if let Some(c) = count {
                cmd.arg("COUNT").arg(c);
            }
            let (next, arr): (String, Vec<String>) = cmd.query(&mut con)?;
            assert_eq!(arr.len() % 2, 0);
            if let Some(c) = count {
                assert!(arr.len() <= c * 2);
            }
            for chunk in arr.chunks(2) {
                let member = &chunk[0];
                let score = &chunk[1];
                let idx: usize = member[1..].parse().unwrap();
                assert_eq!(score.parse::<usize>().unwrap(), idx);
                seen.push(member.clone());
            }
            cursor = next;
            if cursor == "0" {
                break;
            }
        }
        assert_eq!(seen, expected);
    }

    Ok(())
}

#[test]
fn gzscan_cursor_encoding_special_members() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;

    let members = [
        (0.5, "pipe|value"),
        (1.25, "percent%value"),
        (2.75, "encoded%7C"),
        (3.5, "mix%7C|é"),
        (4.125, "雪%25|☃"),
    ];

    let mut pipe = redis::pipe();
    for (score, member) in members {
        pipe.cmd("GZADD").arg("s").arg(score).arg(member);
    }
    pipe.query::<()>(&mut con)?;

    let mut cursor = "0".to_string();
    let mut seen_members = Vec::new();
    let mut seen_scores = Vec::new();
    loop {
        let (next, arr): (String, Vec<String>) = redis::cmd("GZSCAN")
            .arg("s")
            .arg(&cursor)
            .arg("COUNT")
            .arg(1)
            .query(&mut con)?;
        assert_eq!(arr.len(), 2);
        for chunk in arr.chunks(2) {
            seen_members.push(chunk[0].clone());
            seen_scores.push(chunk[1].clone());
        }
        if next != "0" {
            let (score_part, _) = next.split_once('|').unwrap();
            let parsed: f64 = score_part.parse().unwrap();
            let canonical = with_fmt_buf(|b| fmt_f64(b, parsed).to_owned());
            assert_eq!(score_part, canonical);
        }
        cursor = next;
        if cursor == "0" {
            break;
        }
    }

    let expected_members: Vec<String> = members.iter().map(|(_, m)| m.to_string()).collect();
    assert_eq!(seen_members, expected_members);

    let expected_scores: Vec<String> = members
        .iter()
        .map(|(s, _)| with_fmt_buf(|b| fmt_f64(b, *s).to_owned()))
        .collect();
    assert_eq!(seen_scores, expected_scores);

    Ok(())
}

#[test]
fn gzscan_mutation_between_calls() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;

    let mut pipe = redis::pipe();
    for i in 0..15 {
        pipe.cmd("GZADD").arg("s").arg(i).arg(format!("m{i}"));
    }
    pipe.query::<()>(&mut con)?;

    let (cur1, arr1): (String, Vec<String>) =
        redis::cmd("GZSCAN").arg("s").arg("0").query(&mut con)?;
    assert_ne!(cur1, "0");

    // mutate between scans
    redis::cmd("GZREM").arg("s").arg("m10").execute(&mut con);
    redis::cmd("GZADD")
        .arg("s")
        .arg(20)
        .arg("m20")
        .execute(&mut con);

    let (cur2, arr2): (String, Vec<String>) =
        redis::cmd("GZSCAN").arg("s").arg(&cur1).query(&mut con)?;
    assert_eq!(cur2, "0");

    let mut members = Vec::new();
    for chunk in arr1.chunks(2).chain(arr2.chunks(2)) {
        members.push(chunk[0].clone());
    }
    let expected: Vec<String> = redis::cmd("GZRANGE")
        .arg("s")
        .arg(0)
        .arg(-1)
        .query(&mut con)?;
    // After mutation, the union of scanned items should match the final set.
    // As with Redis SCAN, duplicates or skips are not guaranteed across changes.
    assert_eq!(members, expected);
    Ok(())
}

#[test]
fn gzscan_rejects_invalid_cursor() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;

    redis::cmd("GZADD")
        .arg("s")
        .arg(0)
        .arg("member")
        .execute(&mut con);

    let cursors = ["1|member%", "1|member%zz", "inf|member"];
    for cur in cursors {
        let err = redis::cmd("GZSCAN")
            .arg("s")
            .arg(cur)
            .query::<(String, Vec<String>)>(&mut con)
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.to_ascii_lowercase().contains("invalid cursor"),
            "unexpected error: {msg}"
        );
    }

    Ok(())
}

#[test]
fn gzscan_rejects_invalid_count() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;

    redis::cmd("GZADD")
        .arg("s")
        .arg(0)
        .arg("member")
        .execute(&mut con);

    let invalid_counts: [i64; 3] = [0, -1, 2048];
    for &count in &invalid_counts {
        let err = redis::cmd("GZSCAN")
            .arg("s")
            .arg("0")
            .arg("COUNT")
            .arg(count)
            .query::<(String, Vec<String>)>(&mut con)
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.to_ascii_lowercase().contains("count"),
            "unexpected error: {msg}"
        );
    }

    let err = redis::cmd("GZSCAN")
        .arg("s")
        .arg("0")
        .arg("COUNT")
        .arg(5)
        .arg("COUNT")
        .arg(5)
        .query::<(String, Vec<String>)>(&mut con)
        .unwrap_err();
    let msg = err.to_string();
    assert!(
        msg.to_ascii_lowercase().contains("syntax"),
        "unexpected error: {msg}"
    );

    Ok(())
}
