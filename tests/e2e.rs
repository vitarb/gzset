mod helpers;

#[test]
#[ignore]
fn gzadd_gzrank() {
    let vk = helpers::ValkeyInstance::start();
    let client = redis::Client::open(vk.url()).expect("client");
    let mut con = client.get_connection().expect("conn");
    let add: i32 = redis::cmd("GZADD")
        .arg("k")
        .arg("1.0")
        .arg("a")
        .query(&mut con)
        .expect("gzadd");
    assert_eq!(add, 1);
    let rank: i32 = redis::cmd("GZRANK")
        .arg("k")
        .arg("a")
        .query(&mut con)
        .expect("gzrank");
    assert_eq!(rank, 0);
    drop(vk);
}

#[test]
#[ignore]
fn gzrange_rem_score() {
    let vk = helpers::ValkeyInstance::start();
    let client = redis::Client::open(vk.url()).expect("client");
    let mut con = client.get_connection().expect("conn");

    redis::cmd("GZADD")
        .arg("k")
        .arg("1")
        .arg("a")
        .query::<i32>(&mut con)
        .unwrap();
    redis::cmd("GZADD")
        .arg("k")
        .arg("2")
        .arg("b")
        .query::<i32>(&mut con)
        .unwrap();
    let range: Vec<String> = redis::cmd("GZRANGE")
        .arg("k")
        .arg("0")
        .arg("-1")
        .query(&mut con)
        .unwrap();
    assert_eq!(range, vec!["a", "b"]);

    let score: f64 = redis::cmd("GZSCORE")
        .arg("k")
        .arg("b")
        .query(&mut con)
        .unwrap();
    assert_eq!(score, 2.0);

    let removed: i32 = redis::cmd("GZREM")
        .arg("k")
        .arg("a")
        .query(&mut con)
        .unwrap();
    assert_eq!(removed, 1);

    let range2: Vec<String> = redis::cmd("GZRANGE")
        .arg("k")
        .arg("0")
        .arg("-1")
        .query(&mut con)
        .unwrap();
    assert_eq!(range2, vec!["b"]);

    drop(vk);
}

#[test]
#[ignore]
fn edge_cases() {
    let vk = helpers::ValkeyInstance::start();
    let client = redis::Client::open(vk.url()).expect("client");
    let mut con = client.get_connection().expect("conn");

    // large positive float
    redis::cmd("GZADD")
        .arg("k")
        .arg("1e308")
        .arg("big")
        .query::<i32>(&mut con)
        .unwrap();
    let score: f64 = redis::cmd("GZSCORE")
        .arg("k")
        .arg("big")
        .query(&mut con)
        .unwrap();
    assert_eq!(score, 1e308);

    // negative float with unicode member
    let member = "neg-😄";
    redis::cmd("GZADD")
        .arg("k")
        .arg("-5.5")
        .arg(member)
        .query::<i32>(&mut con)
        .unwrap();
    let score2: f64 = redis::cmd("GZSCORE")
        .arg("k")
        .arg(member)
        .query(&mut con)
        .unwrap();
    assert_eq!(score2, -5.5);

    let range: Vec<String> = redis::cmd("GZRANGE")
        .arg("k")
        .arg("0")
        .arg("-1")
        .query(&mut con)
        .unwrap();
    assert!(range.contains(&String::from(member)));

    let err = redis::cmd("GZADD")
        .arg("k")
        .arg("5")
        .query::<i32>(&mut con)
        .unwrap_err();
    assert!(err.to_string().contains("wrong number"));

    drop(vk);
}
