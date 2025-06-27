mod helpers;

use redis::Commands;

#[test]
#[ignore]
fn gzadd_gzrank() {
    let vk = helpers::ValkeyInstance::start();
    let client = redis::Client::open(vk.url()).expect("client");
    let mut con = client.get_connection().expect("conn");
    let add: i32 = redis::cmd("GZADD").arg("k").arg("1.0").arg("a").query(&mut con).expect("gzadd");
    assert_eq!(add, 1);
    let rank: i32 = redis::cmd("GZRANK").arg("k").arg("a").query(&mut con).expect("gzrank");
    assert_eq!(rank, 0);
    drop(vk);
}
