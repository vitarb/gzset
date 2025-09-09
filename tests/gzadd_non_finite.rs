mod helpers;

#[test]
fn gzadd_rejects_non_finite() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;
    for val in ["nan", "inf", "-inf"] {
        let res: redis::RedisResult<()> = redis::cmd("GZADD")
            .arg("s")
            .arg(val)
            .arg("m")
            .query(&mut con);
        assert!(res.is_err());
        let err = res.unwrap_err();
        if val == "nan" {
            assert!(err.to_string().contains("parse as float"));
        } else {
            assert!(err.to_string().contains("score is not a finite number"));
        }
    }
    Ok(())
}
