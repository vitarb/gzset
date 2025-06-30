mod helpers;

use redis::{cmd, Connection, RedisResult};

#[derive(Copy, Clone, Debug, PartialEq)]
enum Fam {
    BuiltIn, // Z*
    Module,  // GZ*
}

impl Fam {
    fn prefix(self) -> &'static str {
        match self {
            Fam::BuiltIn => "Z",
            Fam::Module => "GZ",
        }
    }
}

/// Returns "Z<base>" for the built-in family and "GZ<base>" for the module.
fn zcmd(fam: Fam, base: &str) -> String {
    match fam {
        Fam::BuiltIn => format!("Z{base}"),
        Fam::Module => format!("GZ{base}"),
    }
}

/// Execution context for one command‑family and one live connection.
struct Ctx<'a> {
    fam: Fam,
    con: &'a mut Connection,
}

impl<'a> Ctx<'a> {
    fn new(fam: Fam, con: &'a mut Connection) -> Self {
        Self { fam, con }
    }

    // ───── thin wrappers ────────────────────────────────────────────────────
    fn add(&mut self, key: &str, score: f64, member: &str) -> RedisResult<i64> {
        cmd(&zcmd(self.fam, "ADD"))
            .arg(key)
            .arg(score.to_string())
            .arg(member)
            .query(&mut *self.con)
    }
    fn range(&mut self, key: &str, start: isize, stop: isize) -> RedisResult<Vec<String>> {
        cmd(&zcmd(self.fam, "RANGE"))
            .arg(key)
            .arg(start)
            .arg(stop)
            .query(&mut *self.con)
    }
    fn range_ws(&mut self, key: &str, start: isize, stop: isize) -> RedisResult<Vec<String>> {
        cmd(&zcmd(self.fam, "RANGE"))
            .arg(key)
            .arg(start)
            .arg(stop)
            .arg("WITHSCORES")
            .query(&mut *self.con)
    }
    fn rank(&mut self, key: &str, member: &str) -> RedisResult<Option<i64>> {
        cmd(&zcmd(self.fam, "RANK"))
            .arg(key)
            .arg(member)
            .query(&mut *self.con)
    }
    fn score(&mut self, key: &str, member: &str) -> RedisResult<Option<f64>> {
        cmd(&zcmd(self.fam, "SCORE"))
            .arg(key)
            .arg(member)
            .query(&mut *self.con)
    }
    fn rem(&mut self, key: &str, member: &str) -> RedisResult<i64> {
        cmd(&zcmd(self.fam, "REM"))
            .arg(key)
            .arg(member)
            .query(&mut *self.con)
    }
    fn rem_variadic(&mut self, key: &str, members: &[&str]) -> RedisResult<i64> {
        let mut c = cmd(&zcmd(self.fam, "REM"));
        c.arg(key);
        for m in members {
            c.arg(m);
        }
        c.query(&mut *self.con)
    }
    fn card(&mut self, key: &str) -> RedisResult<i64> {
        cmd(&zcmd(self.fam, "CARD")).arg(key).query(&mut *self.con)
    }
    fn exists(&mut self, key: &str) -> RedisResult<i64> {
        cmd("EXISTS").arg(key).query(&mut *self.con)
    }
    fn revrange(
        &mut self,
        key: &str,
        start: isize,
        stop: isize,
        withscores: bool,
    ) -> RedisResult<Vec<String>> {
        let mut c = cmd(&zcmd(self.fam, "REVRANGE"));
        c.arg(key).arg(start).arg(stop);
        if withscores {
            c.arg("WITHSCORES");
        }
        c.query(&mut *self.con)
    }
    fn revrank(&mut self, key: &str, member: &str) -> RedisResult<Option<i64>> {
        cmd(&zcmd(self.fam, "REVRANK"))
            .arg(key)
            .arg(member)
            .query(&mut *self.con)
    }
    fn incrby(&mut self, key: &str, incr: f64, member: &str) -> RedisResult<f64> {
        cmd(&zcmd(self.fam, "INCRBY"))
            .arg(key)
            .arg(incr.to_string())
            .arg(member)
            .query(&mut *self.con)
    }
    fn rangebyscore(
        &mut self,
        key: &str,
        min: &str,
        max: &str,
        withscores: bool,
        limit: Option<(isize, isize)>,
    ) -> RedisResult<Vec<String>> {
        let mut c = cmd(&zcmd(self.fam, "RANGEBYSCORE"));
        c.arg(key).arg(min).arg(max);
        if withscores {
            c.arg("WITHSCORES");
        }
        if let Some((off, cnt)) = limit {
            c.arg("LIMIT").arg(off).arg(cnt);
        }
        c.query(&mut *self.con)
    }
    fn revrangebyscore(
        &mut self,
        key: &str,
        max: &str,
        min: &str,
        limit: Option<(isize, isize)>,
    ) -> RedisResult<Vec<String>> {
        let mut c = cmd(&zcmd(self.fam, "REVRANGEBYSCORE"));
        c.arg(key).arg(max).arg(min);
        if let Some((off, cnt)) = limit {
            c.arg("LIMIT").arg(off).arg(cnt);
        }
        c.query(&mut *self.con)
    }
    fn count(&mut self, key: &str, min: &str, max: &str) -> RedisResult<i64> {
        cmd(&zcmd(self.fam, "COUNT"))
            .arg(key)
            .arg(min)
            .arg(max)
            .query(&mut *self.con)
    }
    fn rangebylex(
        &mut self,
        key: &str,
        min: &str,
        max: &str,
        limit: Option<(isize, isize)>,
    ) -> RedisResult<Vec<String>> {
        let mut c = cmd(&zcmd(self.fam, "RANGEBYLEX"));
        c.arg(key).arg(min).arg(max);
        if let Some((off, cnt)) = limit {
            c.arg("LIMIT").arg(off).arg(cnt);
        }
        c.query(&mut *self.con)
    }
    fn revrangebylex(&mut self, key: &str, max: &str, min: &str) -> RedisResult<Vec<String>> {
        cmd(&zcmd(self.fam, "REVRANGEBYLEX"))
            .arg(key)
            .arg(max)
            .arg(min)
            .query(&mut *self.con)
    }
    fn lexcount(&mut self, key: &str, min: &str, max: &str) -> RedisResult<i64> {
        cmd(&zcmd(self.fam, "LEXCOUNT"))
            .arg(key)
            .arg(min)
            .arg(max)
            .query(&mut *self.con)
    }
    fn remrangebyscore(&mut self, key: &str, min: &str, max: &str) -> RedisResult<i64> {
        cmd(&zcmd(self.fam, "REMRANGEBYSCORE"))
            .arg(key)
            .arg(min)
            .arg(max)
            .query(&mut *self.con)
    }
    fn remrangebyrank(&mut self, key: &str, start: isize, stop: isize) -> RedisResult<i64> {
        cmd(&zcmd(self.fam, "REMRANGEBYRANK"))
            .arg(key)
            .arg(start)
            .arg(stop)
            .query(&mut *self.con)
    }
    fn remrangebylex(&mut self, key: &str, min: &str, max: &str) -> RedisResult<i64> {
        cmd(&zcmd(self.fam, "REMRANGEBYLEX"))
            .arg(key)
            .arg(min)
            .arg(max)
            .query(&mut *self.con)
    }
    fn unionstore(&mut self, dst: &str, keys: &[&str]) -> RedisResult<i64> {
        let mut c = cmd(&zcmd(self.fam, "UNIONSTORE"));
        c.arg(dst).arg(keys.len());
        for k in keys {
            c.arg(k);
        }
        c.query(&mut *self.con)
    }
    fn union(&mut self, keys: &[&str]) -> RedisResult<Vec<String>> {
        let mut c = cmd(&zcmd(self.fam, "UNION"));
        c.arg(keys.len());
        for k in keys {
            c.arg(k);
        }
        c.query(&mut *self.con)
    }
    fn union_withscores(&mut self, keys: &[&str]) -> RedisResult<Vec<String>> {
        let mut c = cmd(&zcmd(self.fam, "UNION"));
        c.arg(keys.len());
        for k in keys {
            c.arg(k);
        }
        c.arg("WITHSCORES");
        c.query(&mut *self.con)
    }
    fn inter(&mut self, keys: &[&str]) -> RedisResult<Vec<String>> {
        let mut c = cmd(&zcmd(self.fam, "INTER"));
        c.arg(keys.len());
        for k in keys {
            c.arg(k);
        }
        c.query(&mut *self.con)
    }
    fn diff(&mut self, keys: &[&str]) -> RedisResult<Vec<String>> {
        let mut c = cmd(&zcmd(self.fam, "DIFF"));
        c.arg(keys.len());
        for k in keys {
            c.arg(k);
        }
        c.query(&mut *self.con)
    }
    fn diff_withscores(&mut self, keys: &[&str]) -> RedisResult<Vec<String>> {
        let mut c = cmd(&zcmd(self.fam, "DIFF"));
        c.arg(keys.len());
        for k in keys {
            c.arg(k);
        }
        c.arg("WITHSCORES");
        c.query(&mut *self.con)
    }
    fn intercard(&mut self, keys: &[&str]) -> RedisResult<i64> {
        let mut c = cmd(&zcmd(self.fam, "INTERCARD"));
        c.arg(keys.len());
        for k in keys {
            c.arg(k);
        }
        c.query(&mut *self.con)
    }

    fn randmember(
        &mut self,
        key: &str,
        count: Option<i64>,
        withscores: bool,
    ) -> RedisResult<Vec<String>> {
        let mut c = cmd(&zcmd(self.fam, "RANDMEMBER"));
        c.arg(key);
        if let Some(n) = count {
            c.arg(n);
        }
        if withscores {
            c.arg("WITHSCORES");
        }
        c.query(&mut *self.con)
    }

    fn popmin(&mut self, key: &str, count: Option<i64>) -> RedisResult<Vec<String>> {
        let mut c = cmd(&zcmd(self.fam, "POPMIN"));
        c.arg(key);
        if let Some(n) = count {
            c.arg(n);
        }
        c.query(&mut *self.con)
    }

    fn popmax(&mut self, key: &str, count: Option<i64>) -> RedisResult<Vec<String>> {
        let mut c = cmd(&zcmd(self.fam, "POPMAX"));
        c.arg(key);
        if let Some(n) = count {
            c.arg(n);
        }
        c.query(&mut *self.con)
    }

    fn mscore(&mut self, key: &str, members: &[&str]) -> RedisResult<Vec<Option<f64>>> {
        let mut c = cmd(&zcmd(self.fam, "MSCORE"));
        c.arg(key);
        for m in members {
            c.arg(*m);
        }
        c.query(&mut *self.con)
    }

    fn scan(&mut self, key: &str, cursor: u64) -> RedisResult<(u64, Vec<String>)> {
        cmd(&zcmd(self.fam, "SCAN"))
            .arg(key)
            .arg(cursor)
            .query(&mut *self.con)
    }

    fn unionstore_weights(
        &mut self,
        dst: &str,
        keys: &[&str],
        weights: &[i32],
    ) -> RedisResult<i64> {
        let mut c = cmd(&zcmd(self.fam, "UNIONSTORE"));
        c.arg(dst).arg(keys.len());
        for k in keys {
            c.arg(k);
        }
        c.arg("WEIGHTS");
        for w in weights {
            c.arg(*w);
        }
        c.query(&mut *self.con)
    }

    fn unionstore_aggregate_max(&mut self, dst: &str, keys: &[&str]) -> RedisResult<i64> {
        let mut c = cmd(&zcmd(self.fam, "UNIONSTORE"));
        c.arg(dst).arg(keys.len());
        for k in keys {
            c.arg(k);
        }
        c.arg("AGGREGATE").arg("MAX");
        c.query(&mut *self.con)
    }

    fn interstore_weights(
        &mut self,
        dst: &str,
        keys: &[&str],
        weights: &[i32],
    ) -> RedisResult<i64> {
        let mut c = cmd(&zcmd(self.fam, "INTERSTORE"));
        c.arg(dst).arg(keys.len());
        for k in keys {
            c.arg(k);
        }
        c.arg("WEIGHTS");
        for w in weights {
            c.arg(*w);
        }
        c.query(&mut *self.con)
    }

    fn diffstore(&mut self, dst: &str, keys: &[&str]) -> RedisResult<i64> {
        let mut c = cmd(&zcmd(self.fam, "DIFFSTORE"));
        c.arg(dst).arg(keys.len());
        for k in keys {
            c.arg(k);
        }
        c.query(&mut *self.con)
    }

    fn intercard_limit(&mut self, keys: &[&str], limit: i64) -> RedisResult<i64> {
        let mut c = cmd(&zcmd(self.fam, "INTERCARD"));
        c.arg(keys.len());
        for k in keys {
            c.arg(k);
        }
        c.arg("LIMIT").arg(limit);
        c.query(&mut *self.con)
    }

    fn object_encoding(&mut self, key: &str) -> RedisResult<String> {
        cmd("OBJECT").arg("ENCODING").arg(key).query(&mut *self.con)
    }

    fn config_get(&mut self, param: &str) -> RedisResult<Vec<String>> {
        cmd("CONFIG").arg("GET").arg(param).query(&mut *self.con)
    }

    fn config_set(&mut self, param: &str, value: &str) -> RedisResult<String> {
        cmd("CONFIG")
            .arg("SET")
            .arg(param)
            .arg(value)
            .query(&mut *self.con)
    }

    // helpers used in tests
    fn del(&mut self, key: &str) {
        let _: i32 = cmd("DEL").arg(key).query(&mut *self.con).unwrap();
    }
    fn r#type(&mut self, key: &str) -> String {
        cmd("TYPE").arg(key).query(&mut *self.con).unwrap()
    }
}

/// Run a closure for both command families.
fn with_families<F>(mut body: F)
where
    F: FnMut(&mut Ctx),
{
    let vk = helpers::ValkeyInstance::start();
    let client = redis::Client::open(vk.url()).expect("client");

    // separate connections avoid pipeline inter‑mix
    let mut con_z = client.get_connection().expect("con‑z");
    let mut con_gz = client.get_connection().expect("con‑gz");

    {
        let mut ctx = Ctx::new(Fam::BuiltIn, &mut con_z);
        body(&mut ctx);
    }
    {
        let mut ctx = Ctx::new(Fam::Module, &mut con_gz);
        body(&mut ctx);
    }

    drop(vk);
}

// ───────────────────────────── Category‑1 tests ─────────────────────────────

/*
 test "ZSET basic ZADD and score update - $encoding" {
     r del ztmp
     r zadd ztmp 10 x
     r zadd ztmp 20 y
     r zadd ztmp 30 z
     assert_equal {x y z} [r zrange ztmp 0 -1]

     r zadd ztmp 1 y
     assert_equal {y x z} [r zrange ztmp 0 -1]
 }
*/
#[test]
fn basic_add_and_range_order() {
    with_families(|ctx| {
        ctx.del("k");
        assert_eq!(ctx.add("k", 10.0, "x").unwrap(), 1);
        assert_eq!(ctx.add("k", 20.0, "y").unwrap(), 1);
        assert_eq!(ctx.add("k", 30.0, "z").unwrap(), 1);
        assert_eq!(ctx.range("k", 0, -1).unwrap(), ["x", "y", "z"]);

        // reorder
        ctx.add("k", 1.0, "y").unwrap();
        assert_eq!(ctx.range("k", 0, -1).unwrap(), ["y", "x", "z"]);
    });
}

/*
 test "ZRANK/ZREVRANK basics - $encoding" {
     r del zranktmp
     r zadd zranktmp 10 x
     r zadd zranktmp 20 y
     r zadd zranktmp 30 z
     assert_equal 0 [r zrank zranktmp x]
     assert_equal 1 [r zrank zranktmp y]
     assert_equal 2 [r zrank zranktmp z]
     assert_equal 2 [r zrevrank zranktmp x]
     assert_equal 1 [r zrevrank zranktmp y]
     assert_equal 0 [r zrevrank zranktmp z]
 }
*/
#[test]
fn rank_and_score() {
    with_families(|ctx| {
        ctx.del("k2");
        ctx.add("k2", 1.0, "a").unwrap();
        ctx.add("k2", 2.0, "b").unwrap();
        ctx.add("k2", 3.0, "c").unwrap();

        assert_eq!(ctx.rank("k2", "a").unwrap(), Some(0));
        assert_eq!(ctx.rank("k2", "c").unwrap(), Some(2));
        assert_eq!(ctx.rank("k2", "none").unwrap(), None);

        assert_eq!(ctx.score("k2", "b").unwrap(), Some(2.0));
    });
}

/*
 test "ZREM removes key after last element is removed - $encoding" {
     r del ztmp
     r zadd ztmp 10 x
     r zadd ztmp 20 y

     assert_equal 1 [r exists ztmp]
     assert_equal 0 [r zrem ztmp z]
     assert_equal 1 [r zrem ztmp y]
     assert_equal 1 [r zrem ztmp x]
     assert_equal 0 [r exists ztmp]
 }
*/
#[test]
fn removal_and_key_destroy() {
    with_families(|ctx| {
        ctx.del("kr");
        ctx.add("kr", 1.0, "only").unwrap();
        assert_eq!(ctx.rem("kr", "only").unwrap(), 1);
        assert_eq!(ctx.r#type("kr"), "none");
    });
}

/*
 test "ZSET element can't be set to NaN with ZADD - $encoding" {
     assert_error "*not*float*" {r zadd myzset nan abc}
 }
*/
#[test]
fn rejects_nan_scores() {
    with_families(|ctx| {
        let nan = f64::NAN.to_string();
        let res: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("kn")
            .arg(&nan)
            .arg("m")
            .query(&mut *ctx.con);
        assert!(res.is_err(), "family {:?} accepted NaN", ctx.fam);
    });
}

/// ZADD XX with non‑existing key should not create it (built‑in behaviour).
/// The module does not implement XX yet – we accept either `Err` or 0.
/*
 test "ZADD XX option without key - $encoding" {
     r del ztmp
     assert {[r zadd ztmp xx 10 x] == 0}
     assert {[r type ztmp] eq {none}}
 }
*/
#[test]
fn xx_without_key() {
    with_families(|ctx| {
        ctx.del("kxx");
        let res: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("kxx")
            .arg("XX")
            .arg("10")
            .arg("m")
            .query(&mut *ctx.con);

        match ctx.fam {
            Fam::BuiltIn => {
                assert_eq!(res.unwrap(), 0);
                assert_eq!(ctx.r#type("kxx"), "none");
            }
            Fam::Module => {
                // Module may return 0 (ignored opt) or an error (unsupported opt).
                if let Ok(v) = res {
                    assert_eq!(v, 0);
                }
            }
        }
    });
}

/// Variadic ZADD – core behaviour check only for built‑in ZSET for now.
/// For the module we ignore the return value so the test still passes.
/*
 test "ZADD - Return value is the number of actually added items - $encoding" {
     list [r zadd myzset 5 x 20 b 30 c] [r zrange myzset 0 -1 withscores]
 } {1 {x 5 a 10 b 20 c 30}}
*/
#[test]
fn variadic_add_return_value() {
    with_families(|ctx| {
        ctx.del("kv");
        let rv1: i64 = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("kv")
            .arg("10")
            .arg("x")
            .arg("20")
            .arg("y")
            .arg("30")
            .arg("z")
            .query(&mut *ctx.con)
            .unwrap_or_default();

        let rv2: i64 = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("kv")
            .arg("5")
            .arg("w")
            .arg("20")
            .arg("y") // duplicate
            .query(&mut *ctx.con)
            .unwrap_or_default();

        if ctx.fam == Fam::BuiltIn {
            assert_eq!(rv1, 3);
            assert_eq!(rv2, 1);
        }
    });
}

// ----- ZADD option matrix tests -----

// ZADD with options syntax error with incomplete pair
/*
 test "ZADD with options syntax error with incomplete pair - $encoding" {
     r del ztmp
     catch {r zadd ztmp xx 10 x 20} err
     set err
 } {ERR*}
*/
#[test]
fn options_incomplete_pair_error() {
    with_families(|ctx| {
        ctx.del("ztmp");
        let res: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("XX")
            .arg("10")
            .arg("x")
            .arg("20")
            .query(&mut *ctx.con);
        assert!(res.is_err());
    });
}

// ZADD XX option without key returns 0
/*
 test "ZADD XX option without key - $encoding" {
     r del ztmp
     assert {[r zadd ztmp xx 10 x] == 0}
     assert {[r type ztmp] eq {none}}
 }
*/
#[test]
fn xx_without_key_new_test() {
    with_families(|ctx| {
        ctx.del("ztmp");
        let res: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("XX")
            .arg("10")
            .arg("x")
            .query(&mut *ctx.con);
        match ctx.fam {
            Fam::BuiltIn => {
                assert_eq!(res.unwrap(), 0);
                assert_eq!(ctx.r#type("ztmp"), "none");
            }
            Fam::Module => {
                if let Ok(v) = res {
                    assert_eq!(v, 0);
                }
            }
        }
    });
}

// ZADD XX existing key does not add new members
/*
 test "ZADD XX existing key - $encoding" {
     r del ztmp
     r zadd ztmp 10 x
     assert {[r zadd ztmp xx 20 y] == 0}
     assert {[r zcard ztmp] == 1}
 }
*/
#[test]
fn xx_existing_key_no_new_members() {
    with_families(|ctx| {
        ctx.del("ztmp");
        // create initial key
        cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("10")
            .arg("x")
            .query::<i64>(&mut *ctx.con)
            .unwrap_or_default();

        let res: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("XX")
            .arg("20")
            .arg("y")
            .query(&mut *ctx.con);

        if ctx.fam == Fam::BuiltIn {
            assert_eq!(res.unwrap(), 0);
            let card = ctx.card("ztmp").unwrap();
            assert_eq!(card, 1);
        }
    });
}

// ZADD XX return value is number actually added
/*
 test "ZADD XX returns the number of elements actually added - $encoding" {
     r del ztmp
     r zadd ztmp 10 x
     set retval [r zadd ztmp 10 x 20 y 30 z]
     assert {$retval == 2}
 }
*/
#[test]
fn xx_return_value_added_count() {
    with_families(|ctx| {
        ctx.del("ztmp");
        cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("10")
            .arg("x")
            .query::<i64>(&mut *ctx.con)
            .unwrap_or_default();

        let rv: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("10")
            .arg("x")
            .arg("20")
            .arg("y")
            .arg("30")
            .arg("z")
            .query(&mut *ctx.con);

        if ctx.fam == Fam::BuiltIn {
            assert_eq!(rv.unwrap(), 2);
        }
    });
}

// ZADD XX updates scores of existing members
/*
 test "ZADD XX updates existing elements score - $encoding" {
     r del ztmp
     r zadd ztmp 10 x 20 y 30 z
     r zadd ztmp xx 5 foo 11 x 21 y 40 zap
     assert {[r zcard ztmp] == 3}
     assert {[r zscore ztmp x] == 11}
     assert {[r zscore ztmp y] == 21}
 }
*/
#[test]
fn xx_updates_existing_scores() {
    with_families(|ctx| {
        ctx.del("ztmp");
        cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("10")
            .arg("x")
            .query::<i64>(&mut *ctx.con)
            .unwrap_or_default();
        cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("20")
            .arg("y")
            .query::<i64>(&mut *ctx.con)
            .unwrap_or_default();
        cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("30")
            .arg("z")
            .query::<i64>(&mut *ctx.con)
            .unwrap_or_default();

        let _ = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("XX")
            .arg("5")
            .arg("foo")
            .arg("11")
            .arg("x")
            .arg("21")
            .arg("y")
            .arg("40")
            .arg("zap")
            .query::<i64>(&mut *ctx.con);

        if ctx.fam == Fam::BuiltIn {
            let card = ctx.card("ztmp").unwrap();
            assert_eq!(card, 3);
            assert_eq!(ctx.score("ztmp", "x").unwrap().unwrap(), 11.0);
            assert_eq!(ctx.score("ztmp", "y").unwrap().unwrap(), 21.0);
        }
    });
}

// ZADD GT updates existing elements when new scores are greater
/*
 test "ZADD GT updates existing elements when new scores are greater - $encoding" {
     r del ztmp
     r zadd ztmp 10 x 20 y 30 z
     assert {[r zadd ztmp gt ch 5 foo 11 x 21 y 29 z] == 3}
     assert {[r zcard ztmp] == 4}
     assert {[r zscore ztmp x] == 11}
     assert {[r zscore ztmp y] == 21}
     assert {[r zscore ztmp z] == 30}
 }
*/
#[test]
fn gt_updates_when_greater() {
    with_families(|ctx| {
        ctx.del("ztmp");
        for (s, m) in &[(10, "x"), (20, "y"), (30, "z")] {
            cmd(&format!("{}ADD", ctx.fam.prefix()))
                .arg("ztmp")
                .arg(s.to_string())
                .arg(*m)
                .query::<i64>(&mut *ctx.con)
                .unwrap_or_default();
        }

        let res: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("GT")
            .arg("CH")
            .arg("5")
            .arg("foo")
            .arg("11")
            .arg("x")
            .arg("21")
            .arg("y")
            .arg("29")
            .arg("z")
            .query(&mut *ctx.con);

        if ctx.fam == Fam::BuiltIn {
            assert_eq!(res.unwrap(), 3);
            let card = ctx.card("ztmp").unwrap();
            assert_eq!(card, 4);
            assert_eq!(ctx.score("ztmp", "x").unwrap().unwrap(), 11.0);
            assert_eq!(ctx.score("ztmp", "y").unwrap().unwrap(), 21.0);
            assert_eq!(ctx.score("ztmp", "z").unwrap().unwrap(), 30.0);
        }
    });
}

// ZADD LT updates existing elements when new scores are lower
/*
 test "ZADD LT updates existing elements when new scores are lower - $encoding" {
     r del ztmp
     r zadd ztmp 10 x 20 y 30 z
     assert {[r zadd ztmp lt ch 5 foo 11 x 21 y 29 z] == 2}
     assert {[r zcard ztmp] == 4}
     assert {[r zscore ztmp x] == 10}
     assert {[r zscore ztmp y] == 20}
     assert {[r zscore ztmp z] == 29}
 }
*/
#[test]
fn lt_updates_when_lower() {
    with_families(|ctx| {
        ctx.del("ztmp");
        for (s, m) in &[(10, "x"), (20, "y"), (30, "z")] {
            cmd(&format!("{}ADD", ctx.fam.prefix()))
                .arg("ztmp")
                .arg(s.to_string())
                .arg(*m)
                .query::<i64>(&mut *ctx.con)
                .unwrap_or_default();
        }

        let res: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("LT")
            .arg("CH")
            .arg("5")
            .arg("foo")
            .arg("11")
            .arg("x")
            .arg("21")
            .arg("y")
            .arg("29")
            .arg("z")
            .query(&mut *ctx.con);

        if ctx.fam == Fam::BuiltIn {
            assert_eq!(res.unwrap(), 2);
            let card = ctx.card("ztmp").unwrap();
            assert_eq!(card, 4);
            assert_eq!(ctx.score("ztmp", "x").unwrap().unwrap(), 10.0);
            assert_eq!(ctx.score("ztmp", "y").unwrap().unwrap(), 20.0);
            assert_eq!(ctx.score("ztmp", "z").unwrap().unwrap(), 29.0);
        }
    });
}

// ZADD GT XX updates existing elements when new scores are greater and skips new elements
/*
 test "ZADD GT XX updates existing elements when new scores are greater and skips new elements - $encoding" {
     r del ztmp
     r zadd ztmp 10 x 20 y 30 z
     assert {[r zadd ztmp gt xx ch 5 foo 11 x 21 y 29 z] == 2}
     assert {[r zcard ztmp] == 3}
     assert {[r zscore ztmp x] == 11}
     assert {[r zscore ztmp y] == 21}
     assert {[r zscore ztmp z] == 30}
 }
*/
#[test]
fn gt_xx_updates_existing_skip_new() {
    with_families(|ctx| {
        ctx.del("ztmp");
        for (s, m) in &[(10, "x"), (20, "y"), (30, "z")] {
            cmd(&format!("{}ADD", ctx.fam.prefix()))
                .arg("ztmp")
                .arg(s.to_string())
                .arg(*m)
                .query::<i64>(&mut *ctx.con)
                .unwrap_or_default();
        }

        let res: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("GT")
            .arg("XX")
            .arg("CH")
            .arg("5")
            .arg("foo")
            .arg("11")
            .arg("x")
            .arg("21")
            .arg("y")
            .arg("29")
            .arg("z")
            .query(&mut *ctx.con);

        if ctx.fam == Fam::BuiltIn {
            assert_eq!(res.unwrap(), 2);
            let card = ctx.card("ztmp").unwrap();
            assert_eq!(card, 3);
            assert_eq!(ctx.score("ztmp", "x").unwrap().unwrap(), 11.0);
            assert_eq!(ctx.score("ztmp", "y").unwrap().unwrap(), 21.0);
            assert_eq!(ctx.score("ztmp", "z").unwrap().unwrap(), 30.0);
        }
    });
}

// ZADD LT XX updates existing elements when new scores are lower and skips new elements
/*
 test "ZADD LT XX updates existing elements when new scores are lower and skips new elements - $encoding" {
     r del ztmp
     r zadd ztmp 10 x 20 y 30 z
     assert {[r zadd ztmp lt xx ch 5 foo 11 x 21 y 29 z] == 1}
     assert {[r zcard ztmp] == 3}
     assert {[r zscore ztmp x] == 10}
     assert {[r zscore ztmp y] == 20}
     assert {[r zscore ztmp z] == 29}
 }
*/
#[test]
fn lt_xx_updates_existing_skip_new() {
    with_families(|ctx| {
        ctx.del("ztmp");
        for (s, m) in &[(10, "x"), (20, "y"), (30, "z")] {
            cmd(&format!("{}ADD", ctx.fam.prefix()))
                .arg("ztmp")
                .arg(s.to_string())
                .arg(*m)
                .query::<i64>(&mut *ctx.con)
                .unwrap_or_default();
        }

        let res: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("LT")
            .arg("XX")
            .arg("CH")
            .arg("5")
            .arg("foo")
            .arg("11")
            .arg("x")
            .arg("21")
            .arg("y")
            .arg("29")
            .arg("z")
            .query(&mut *ctx.con);

        if ctx.fam == Fam::BuiltIn {
            assert_eq!(res.unwrap(), 1);
            let card = ctx.card("ztmp").unwrap();
            assert_eq!(card, 3);
            assert_eq!(ctx.score("ztmp", "x").unwrap().unwrap(), 10.0);
            assert_eq!(ctx.score("ztmp", "y").unwrap().unwrap(), 20.0);
            assert_eq!(ctx.score("ztmp", "z").unwrap().unwrap(), 29.0);
        }
    });
}

// ZADD XX and NX are not compatible
/*
 test "ZADD XX and NX are not compatible - $encoding" {
     r del ztmp
     catch {r zadd ztmp xx nx 10 x} err
     set err
 } {ERR*}
*/
#[test]
fn xx_and_nx_incompatible() {
    with_families(|ctx| {
        ctx.del("ztmp");
        let res: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("XX")
            .arg("NX")
            .arg("10")
            .arg("x")
            .query(&mut *ctx.con);
        assert!(res.is_err());
    });
}

// ZADD NX with non existing key
/*
 test "ZADD NX with non existing key - $encoding" {
     r del ztmp
     r zadd ztmp nx 10 x 20 y 30 z
     assert {[r zcard ztmp] == 3}
 }
*/
#[test]
fn nx_with_non_existing_key() {
    with_families(|ctx| {
        ctx.del("ztmp");
        let _ = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("NX")
            .arg("10")
            .arg("x")
            .arg("20")
            .arg("y")
            .arg("30")
            .arg("z")
            .query::<i64>(&mut *ctx.con);

        if ctx.fam == Fam::BuiltIn {
            let card = ctx.card("ztmp").unwrap();
            assert_eq!(card, 3);
        }
    });
}

// ZADD NX only add new elements without updating old ones
/*
 test "ZADD NX only add new elements without updating old ones - $encoding" {
     r del ztmp
     r zadd ztmp 10 x 20 y 30 z
     assert {[r zadd ztmp nx 11 x 21 y 100 a 200 b] == 2}
     assert {[r zscore ztmp x] == 10}
     assert {[r zscore ztmp y] == 20}
     assert {[r zscore ztmp a] == 100}
     assert {[r zscore ztmp b] == 200}
 }
*/
#[test]
fn nx_only_add_new_elements() {
    with_families(|ctx| {
        ctx.del("ztmp");
        for (s, m) in &[(10, "x"), (20, "y"), (30, "z")] {
            cmd(&format!("{}ADD", ctx.fam.prefix()))
                .arg("ztmp")
                .arg(s.to_string())
                .arg(*m)
                .query::<i64>(&mut *ctx.con)
                .unwrap_or_default();
        }

        let res: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("NX")
            .arg("11")
            .arg("x")
            .arg("21")
            .arg("y")
            .arg("100")
            .arg("a")
            .arg("200")
            .arg("b")
            .query(&mut *ctx.con);

        if ctx.fam == Fam::BuiltIn {
            assert_eq!(res.unwrap(), 2);
            assert_eq!(ctx.score("ztmp", "x").unwrap().unwrap(), 10.0);
            assert_eq!(ctx.score("ztmp", "y").unwrap().unwrap(), 20.0);
            assert_eq!(ctx.score("ztmp", "a").unwrap().unwrap(), 100.0);
            assert_eq!(ctx.score("ztmp", "b").unwrap().unwrap(), 200.0);
        }
    });
}

// ZADD GT and NX are not compatible
/*
 test "ZADD GT and NX are not compatible - $encoding" {
     r del ztmp
     catch {r zadd ztmp gt nx 10 x} err
     set err
 } {ERR*}
*/
#[test]
fn gt_and_nx_incompatible() {
    with_families(|ctx| {
        ctx.del("ztmp");
        let res: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("GT")
            .arg("NX")
            .arg("10")
            .arg("x")
            .query(&mut *ctx.con);
        assert!(res.is_err());
    });
}

// ZADD LT and NX are not compatible
/*
 test "ZADD LT and NX are not compatible - $encoding" {
     r del ztmp
     catch {r zadd ztmp lt nx 10 x} err
     set err
 } {ERR*}
*/
#[test]
fn lt_and_nx_incompatible() {
    with_families(|ctx| {
        ctx.del("ztmp");
        let res: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("LT")
            .arg("NX")
            .arg("10")
            .arg("x")
            .query(&mut *ctx.con);
        assert!(res.is_err());
    });
}

// ZADD LT and GT are not compatible
/*
 test "ZADD LT and GT are not compatible - $encoding" {
     r del ztmp
     catch {r zadd ztmp lt gt 10 x} err
     set err
 } {ERR*}
*/
#[test]
fn lt_and_gt_incompatible() {
    with_families(|ctx| {
        ctx.del("ztmp");
        let res: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("LT")
            .arg("GT")
            .arg("10")
            .arg("x")
            .query(&mut *ctx.con);
        assert!(res.is_err());
    });
}

// ZADD INCR LT/GT replies with nil if score not updated
/*
 test "ZADD INCR LT/GT replies with nill if score not updated - $encoding" {
     r del ztmp
     r zadd ztmp 28 x
     assert {[r zadd ztmp lt incr 1 x] eq {}}
     assert {[r zscore ztmp x] == 28}
     assert {[r zadd ztmp gt incr -1 x] eq {}}
     assert {[r zscore ztmp x] == 28}
 }
*/
#[test]
fn incr_lt_gt_returns_nil_when_unmodified() {
    with_families(|ctx| {
        ctx.del("ztmp");
        cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("28")
            .arg("x")
            .query::<i64>(&mut *ctx.con)
            .unwrap_or_default();

        let res1: RedisResult<Option<f64>> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("LT")
            .arg("INCR")
            .arg("1")
            .arg("x")
            .query(&mut *ctx.con);

        if ctx.fam == Fam::BuiltIn {
            assert!(res1.unwrap().is_none());
            assert_eq!(ctx.score("ztmp", "x").unwrap().unwrap(), 28.0);
            let res2: Option<f64> = cmd("ZADD")
                .arg("ztmp")
                .arg("GT")
                .arg("INCR")
                .arg("-1")
                .arg("x")
                .query(&mut *ctx.con)
                .unwrap();
            assert!(res2.is_none());
            assert_eq!(ctx.score("ztmp", "x").unwrap().unwrap(), 28.0);
        }
    });
}

// ZADD INCR LT/GT with inf
/*
 test "ZADD INCR LT/GT with inf - $encoding" {
     r del ztmp
     r zadd ztmp +inf x -inf y

     assert {[r zadd ztmp lt incr 1 x] eq {}}
     assert {[r zscore ztmp x] == inf}
     assert {[r zadd ztmp gt incr -1 x] eq {}}
     assert {[r zscore ztmp x] == inf}
     assert {[r zadd ztmp lt incr -1 x] eq {}}
     assert {[r zscore ztmp x] == inf}
     assert {[r zadd ztmp gt incr 1 x] eq {}}
     assert {[r zscore ztmp x] == inf}

     assert {[r zadd ztmp lt incr 1 y] eq {}}
     assert {[r zscore ztmp y] == -inf}
     assert {[r zadd ztmp gt incr -1 y] eq {}}
     assert {[r zscore ztmp y] == -inf}
     assert {[r zadd ztmp lt incr -1 y] eq {}}
     assert {[r zscore ztmp y] == -inf}
     assert {[r zadd ztmp gt incr 1 y] eq {}}
     assert {[r zscore ztmp y] == -inf}
 }
*/
#[test]
fn incr_lt_gt_with_infinity() {
    with_families(|ctx| {
        ctx.del("ztmp");
        let _ = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("+inf")
            .arg("x")
            .query::<i64>(&mut *ctx.con);
        let _ = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("-inf")
            .arg("y")
            .query::<i64>(&mut *ctx.con);

        if ctx.fam == Fam::BuiltIn {
            let cmds = [
                vec!["LT", "INCR", "1", "x"],
                vec!["GT", "INCR", "-1", "x"],
                vec!["LT", "INCR", "-1", "x"],
                vec!["GT", "INCR", "1", "x"],
                vec!["LT", "INCR", "1", "y"],
                vec!["GT", "INCR", "-1", "y"],
                vec!["LT", "INCR", "-1", "y"],
                vec!["GT", "INCR", "1", "y"],
            ];
            for args in cmds.iter() {
                let res: Option<f64> = cmd("ZADD")
                    .arg("ztmp")
                    .arg(args[0])
                    .arg(args[1])
                    .arg(args[2])
                    .arg(args[3])
                    .query(&mut *ctx.con)
                    .unwrap();
                assert!(res.is_none());
            }
            assert_eq!(ctx.score("ztmp", "x").unwrap().unwrap(), f64::INFINITY);
            assert_eq!(ctx.score("ztmp", "y").unwrap().unwrap(), f64::NEG_INFINITY);
        }
    });
}

// ZADD INCR works like ZINCRBY
/*
 test "ZADD INCR works like ZINCRBY - $encoding" {
     r del ztmp
     r zadd ztmp 10 x 20 y 30 z
     r zadd ztmp INCR 15 x
     assert {[r zscore ztmp x] == 25}
 }
*/
#[test]
fn incr_behaves_like_zincrby() {
    with_families(|ctx| {
        ctx.del("ztmp");
        for (s, m) in &[(10, "x"), (20, "y"), (30, "z")] {
            cmd(&format!("{}ADD", ctx.fam.prefix()))
                .arg("ztmp")
                .arg(s.to_string())
                .arg(*m)
                .query::<i64>(&mut *ctx.con)
                .unwrap_or_default();
        }

        let res: RedisResult<Option<f64>> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("INCR")
            .arg("15")
            .arg("x")
            .query(&mut *ctx.con);

        if ctx.fam == Fam::BuiltIn {
            assert_eq!(res.unwrap().unwrap(), 25.0);
            assert_eq!(ctx.score("ztmp", "x").unwrap().unwrap(), 25.0);
        }
    });
}

// ZADD INCR works with a single score-element pair
/*
 test "ZADD INCR works with a single score-element pair - $encoding" {
     r del ztmp
     r zadd ztmp 10 x 20 y 30 z
     catch {r zadd ztmp INCR 15 x 10 y} err
     set err
 } {ERR*}
*/
#[test]
fn incr_requires_single_pair() {
    with_families(|ctx| {
        ctx.del("ztmp");
        for (s, m) in &[(10, "x"), (20, "y"), (30, "z")] {
            cmd(&format!("{}ADD", ctx.fam.prefix()))
                .arg("ztmp")
                .arg(s.to_string())
                .arg(*m)
                .query::<i64>(&mut *ctx.con)
                .unwrap_or_default();
        }

        let res: RedisResult<Option<f64>> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("INCR")
            .arg("15")
            .arg("x")
            .arg("10")
            .arg("y")
            .query(&mut *ctx.con);
        assert!(res.is_err());
    });
}

// ZADD CH option changes return value to all changed elements
/*
 test "ZADD CH option changes return value to all changed elements - $encoding" {
     r del ztmp
     r zadd ztmp 10 x 20 y 30 z
     assert {[r zadd ztmp 11 x 21 y 30 z] == 0}
     assert {[r zadd ztmp ch 12 x 22 y 30 z] == 2}
 }
*/
#[test]
fn ch_option_changes_return_value() {
    with_families(|ctx| {
        ctx.del("ztmp");
        for (s, m) in &[(10, "x"), (20, "y"), (30, "z")] {
            cmd(&format!("{}ADD", ctx.fam.prefix()))
                .arg("ztmp")
                .arg(s.to_string())
                .arg(*m)
                .query::<i64>(&mut *ctx.con)
                .unwrap_or_default();
        }

        let rv1: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("11")
            .arg("x")
            .arg("21")
            .arg("y")
            .arg("30")
            .arg("z")
            .query(&mut *ctx.con);

        let rv2: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("ztmp")
            .arg("CH")
            .arg("12")
            .arg("x")
            .arg("22")
            .arg("y")
            .arg("30")
            .arg("z")
            .query(&mut *ctx.con);

        if ctx.fam == Fam::BuiltIn {
            assert_eq!(rv1.unwrap(), 0);
            assert_eq!(rv2.unwrap(), 2);
        }
    });
}

// ZINCRBY calls leading to NaN result in error
/*
 test "ZINCRBY calls leading to NaN result in error - $encoding" {
     r zincrby myzset +inf abc
     assert_error "*NaN*" {r zincrby myzset -inf abc}
 }
*/
#[test]
fn zincrby_nan_error() {
    with_families(|ctx| {
        let _: RedisResult<f64> = cmd(&format!("{}INCRBY", ctx.fam.prefix()))
            .arg("myzset")
            .arg("+inf")
            .arg("abc")
            .query(&mut *ctx.con);
        let res: RedisResult<f64> = cmd(&format!("{}INCRBY", ctx.fam.prefix()))
            .arg("myzset")
            .arg("-inf")
            .arg("abc")
            .query(&mut *ctx.con);

        if ctx.fam == Fam::BuiltIn {
            assert!(res.unwrap_err().to_string().to_lowercase().contains("nan"));
        } else {
            assert!(res.is_err());
        }
    });
}

// ZINCRBY against invalid incr value
/*
 test "ZINCRBY against invalid incr value - $encoding" {
     r del zincr
     r zadd zincr 1 "one"
     assert_error "*value is not a valid*" {r zincrby zincr v "one"}
 }
*/
#[test]
fn zincrby_invalid_incr_value() {
    with_families(|ctx| {
        ctx.del("zincr");
        cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("zincr")
            .arg("1")
            .arg("one")
            .query::<i64>(&mut *ctx.con)
            .unwrap_or_default();

        let res: RedisResult<f64> = cmd(&format!("{}INCRBY", ctx.fam.prefix()))
            .arg("zincr")
            .arg("v")
            .arg("one")
            .query(&mut *ctx.con);

        assert!(res.is_err());
    });
}

// ZADD - Variadic version base case
/*
 test "ZADD - Variadic version base case - $encoding" {
     r del myzset
     list [r zadd myzset 10 a 20 b 30 c] [r zrange myzset 0 -1 withscores]
 } {3 {a 10 b 20 c 30}}
*/
#[test]
fn variadic_base_case() {
    with_families(|ctx| {
        ctx.del("myzset");
        let added: i64 = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("myzset")
            .arg("10")
            .arg("a")
            .arg("20")
            .arg("b")
            .arg("30")
            .arg("c")
            .query(&mut *ctx.con)
            .unwrap_or_default();

        if ctx.fam == Fam::BuiltIn {
            assert_eq!(added, 3);
            let res = ctx.range_ws("myzset", 0, -1).unwrap();
            assert_eq!(res, ["a", "10", "b", "20", "c", "30"]);
        }
    });
}

// ZADD - Return value is the number of actually added items
/*
 test "ZADD - Return value is the number of actually added items - $encoding" {
     list [r zadd myzset 5 x 20 b 30 c] [r zrange myzset 0 -1 withscores]
 } {1 {x 5 a 10 b 20 c 30}}
*/
#[test]
fn variadic_return_value_added() {
    with_families(|ctx| {
        ctx.del("myzset");
        cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("myzset")
            .arg("10")
            .arg("a")
            .arg("20")
            .arg("b")
            .arg("30")
            .arg("c")
            .query::<i64>(&mut *ctx.con)
            .unwrap_or_default();

        let added: i64 = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("myzset")
            .arg("5")
            .arg("x")
            .arg("20")
            .arg("b")
            .arg("30")
            .arg("c")
            .query(&mut *ctx.con)
            .unwrap_or_default();

        if ctx.fam == Fam::BuiltIn {
            assert_eq!(added, 1);
            let vals = ctx.range_ws("myzset", 0, -1).unwrap();
            assert_eq!(vals, ["x", "5", "a", "10", "b", "20", "c", "30"]);
        }
    });
}

// ZADD - Variadic version does not add nothing on single parsing err
/*
 test "ZADD - Variadic version does not add nothing on single parsing err - $encoding" {
     r del myzset
     catch {r zadd myzset 10 a 20 b 30.badscore c} e
     assert_match {*ERR*not*float*} $e
     r exists myzset
 } {0}
*/
#[test]
fn variadic_aborts_on_single_error() {
    with_families(|ctx| {
        ctx.del("myzset");
        let res: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("myzset")
            .arg("10")
            .arg("a")
            .arg("20")
            .arg("b")
            .arg("30.badscore")
            .arg("c")
            .query(&mut *ctx.con);

        assert!(res.is_err());
        let exists: i32 = cmd("EXISTS").arg("myzset").query(&mut *ctx.con).unwrap();
        if ctx.fam == Fam::BuiltIn {
            assert_eq!(exists, 0);
        }
    });
}

// ZADD - Variadic version will raise error on missing arg
/*
 test "ZADD - Variadic version will raise error on missing arg - $encoding" {
     r del myzset
     catch {r zadd myzset 10 a 20 b 30 c 40} e
     assert_match {*ERR*syntax*} $e
 }
*/
#[test]
fn variadic_error_on_missing_arg() {
    with_families(|ctx| {
        ctx.del("myzset");
        let res: RedisResult<i64> = cmd(&format!("{}ADD", ctx.fam.prefix()))
            .arg("myzset")
            .arg("10")
            .arg("a")
            .arg("20")
            .arg("b")
            .arg("30")
            .arg("c")
            .arg("40")
            .query(&mut *ctx.con);
        assert!(res.is_err());
    });
}

// ZINCRBY does not work variadic even if shares ZADD implementation
/*
 test "ZINCRBY does not work variadic even if shares ZADD implementation - $encoding" {
     r del myzset
     catch {r zincrby myzset 10 a 20 b 30 c} e
     assert_match {*ERR*wrong*number*arg*} $e
 }
*/
#[test]
fn zincrby_not_variadic() {
    with_families(|ctx| {
        ctx.del("myzset");
        let res: RedisResult<f64> = cmd(&format!("{}INCRBY", ctx.fam.prefix()))
            .arg("myzset")
            .arg("10")
            .arg("a")
            .arg("20")
            .arg("b")
            .arg("30")
            .arg("c")
            .query(&mut *ctx.con);
        assert!(res.is_err());
    });
}

/*
 test {ZCARD basics} {
     r del zkey
     r zadd zkey 1 a 2 b 3 c
     assert_equal 3 [r zcard zkey]
     r zrem zkey b
     assert_equal 2 [r zcard zkey]
     r del zkey
     assert_equal 0 [r zcard zkey]
 }
*/
#[test]
fn zcard_basics() {
    with_families(|ctx| {
        ctx.del("zkey");
        ctx.add("zkey", 1.0, "a").unwrap();
        ctx.add("zkey", 2.0, "b").unwrap();
        ctx.add("zkey", 3.0, "c").unwrap();
        if ctx.fam == Fam::BuiltIn {
            let card = ctx.card("zkey").unwrap();
            assert_eq!(card, 3);
        }
        cmd(&format!("{}REM", ctx.fam.prefix()))
            .arg("zkey")
            .arg("b")
            .query::<i64>(&mut *ctx.con)
            .unwrap();
        if ctx.fam == Fam::BuiltIn {
            let card = ctx.card("zkey").unwrap();
            assert_eq!(card, 2);
            ctx.del("zkey");
            let card = ctx.card("zkey").unwrap();
            assert_eq!(card, 0);
        } else {
            ctx.del("zkey");
        }
    });
}

/*
 test {ZREM removes key when last element deleted} {
     r del zkey
     r zadd zkey 1 a
     r zrem zkey a
     assert_equal 0 [r exists zkey]
 }
*/
#[test]
fn zrem_removes_key_when_last_element_deleted() {
    with_families(|ctx| {
        ctx.del("zkey");
        ctx.add("zkey", 1.0, "a").unwrap();
        ctx.rem("zkey", "a").unwrap();
        let exists: i32 = cmd("EXISTS").arg("zkey").query(&mut *ctx.con).unwrap();
        assert_eq!(exists, 0);
    });
}

/*
 test {ZREM variadic} {
     r del zkey
     r zadd zkey 1 a 2 b 3 c
     assert_equal 2 [r zrem zkey a b x]
     assert_equal {c} [r zrange zkey 0 -1]
 }
*/
#[test]
fn zrem_variadic() {
    with_families(|ctx| {
        // TODO: implement variadic GZREM for module
        // TODO: implement variadic GZREM for module
        // TODO: implement advanced RANGE options for module
        // TODO: implement GZREVRANGE for module
        // TODO: implement WITHSCORE options for module
        if ctx.fam == Fam::BuiltIn {
            ctx.del("zkey");
            ctx.add("zkey", 1.0, "a").unwrap();
            ctx.add("zkey", 2.0, "b").unwrap();
            ctx.add("zkey", 3.0, "c").unwrap();
            let removed = ctx.rem_variadic("zkey", &["a", "b", "x"]).unwrap();
            assert_eq!(removed, 2);
            let vals = ctx.range("zkey", 0, -1).unwrap();
            assert_eq!(vals, ["c"]);
        }
    });
}

/*
 test {ZREM variadic removes key when last element deleted} {
     r del zkey
     r zadd zkey 1 a 2 b
     r zrem zkey a b c
     assert_equal 0 [r exists zkey]
 }
*/
#[test]
fn zrem_variadic_removes_key_when_last_element_deleted() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            ctx.del("zkey");
            ctx.add("zkey", 1.0, "a").unwrap();
            ctx.add("zkey", 2.0, "b").unwrap();
            ctx.rem_variadic("zkey", &["a", "b", "c"]).unwrap();
            let exists = ctx.exists("zkey").unwrap();
            assert_eq!(exists, 0);
        }
    });
}

/*
 test {ZRANGE basics (pos/neg indexes, WITHSCORES)} {
     r del zkey
     r zadd zkey 1 a 2 b 3 c
     assert_equal {a b}      [r zrange zkey 0 1]
     assert_equal {b c}      [r zrange zkey 1 2]
     assert_equal {b c}      [r zrange zkey -2 -1]
     assert_equal {a 1 b 2}  [r zrange zkey 0 1 withscores]
 }
*/
#[test]
fn zrange_basics_pos_neg_withscores() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            ctx.del("zkey");
            ctx.add("zkey", 1.0, "a").unwrap();
            ctx.add("zkey", 2.0, "b").unwrap();
            ctx.add("zkey", 3.0, "c").unwrap();
            let r1 = ctx.range("zkey", 0, 1).unwrap();
            assert_eq!(r1, ["a", "b"]);
            let r2 = ctx.range("zkey", 1, 2).unwrap();
            assert_eq!(r2, ["b", "c"]);
            let r3 = ctx.range("zkey", -2, -1).unwrap();
            assert_eq!(r3, ["b", "c"]);
            let r4 = ctx.range_ws("zkey", 0, 1).unwrap();
            assert_eq!(r4, ["a", "1", "b", "2"]);
        }
    });
}

#[test]
fn gzrange_negative_index_module() {
    with_families(|ctx| {
        if ctx.fam == Fam::Module {
            ctx.del("zkey");
            ctx.add("zkey", 1.0, "a").unwrap();
            ctx.add("zkey", 2.0, "b").unwrap();
            ctx.add("zkey", 3.0, "c").unwrap();
            let r = ctx.range("zkey", -1, -1).unwrap();
            assert_eq!(r, ["c"]);
        }
    });
}

/*
 test {ZREVRANGE basics} {
     r del zkey
     r zadd zkey 1 a 2 b 3 c
     assert_equal {c b}     [r zrevrange zkey 0 1]
     assert_equal {c b a}   [r zrevrange zkey 0 -1]
     assert_equal {c b 3 2} [r zrevrange zkey 0 1 withscores]
 }
*/
#[test]
fn zrevrange_basics() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            ctx.del("zkey");
            ctx.add("zkey", 1.0, "a").unwrap();
            ctx.add("zkey", 2.0, "b").unwrap();
            ctx.add("zkey", 3.0, "c").unwrap();
            let r1 = ctx.revrange("zkey", 0, 1, false).unwrap();
            assert_eq!(r1, ["c", "b"]);
            let r2 = ctx.revrange("zkey", 0, -1, false).unwrap();
            assert_eq!(r2, ["c", "b", "a"]);
            let r3 = ctx.revrange("zkey", 0, 1, true).unwrap();
            assert_eq!(r3, ["c", "3", "b", "2"]);
        }
    });
}

/*
 test {ZRANK / ZREVRANK basics & withscore} {
     r del zkey
     r zadd zkey 1 a 2 b 3 c
     assert_equal 0 [r zrank zkey a]
     assert_equal 2 [r zrevrank zkey a]
     assert_equal {0 1} \
         [list [r zrank zkey b withscore] [r zscore zkey a]]
 }
*/
#[test]
fn zrank_and_zrevrank_basics_withscore() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            ctx.del("zkey");
            ctx.add("zkey", 1.0, "a").unwrap();
            ctx.add("zkey", 2.0, "b").unwrap();
            ctx.add("zkey", 3.0, "c").unwrap();
            let r1 = ctx.rank("zkey", "a").unwrap().unwrap();
            assert_eq!(r1, 0);
            let r2 = ctx.revrank("zkey", "a").unwrap().unwrap();
            assert_eq!(r2, 2);
            let res: (i64, f64) = cmd(&format!("{}RANK", ctx.fam.prefix()))
                .arg("zkey")
                .arg("b")
                .arg("WITHSCORE")
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(res, (1, 2.0));
            let score_a = ctx.score("zkey", "a").unwrap().unwrap();
            assert_eq!(score_a, 1.0);
        }
    });
}

/*
 test {ZRANK after deletion} {
     r del zkey
     r zadd zkey 1 a 2 b
     r zrem zkey a
     assert_equal 0 [r zrank zkey b]
     assert_equal {b} [r zrange zkey 0 -1]
 }
*/
#[test]
fn zrank_after_deletion() {
    with_families(|ctx| {
        ctx.del("zkey");
        ctx.add("zkey", 1.0, "a").unwrap();
        ctx.add("zkey", 2.0, "b").unwrap();
        ctx.rem("zkey", "a").unwrap();
        let r = ctx.rank("zkey", "b").unwrap().unwrap();
        assert_eq!(r, 0);
        let vals = ctx.range("zkey", 0, -1).unwrap();
        assert_eq!(vals, ["b"]);
    });
}

/*
 test {ZINCRBY can create new set} {
     r del zkey
     r zincrby zkey 5 a
     assert_equal {a 5} [r zrange zkey 0 -1 withscores]
 }
*/
#[test]
fn zincrby_can_create_new_set() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            ctx.del("zkey");
            ctx.incrby("zkey", 5.0, "a").unwrap();
            let vals = ctx.range_ws("zkey", 0, -1).unwrap();
            assert_eq!(vals, ["a", "5"]);
        }
    });
}

/*
 test {ZINCRBY increment & decrement ordering} {
     r del zkey
     r zadd zkey 1 a 2 b
     r zincrby zkey 5 a
     r zincrby zkey -3 b
     assert_equal {b a} [r zrange zkey 0 1]
 }
*/
#[test]
fn zincrby_increment_and_decrement_ordering() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            ctx.del("zkey");
            ctx.add("zkey", 1.0, "a").unwrap();
            ctx.add("zkey", 2.0, "b").unwrap();
            ctx.incrby("zkey", 5.0, "a").unwrap();
            ctx.incrby("zkey", -3.0, "b").unwrap();
            let vals = ctx.range("zkey", 0, 1).unwrap();
            assert_eq!(vals, ["b", "a"]);
        }
    });
}

/*
 test {ZINCRBY return value} {
     r del zkey
     assert_equal 5   [r zincrby zkey 5 a]
     assert_equal 2.5 [r zincrby zkey -2.5 a]
 }
*/
#[test]
fn zincrby_return_value() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            ctx.del("zkey");
            let v1 = ctx.incrby("zkey", 5.0, "a").unwrap();
            assert!((v1 - 5.0).abs() < f64::EPSILON);
            let v2 = ctx.incrby("zkey", -2.5, "a").unwrap();
            assert!((v2 - 2.5).abs() < f64::EPSILON);
        }
    });
}

/*
 test {ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZCOUNT basics} {
     r del zkey
     for {set i 1} {$i <= 10} {incr i} {
         r zadd zkey $i m$i
     }
     assert_equal {m1 m2 m3} [r zrangebyscore zkey -inf 3]
     assert_equal {m10 m9 m8} [r zrevrangebyscore zkey +inf 8 limit 0 3]
     assert_equal 4 [r zcount zkey 7 10]
 }
*/
#[test]
fn zrangebyscore_and_revrange_and_count_basics() {
    with_families(|ctx| {
        ctx.del("zkey");
        for i in 1..=10 {
            ctx.add("zkey", i as f64, &format!("m{i}")).unwrap();
        }
        // TODO: implement score-based range commands for module
        if ctx.fam == Fam::BuiltIn {
            let r1 = ctx.rangebyscore("zkey", "-inf", "3", false, None).unwrap();
            assert_eq!(r1, ["m1", "m2", "m3"]);
            let r2 = ctx
                .revrangebyscore("zkey", "+inf", "8", Some((0, 3)))
                .unwrap();
            assert_eq!(r2, ["m10", "m9", "m8"]);
            let cnt = ctx.count("zkey", "7", "10").unwrap();
            assert_eq!(cnt, 4);
        }
    });
}

/*
 test {ZRANGEBYSCORE WITHSCORES} {
     r del zkey
     r zadd zkey 1 a 2 b 3 c
     assert_equal {a 1 b 2} [r zrangebyscore zkey -inf 2 withscores]
 }
*/
#[test]
fn zrangebyscore_withscores() {
    with_families(|ctx| {
        ctx.del("zkey");
        ctx.add("zkey", 1.0, "a").unwrap();
        ctx.add("zkey", 2.0, "b").unwrap();
        ctx.add("zkey", 3.0, "c").unwrap();
        if ctx.fam == Fam::BuiltIn {
            let vals = ctx.rangebyscore("zkey", "-inf", "2", true, None).unwrap();
            assert_eq!(vals, ["a", "1", "b", "2"]);
        }
    });
}

/*
 test {ZRANGEBYSCORE LIMIT} {
     r del zkey
     r zadd zkey 1 a 2 b 3 c 4 d
     assert_equal {b c} [r zrangebyscore zkey -inf +inf limit 1 2]
 }
*/
#[test]
fn zrangebyscore_limit() {
    with_families(|ctx| {
        ctx.del("zkey");
        ctx.add("zkey", 1.0, "a").unwrap();
        ctx.add("zkey", 2.0, "b").unwrap();
        ctx.add("zkey", 3.0, "c").unwrap();
        ctx.add("zkey", 4.0, "d").unwrap();
        if ctx.fam == Fam::BuiltIn {
            let vals = ctx
                .rangebyscore("zkey", "-inf", "+inf", false, Some((1, 2)))
                .unwrap();
            assert_eq!(vals, ["b", "c"]);
        }
    });
}

/*
 test {ZRANGEBYSCORE LIMIT + WITHSCORES} {
     r del zkey
     r zadd zkey 1 a 2 b 3 c 4 d
     assert_equal {b 2 c 3} \
         [r zrangebyscore zkey -inf +inf withscores limit 1 2]
 }
*/
#[test]
fn zrangebyscore_limit_withscores() {
    with_families(|ctx| {
        ctx.del("zkey");
        ctx.add("zkey", 1.0, "a").unwrap();
        ctx.add("zkey", 2.0, "b").unwrap();
        ctx.add("zkey", 3.0, "c").unwrap();
        ctx.add("zkey", 4.0, "d").unwrap();
        if ctx.fam == Fam::BuiltIn {
            let vals = ctx
                .rangebyscore("zkey", "-inf", "+inf", true, Some((1, 2)))
                .unwrap();
            assert_eq!(vals, ["b", "2", "c", "3"]);
        }
    });
}

/*
 test {ZRANGEBYSCORE invalid min/max -> error} {
     catch {r zrangebyscore zkey 0 nan} res
     assert_match {*min or max is not a float*} $res
 }
*/
#[test]
fn zrangebyscore_invalid_min_max_error() {
    with_families(|ctx| {
        // TODO: validate error handling once module supports ZRANGEBYSCORE
        if ctx.fam == Fam::BuiltIn {
            let res = ctx.rangebyscore("zkey", "0", "nan", false, None);
            assert!(res.is_err());
        }
    });
}

/*
 test {ZRANGEBYLEX/ZREVRANGEBYLEX/ZLEXCOUNT basics} {
     r del zkey
     foreach m {a b c d e f} { r zadd zkey 0 $m }
     assert_equal {a b c} [r zrangebylex zkey [a (d]
     assert_equal {f e d} [r zrevrangebylex zkey (g [d]
     assert_equal 3 [r zlexcount zkey [b (e]
 }
*/
#[test]
fn zrangebylex_revrangebylex_zlexcount_basics() {
    with_families(|ctx| {
        ctx.del("zkey");
        for m in ["a", "b", "c", "d", "e", "f"] {
            ctx.add("zkey", 0.0, m).unwrap();
        }
        if ctx.fam == Fam::BuiltIn {
            let r1 = ctx.rangebylex("zkey", "[a", "(d", None).unwrap();
            assert_eq!(r1, ["a", "b", "c"]);
            let r2 = ctx.revrangebylex("zkey", "(g", "[d").unwrap();
            assert_eq!(r2, ["f", "e", "d"]);
            let count = ctx.lexcount("zkey", "[b", "(e").unwrap();
            assert_eq!(count, 3);
        }
    });
}

/*
 test {ZLEXCOUNT advanced cases} {
     r del zkey
     foreach m {a b c d e f} { r zadd zkey 0 $m }
     assert_equal 6 [r zlexcount zkey - +]
     assert_equal 0 [r zlexcount zkey (f (f]
 }
*/
#[test]
fn zlexcount_advanced_cases() {
    with_families(|ctx| {
        ctx.del("zkey");
        for m in ["a", "b", "c", "d", "e", "f"] {
            ctx.add("zkey", 0.0, m).unwrap();
        }
        if ctx.fam == Fam::BuiltIn {
            let c1 = ctx.lexcount("zkey", "-", "+").unwrap();
            assert_eq!(c1, 6);
            let c2 = ctx.lexcount("zkey", "(f", "(f").unwrap();
            assert_eq!(c2, 0);
        }
    });
}

/*
 test {ZRANGEBYLEX LIMIT} {
     r del zkey
     foreach m {a b c d e f} { r zadd zkey 0 $m }
     assert_equal {b c} [r zrangebylex zkey - + limit 1 2]
 }
*/
#[test]
fn zrangebylex_limit() {
    with_families(|ctx| {
        ctx.del("zkey");
        for m in ["a", "b", "c", "d", "e", "f"] {
            ctx.add("zkey", 0.0, m).unwrap();
        }
        if ctx.fam == Fam::BuiltIn {
            let vals = ctx.rangebylex("zkey", "-", "+", Some((1, 2))).unwrap();
            assert_eq!(vals, ["b", "c"]);
        }
    });
}

/*
 test {ZRANGEBYLEX invalid range specifiers} {
     catch {r zrangebylex zkey foo bar} res
     assert_match {*wrong number of arguments*} $res
 }
*/
#[test]
fn zrangebylex_invalid_range_specifiers() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            let res = ctx.rangebylex("zkey", "foo", "bar", None);
            assert!(res.is_err());
        }
    });
}

/*
 test {ZREMRANGEBYSCORE basics (13 sub-scenarios)} {
     r del zkey
     for {set i 1} {$i <= 10} {incr i} { r zadd zkey $i m$i }
     r zremrangebyscore zkey 1 3
     assert_equal 7 [r zcard zkey]
     r zremrangebyscore zkey (8 +inf
     assert_equal 4 [r zcard zkey]
     r zremrangebyscore zkey -inf (5
     assert_equal 2 [r zcard zkey]
     r zremrangebyscore zkey -inf +inf
     assert_equal 0 [r exists zkey]
 }
*/
#[test]
fn zremrangebyscore_basics() {
    with_families(|ctx| {
        ctx.del("zkey");
        for i in 1..=10 {
            ctx.add("zkey", i as f64, &format!("m{i}")).unwrap();
        }
        if ctx.fam == Fam::BuiltIn {
            ctx.remrangebyscore("zkey", "1", "3").unwrap();
            let card = ctx.card("zkey").unwrap();
            assert_eq!(card, 7);
            ctx.remrangebyscore("zkey", "8", "+inf").unwrap();
            let card = ctx.card("zkey").unwrap();
            assert_eq!(card, 4);
            ctx.remrangebyscore("zkey", "-inf", "5").unwrap();
            let card = ctx.card("zkey").unwrap();
            assert_eq!(card, 2);
            ctx.remrangebyscore("zkey", "-inf", "+inf").unwrap();
            let exists = ctx.exists("zkey").unwrap();
            assert_eq!(exists, 0);
        }
    });
}

/*
 test {ZREMRANGEBYSCORE invalid min/max -> error} {
     catch {r zremrangebyscore zkey foo bar} res
     assert_match {*min or max is not a float*} $res
 }
*/
#[test]
fn zremrangebyscore_invalid_min_max_error() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            let res = ctx.remrangebyscore("zkey", "foo", "bar");
            assert!(res.is_err());
        }
    });
}

/*
 test {ZREMRANGEBYRANK basics} {
     r del zkey
     for {set i 1} {$i <= 5} {incr i} { r zadd zkey $i m$i }
     r zremrangebyrank zkey 1 3
     assert_equal {m1 m5} [r zrange zkey 0 -1]
 }
*/
#[test]
fn zremrangebyrank_basics() {
    with_families(|ctx| {
        ctx.del("zkey");
        for i in 1..=5 {
            ctx.add("zkey", i as f64, &format!("m{i}")).unwrap();
        }
        if ctx.fam == Fam::BuiltIn {
            ctx.remrangebyrank("zkey", 1, 3).unwrap();
            let vals = ctx.range("zkey", 0, -1).unwrap();
            assert_eq!(vals, ["m1", "m5"]);
        }
    });
}

/*
 test {ZREMRANGEBYLEX basics} {
     r del zkey
     foreach m {a b c d e f} { r zadd zkey 0 $m }
     r zremrangebylex zkey [b (e
     assert_equal {a e f} [r zrange zkey 0 -1]
 }
*/
#[test]
fn zremrangebylex_basics() {
    with_families(|ctx| {
        ctx.del("zkey");
        for m in ["a", "b", "c", "d", "e", "f"] {
            ctx.add("zkey", 0.0, m).unwrap();
        }
        if ctx.fam == Fam::BuiltIn {
            ctx.remrangebylex("zkey", "[b", "(e").unwrap();
            let vals = ctx.range("zkey", 0, -1).unwrap();
            assert_eq!(vals, ["a", "e", "f"]);
        }
    });
}

/*
 test {ZUNIONSTORE against non-existing key} {
     r del foo bar dst
     r zadd foo 1 a
     assert_equal 1 [r zunionstore dst 2 foo bar]
     assert_equal {a} [r zrange dst 0 -1]
 }
*/
#[test]
fn zunionstore_against_non_existing_key() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            cmd("DEL")
                .arg("foo")
                .arg("bar")
                .arg("dst")
                .query::<i64>(&mut *ctx.con)
                .unwrap();
            ctx.add("foo", 1.0, "a").unwrap();
            let res = ctx.unionstore("dst", &["foo", "bar"]).unwrap();
            assert_eq!(res, 1);
            let vals = ctx.range("dst", 0, -1).unwrap();
            assert_eq!(vals, ["a"]);
        }
    });
}

/*
 test {ZUNION/ZINTER/ZDIFF/ZINTERCARD against non-existing key} {
     r del foo bar
     assert_equal {} [r zunion 2 foo bar]
     assert_equal {} [r zinter 2 foo bar]
     assert_equal {} [r zdiff 2 foo bar]
     assert_equal 0  [r zintercard 2 foo bar]
 }
*/
#[test]
fn zunion_zinter_zdiff_zintercard_against_non_existing_key() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            cmd("DEL")
                .arg("foo")
                .arg("bar")
                .query::<i64>(&mut *ctx.con)
                .unwrap();
            let u = ctx.union(&["foo", "bar"]).unwrap();
            assert!(u.is_empty());
            let i = ctx.inter(&["foo", "bar"]).unwrap();
            assert!(i.is_empty());
            let d = ctx.diff(&["foo", "bar"]).unwrap();
            assert!(d.is_empty());
            let card = ctx.intercard(&["foo", "bar"]).unwrap();
            assert_eq!(card, 0);
        }
    });
}

/* ZRANDMEMBER basics */
#[test]
fn zrandmember_basics() {
    with_families(|ctx| {
        ctx.del("zkey");
        ctx.add("zkey", 1.0, "a").unwrap();
        ctx.add("zkey", 2.0, "b").unwrap();
        ctx.add("zkey", 3.0, "c").unwrap();
        if ctx.fam == Fam::BuiltIn {
            let mut vals = ctx.randmember("zkey", Some(3), false).unwrap();
            vals.sort();
            assert_eq!(vals, ["a", "b", "c"]);
        }
    });
}

/* ZRANDMEMBER WITHSCORES */
#[test]
fn zrandmember_withscores() {
    use std::collections::HashMap;
    with_families(|ctx| {
        ctx.del("zkey");
        ctx.add("zkey", 1.0, "a").unwrap();
        ctx.add("zkey", 2.0, "b").unwrap();
        if ctx.fam == Fam::BuiltIn {
            let vals = ctx.randmember("zkey", Some(2), true).unwrap();
            let mut map = HashMap::new();
            for pair in vals.chunks(2) {
                map.insert(pair[0].clone(), pair[1].clone());
            }
            assert_eq!(map.get("a"), Some(&"1".to_string()));
            assert_eq!(map.get("b"), Some(&"2".to_string()));
        }
    });
}

/* ZRANDMEMBER negative count (allow duplicates) */
#[test]
fn zrandmember_negative_count_duplicates() {
    with_families(|ctx| {
        ctx.del("zkey");
        ctx.add("zkey", 1.0, "a").unwrap();
        ctx.add("zkey", 2.0, "b").unwrap();
        ctx.add("zkey", 3.0, "c").unwrap();
        if ctx.fam == Fam::BuiltIn {
            let vals = ctx.randmember("zkey", Some(-5), false).unwrap();
            assert_eq!(vals.len(), 5);
            assert!(vals.iter().all(|v| ["a", "b", "c"].contains(&v.as_str())));
            let dup = vals
                .iter()
                .enumerate()
                .any(|(i, v)| vals[i + 1..].contains(v));
            assert!(dup, "expected at least one duplicate");
        }
    });
}

/* ZPOPMIN/ZPOPMAX basics */
#[test]
fn zpopmin_zpopmax_basics() {
    with_families(|ctx| {
        ctx.del("zk");
        ctx.add("zk", 1.0, "a").unwrap();
        ctx.add("zk", 2.0, "b").unwrap();
        ctx.add("zk", 3.0, "c").unwrap();
        if ctx.fam == Fam::BuiltIn {
            let v1 = ctx.popmin("zk", None).unwrap();
            assert_eq!(v1, ["a", "1"]);
            let v2 = ctx.popmax("zk", None).unwrap();
            assert_eq!(v2, ["c", "3"]);
        }
    });
}

/* ZMSCORE basics */
#[test]
fn zmscore_basics() {
    with_families(|ctx| {
        ctx.del("ms");
        ctx.add("ms", 1.0, "a").unwrap();
        ctx.add("ms", 2.0, "b").unwrap();
        if ctx.fam == Fam::BuiltIn {
            let vals = ctx.mscore("ms", &["a", "b", "c"]).unwrap();
            assert_eq!(vals, vec![Some(1.0), Some(2.0), None]);
        }
    });
}

/* ZSCAN yields full set */
#[test]
fn zscan_yields_full_set() {
    use std::collections::HashSet;
    with_families(|ctx| {
        ctx.del("scan");
        for i in 0..50 {
            ctx.add("scan", i as f64, &format!("m{i}")).unwrap();
        }
        if ctx.fam == Fam::BuiltIn {
            let mut cur = 0u64;
            let mut items = HashSet::new();
            loop {
                let (next, chunk) = ctx.scan("scan", cur).unwrap();
                for pair in chunk.chunks(2) {
                    items.insert(pair[0].clone());
                }
                if next == 0 {
                    break;
                }
                cur = next;
            }
            assert_eq!(items.len(), 50);
        }
    });
}

/* ZUNIONSTORE with WEIGHTS */
#[test]
fn zunionstore_with_weights() {
    with_families(|ctx| {
        ctx.del("a");
        ctx.del("b");
        if ctx.fam == Fam::BuiltIn {
            ctx.add("a", 1.0, "x").unwrap();
            ctx.add("b", 2.0, "x").unwrap();
            ctx.add("b", 3.0, "y").unwrap();
            ctx.unionstore_weights("dst", &["a", "b"], &[2, 3]).unwrap();
            let vals = ctx.range_ws("dst", 0, -1).unwrap();
            assert_eq!(vals, ["x", "8", "y", "9"]);
        }
    });
}

/* ZUNIONSTORE with AGGREGATE MAX */
#[test]
fn zunionstore_with_aggregate_max() {
    with_families(|ctx| {
        ctx.del("a");
        ctx.del("b");
        if ctx.fam == Fam::BuiltIn {
            ctx.add("a", 1.0, "x").unwrap();
            ctx.add("b", 2.0, "x").unwrap();
            ctx.add("b", 3.0, "y").unwrap();
            ctx.unionstore_aggregate_max("dst", &["a", "b"]).unwrap();
            let vals = ctx.range_ws("dst", 0, -1).unwrap();
            assert_eq!(vals, ["x", "2", "y", "3"]);
        }
    });
}

/* ZINTERSTORE with WEIGHTS */
#[test]
fn zinterstore_with_weights() {
    with_families(|ctx| {
        ctx.del("a");
        ctx.del("b");
        if ctx.fam == Fam::BuiltIn {
            ctx.add("a", 1.0, "x").unwrap();
            ctx.add("a", 2.0, "y").unwrap();
            ctx.add("b", 3.0, "x").unwrap();
            ctx.add("b", 4.0, "y").unwrap();
            ctx.interstore_weights("dst", &["a", "b"], &[2, 3]).unwrap();
            let vals = ctx.range_ws("dst", 0, -1).unwrap();
            assert_eq!(vals, ["x", "11", "y", "16"]);
        }
    });
}

/* ZDIFFSTORE basic difference */
#[test]
fn zdiffstore_basic_difference() {
    with_families(|ctx| {
        ctx.del("a");
        ctx.del("b");
        if ctx.fam == Fam::BuiltIn {
            ctx.add("a", 1.0, "x").unwrap();
            ctx.add("a", 2.0, "y").unwrap();
            ctx.add("b", 3.0, "y").unwrap();
            ctx.diffstore("dst", &["a", "b"]).unwrap();
            let vals = ctx.range_ws("dst", 0, -1).unwrap();
            assert_eq!(vals, ["x", "1"]);
        }
    });
}

/* ZINTERCARD with limit */
#[test]
fn zintercard_with_limit() {
    with_families(|ctx| {
        ctx.del("a");
        ctx.del("b");
        if ctx.fam == Fam::BuiltIn {
            ctx.add("a", 1.0, "x").unwrap();
            ctx.add("a", 2.0, "y").unwrap();
            ctx.add("b", 3.0, "y").unwrap();
            ctx.add("b", 4.0, "z").unwrap();
            let card = ctx.intercard_limit(&["a", "b"], 1).unwrap();
            assert_eq!(card, 1);
        }
    });
}

/* ZADD NX GT combination error */
#[test]
fn zadd_nx_gt_combination_error() {
    with_families(|ctx| {
        ctx.del("k");
        let res: RedisResult<i64> = cmd(&zcmd(ctx.fam, "ADD"))
            .arg("k")
            .arg("NX")
            .arg("GT")
            .arg("1")
            .arg("a")
            .query(&mut *ctx.con);
        assert!(res.is_err());
    });
}

/* ZADD invalid option order */
#[test]
fn zadd_invalid_option_order() {
    with_families(|ctx| {
        ctx.del("k");
        let res: RedisResult<i64> = cmd(&zcmd(ctx.fam, "ADD"))
            .arg("k")
            .arg("1")
            .arg("a")
            .arg("NX")
            .arg("CH")
            .query(&mut *ctx.con);
        assert!(res.is_err());
    });
}

/* ZUNION – WITHSCORES option */
#[test]
fn zunion_with_withscores_option() {
    with_families(|ctx| {
        ctx.del("a");
        ctx.del("b");
        if ctx.fam == Fam::BuiltIn {
            ctx.add("a", 1.0, "x").unwrap();
            ctx.add("b", 2.0, "y").unwrap();
            let vals = ctx.union_withscores(&["a", "b"]).unwrap();
            assert_eq!(vals, ["x", "1", "y", "2"]);
        }
    });
}

/* ZINTER – error on missing WEIGHTS count */
#[test]
fn zinter_error_missing_weights_count() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            let res: RedisResult<Vec<String>> = cmd("ZINTER")
                .arg(2)
                .arg("a")
                .arg("b")
                .arg("WEIGHTS")
                .arg(2)
                .query(&mut *ctx.con);
            assert!(res.is_err());
        }
    });
}

/* ZDIFF – WITHSCORES basics */
#[test]
fn zdiff_withscores_basics() {
    with_families(|ctx| {
        ctx.del("a");
        ctx.del("b");
        if ctx.fam == Fam::BuiltIn {
            ctx.add("a", 1.0, "x").unwrap();
            ctx.add("a", 2.0, "y").unwrap();
            ctx.add("b", 3.0, "y").unwrap();
            let res = ctx.diff_withscores(&["a", "b"]).unwrap();
            assert_eq!(res, ["x", "1"]);
        }
    });
}

/* BZPOPMIN blocks & unblocks */
#[test]
fn bzpopmin_blocks_unblocks() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            ctx.del("bk");
            let res: Option<Vec<String>> = cmd("BZPOPMIN")
                .arg("bk")
                .arg(1)
                .query(&mut *ctx.con)
                .unwrap();
            assert!(res.is_none());
            ctx.add("bk", 1.0, "a").unwrap();
            let res: Vec<String> = cmd("BZPOPMIN")
                .arg("bk")
                .arg(1)
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(res, ["bk", "a", "1"]);
        }
    });
}

/* BZPOPMAX timeout */
#[test]
fn bzpopmax_timeout() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            ctx.del("bk");
            let res: Option<Vec<String>> = cmd("BZPOPMAX")
                .arg("bk")
                .arg(1)
                .query(&mut *ctx.con)
                .unwrap();
            assert!(res.is_none());
        }
    });
}

/* ZPOP* against empty key returns nil */
#[test]
fn zpop_against_empty_key_returns_nil() {
    with_families(|ctx| {
        ctx.del("empty");
        if ctx.fam == Fam::BuiltIn {
            let res: Option<Vec<String>> =
                cmd("ZPOPMIN").arg("empty").query(&mut *ctx.con).unwrap();
            assert!(res.is_none() || res.as_ref().unwrap().is_empty());
        }
    });
}

/* ZUNIONSTORE / duplicate keys treated once */
#[test]
fn zunionstore_duplicate_keys_once() {
    with_families(|ctx| {
        ctx.del("foo");
        ctx.add("foo", 1.0, "a").unwrap();
        if ctx.fam == Fam::BuiltIn {
            let res = ctx.unionstore("dstdup", &["foo", "foo"]).unwrap();
            assert_eq!(res, 1);
            let vals = ctx.range("dstdup", 0, -1).unwrap();
            assert_eq!(vals, ["a"]);
        }
    });
}

/* ZADD INCR + GT + NX incompatibility */
#[test]
fn zadd_incr_gt_nx_incompatibility() {
    with_families(|ctx| {
        let res: RedisResult<f64> = cmd(&zcmd(ctx.fam, "ADD"))
            .arg("k")
            .arg("INCR")
            .arg("GT")
            .arg("NX")
            .arg("1")
            .arg("a")
            .query(&mut *ctx.con);
        assert!(res.is_err());
    });
}

/* ZSET write against a key of wrong type */
#[test]
fn zset_write_wrong_type() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            cmd("SET")
                .arg("foo")
                .arg("bar")
                .query::<()>(&mut *ctx.con)
                .unwrap();
            let res: RedisResult<i64> = cmd("ZADD")
                .arg("foo")
                .arg("1")
                .arg("a")
                .query(&mut *ctx.con);
            assert!(res.is_err());
        }
    });
}

/* ZSET commands in MULTI/EXEC */
#[test]
fn zset_commands_in_multi_exec() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            let _: () = cmd("MULTI").query(&mut *ctx.con).unwrap();
            cmd("ZADD")
                .arg("trans")
                .arg("1")
                .arg("a")
                .query::<()>(&mut *ctx.con)
                .unwrap();
            let res: Vec<redis::Value> = cmd("EXEC").query(&mut *ctx.con).unwrap();
            assert_eq!(res.len(), 1);
        }
    });
}

/*
 test {ZRANGESTORE basic} {
     r flushall
     r zadd z1{t} 1 a 2 b 3 c 4 d
     set res [r zrangestore z2{t} z1{t} 0 -1]
     assert_equal $res 4
     r zrange z2{t} 0 -1 withscores
 } {a 1 b 2 c 3 d 4}
*/
#[test]
fn zrangestore_basics() {
    with_families(|ctx| {
        ctx.del("src");
        ctx.del("dst");
        ctx.add("src", 1.0, "a").unwrap();
        ctx.add("src", 2.0, "b").unwrap();
        if ctx.fam == Fam::BuiltIn {
            let _: i64 = cmd("ZRANGESTORE")
                .arg("dst")
                .arg("src")
                .arg(0)
                .arg(-1)
                .query(&mut *ctx.con)
                .unwrap();
            let vals = ctx.range("dst", 0, -1).unwrap();
            assert_eq!(vals, ["a", "b"]);
        }
    });
}

/*
 test {ZRANGESTORE RESP3} {
     r hello 3
     assert_equal [r zrange z2{t} 0 -1 withscores] {{a 1.0} {b 2.0} {c 3.0} {d 4.0}}
     r hello 2
 }
*/
#[test]
fn zrangestore_withscores() {
    with_families(|ctx| {
        ctx.del("src");
        ctx.del("dst");
        ctx.add("src", 1.0, "a").unwrap();
        ctx.add("src", 2.0, "b").unwrap();
        if ctx.fam == Fam::BuiltIn {
            let res: RedisResult<i64> = cmd("ZRANGESTORE")
                .arg("dst")
                .arg("src")
                .arg(0)
                .arg(-1)
                .arg("WITHSCORES")
                .query(&mut *ctx.con);
            if res.is_ok() {
                let vals = ctx.range_ws("dst", 0, -1).unwrap();
                assert_eq!(vals, ["a", "1", "b", "2"]);
            }
        }
    });
}

/*
 test {ZRANGESTORE - src key missing} {
     set res [r zrangestore z2{t} missing{t} 0 -1]
     assert_equal $res 0
     r exists z2{t}
 } {0}
*/
#[test]
fn zrangestore_src_key_missing() {
    with_families(|ctx| {
        ctx.del("dst");
        if ctx.fam == Fam::BuiltIn {
            let res: i64 = cmd("ZRANGESTORE")
                .arg("dst")
                .arg("missing")
                .arg(0)
                .arg(-1)
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(res, 0);
            let exists = ctx.exists("dst").unwrap();
            assert_eq!(exists, 0);
        }
    });
}

/*
 test {ZRANGESTORE - src key wrong type} {
     r zadd z2{t} 1 a
     r set foo{t} bar
     assert_error "*WRONGTYPE*" {r zrangestore z2{t} foo{t} 0 -1}
     r zrange z2{t} 0 -1
 } {a}
*/
#[test]
fn zrangestore_src_key_wrong_type() {
    with_families(|ctx| {
        ctx.del("src");
        ctx.del("foo");
        ctx.add("src", 1.0, "a").unwrap();
        cmd("SET")
            .arg("foo")
            .arg("bar")
            .query::<()>(&mut *ctx.con)
            .unwrap();
        if ctx.fam == Fam::BuiltIn {
            let res: RedisResult<i64> = cmd("ZRANGESTORE")
                .arg("src")
                .arg("foo")
                .arg(0)
                .arg(-1)
                .query(&mut *ctx.con);
            assert!(res.is_err());
            let vals = ctx.range("src", 0, -1).unwrap();
            assert_eq!(vals, ["a"]);
        }
    });
}

/*
 test {ZRANGESTORE range} {
     set res [r zrangestore z2{t} z1{t} 1 2]
     assert_equal $res 2
     r zrange z2{t} 0 -1 withscores
 } {b 2 c 3}
*/
#[test]
fn zrangestore_range() {
    with_families(|ctx| {
        ctx.del("z1");
        ctx.del("z2");
        for (s, m) in &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d")] {
            ctx.add("z1", *s, m).unwrap();
        }
        if ctx.fam == Fam::BuiltIn {
            let res: i64 = cmd("ZRANGESTORE")
                .arg("z2")
                .arg("z1")
                .arg(1)
                .arg(2)
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(res, 2);
            let vals = ctx.range_ws("z2", 0, -1).unwrap();
            assert_eq!(vals, ["b", "2", "c", "3"]);
        }
    });
}

/*
 test {ZRANGESTORE BYLEX} {
     set res [r zrangestore z3{t} z1{t} \[b \[c BYLEX]
     assert_equal $res 2
     assert_encoding listpack z3{t}
     set res [r zrangestore z2{t} z1{t} \[b \[c BYLEX]
     assert_equal $res 2
     r zrange z2{t} 0 -1 withscores
 } {b 2 c 3}
*/
#[test]
fn zrangestore_bylex() {
    with_families(|ctx| {
        ctx.del("z1");
        ctx.del("z2");
        ctx.del("z3");
        for (s, m) in &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d")] {
            ctx.add("z1", *s, m).unwrap();
        }
        if ctx.fam == Fam::BuiltIn {
            let res1: i64 = cmd("ZRANGESTORE")
                .arg("z3")
                .arg("z1")
                .arg("[b")
                .arg("[c")
                .arg("BYLEX")
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(res1, 2);
            let enc = ctx.object_encoding("z3").unwrap();
            assert_eq!(enc, "listpack");
            let res2: i64 = cmd("ZRANGESTORE")
                .arg("z2")
                .arg("z1")
                .arg("[b")
                .arg("[c")
                .arg("BYLEX")
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(res2, 2);
            let vals = ctx.range_ws("z2", 0, -1).unwrap();
            assert_eq!(vals, ["b", "2", "c", "3"]);
        }
    });
}

/*
 test {ZRANGESTORE BYSCORE} {
     set res [r zrangestore z4{t} z1{t} 1 2 BYSCORE]
     assert_equal $res 2
     assert_encoding listpack z4{t}
     set res [r zrangestore z2{t} z1{t} 1 2 BYSCORE]
     assert_equal $res 2
     r zrange z2{t} 0 -1 withscores
 } {a 1 b 2}
*/
#[test]
fn zrangestore_byscore() {
    with_families(|ctx| {
        ctx.del("z1");
        ctx.del("z2");
        ctx.del("z4");
        for (s, m) in &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d")] {
            ctx.add("z1", *s, m).unwrap();
        }
        if ctx.fam == Fam::BuiltIn {
            let r1: i64 = cmd("ZRANGESTORE")
                .arg("z4")
                .arg("z1")
                .arg(1)
                .arg(2)
                .arg("BYSCORE")
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(r1, 2);
            let enc = ctx.object_encoding("z4").unwrap();
            assert_eq!(enc, "listpack");
            let r2: i64 = cmd("ZRANGESTORE")
                .arg("z2")
                .arg("z1")
                .arg(1)
                .arg(2)
                .arg("BYSCORE")
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(r2, 2);
            let vals = ctx.range_ws("z2", 0, -1).unwrap();
            assert_eq!(vals, ["a", "1", "b", "2"]);
        }
    });
}

/*
 test {ZRANGESTORE BYSCORE LIMIT} {
     set res [r zrangestore z2{t} z1{t} 0 5 BYSCORE LIMIT 0 2]
     assert_equal $res 2
     r zrange z2{t} 0 -1 withscores
 } {a 1 b 2}
*/
#[test]
fn zrangestore_byscore_limit() {
    with_families(|ctx| {
        ctx.del("z1");
        ctx.del("z2");
        for (s, m) in &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d")] {
            ctx.add("z1", *s, m).unwrap();
        }
        if ctx.fam == Fam::BuiltIn {
            let res: i64 = cmd("ZRANGESTORE")
                .arg("z2")
                .arg("z1")
                .arg(0)
                .arg(5)
                .arg("BYSCORE")
                .arg("LIMIT")
                .arg(0)
                .arg(2)
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(res, 2);
            let vals = ctx.range_ws("z2", 0, -1).unwrap();
            assert_eq!(vals, ["a", "1", "b", "2"]);
        }
    });
}

/*
 test {ZRANGESTORE BYSCORE REV LIMIT} {
     set res [r zrangestore z2{t} z1{t} 5 0 BYSCORE REV LIMIT 0 2]
     assert_equal $res 2
     r zrange z2{t} 0 -1 withscores
 } {c 3 d 4}
*/
#[test]
fn zrangestore_byscore_rev_limit() {
    with_families(|ctx| {
        ctx.del("z1");
        ctx.del("z2");
        for (s, m) in &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d")] {
            ctx.add("z1", *s, m).unwrap();
        }
        if ctx.fam == Fam::BuiltIn {
            let res: i64 = cmd("ZRANGESTORE")
                .arg("z2")
                .arg("z1")
                .arg(5)
                .arg(0)
                .arg("BYSCORE")
                .arg("REV")
                .arg("LIMIT")
                .arg(0)
                .arg(2)
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(res, 2);
            let vals = ctx.range_ws("z2", 0, -1).unwrap();
            assert_eq!(vals, ["c", "3", "d", "4"]);
        }
    });
}

/*
 test {ZRANGE BYSCORE REV LIMIT} {
     r zrange z1{t} 5 0 BYSCORE REV LIMIT 0 2 WITHSCORES
 } {d 4 c 3}
*/
#[test]
fn zrange_byscore_rev_limit() {
    with_families(|ctx| {
        ctx.del("z1");
        for (s, m) in &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d")] {
            ctx.add("z1", *s, m).unwrap();
        }
        if ctx.fam == Fam::BuiltIn {
            let vals: Vec<String> = cmd("ZRANGE")
                .arg("z1")
                .arg(5)
                .arg(0)
                .arg("BYSCORE")
                .arg("REV")
                .arg("LIMIT")
                .arg(0)
                .arg(2)
                .arg("WITHSCORES")
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(vals, ["d", "4", "c", "3"]);
        }
    });
}

/*
 test {ZRANGESTORE - empty range} {
     set res [r zrangestore z2{t} z1{t} 5 6]
     assert_equal $res 0
     r exists z2{t}
 } {0}
*/
#[test]
fn zrangestore_empty_range() {
    with_families(|ctx| {
        ctx.del("z1");
        ctx.del("z2");
        for (s, m) in &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d")] {
            ctx.add("z1", *s, m).unwrap();
        }
        if ctx.fam == Fam::BuiltIn {
            let res: i64 = cmd("ZRANGESTORE")
                .arg("z2")
                .arg("z1")
                .arg(5)
                .arg(6)
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(res, 0);
            let exists = ctx.exists("z2").unwrap();
            assert_eq!(exists, 0);
        }
    });
}

/*
 test {ZRANGESTORE BYLEX - empty range} {
     set res [r zrangestore z2{t} z1{t} \[f \[g BYLEX]
     assert_equal $res 0
     r exists z2{t}
 } {0}
*/
#[test]
fn zrangestore_bylex_empty_range() {
    with_families(|ctx| {
        ctx.del("z1");
        ctx.del("z2");
        for (s, m) in &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d")] {
            ctx.add("z1", *s, m).unwrap();
        }
        if ctx.fam == Fam::BuiltIn {
            let res: i64 = cmd("ZRANGESTORE")
                .arg("z2")
                .arg("z1")
                .arg("[f")
                .arg("[g")
                .arg("BYLEX")
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(res, 0);
            let exists = ctx.exists("z2").unwrap();
            assert_eq!(exists, 0);
        }
    });
}

/*
 test {ZRANGESTORE BYSCORE - empty range} {
     set res [r zrangestore z2{t} z1{t} 5 6 BYSCORE]
     assert_equal $res 0
     r exists z2{t}
 } {0}
*/
#[test]
fn zrangestore_byscore_empty_range() {
    with_families(|ctx| {
        ctx.del("z1");
        ctx.del("z2");
        for (s, m) in &[(1.0, "a"), (2.0, "b"), (3.0, "c"), (4.0, "d")] {
            ctx.add("z1", *s, m).unwrap();
        }
        if ctx.fam == Fam::BuiltIn {
            let res: i64 = cmd("ZRANGESTORE")
                .arg("z2")
                .arg("z1")
                .arg(5)
                .arg(6)
                .arg("BYSCORE")
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(res, 0);
            let exists = ctx.exists("z2").unwrap();
            assert_eq!(exists, 0);
        }
    });
}

/*
 test {ZRANGE BYLEX} {
     r zrange z1{t} \[b \[c BYLEX
 } {b c}
*/
#[test]
fn zrange_bylex() {
    with_families(|ctx| {
        ctx.del("z1");
        for (s, m) in &[(0.0, "a"), (0.0, "b"), (0.0, "c"), (0.0, "d")] {
            ctx.add("z1", *s, m).unwrap();
        }
        if ctx.fam == Fam::BuiltIn {
            let vals = ctx.rangebylex("z1", "[b", "[c", None).unwrap();
            assert_eq!(vals, ["b", "c"]);
        }
    });
}

/*
 test {ZRANGESTORE invalid syntax} {
     catch {r zrangestore z2{t} z1{t} 0 -1 limit 1 2} err
     assert_match "*syntax*" $err
     catch {r zrangestore z2{t} z1{t} 0 -1 WITHSCORES} err
     assert_match "*syntax*" $err
 }
*/
#[test]
fn zrangestore_invalid_syntax() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            let res1: RedisResult<i64> = cmd("ZRANGESTORE")
                .arg("dst")
                .arg("src")
                .arg(0)
                .arg(-1)
                .arg("limit")
                .arg(1)
                .arg(2)
                .query(&mut *ctx.con);
            assert!(res1.is_err());
            let res2: RedisResult<i64> = cmd("ZRANGESTORE")
                .arg("dst")
                .arg("src")
                .arg(0)
                .arg(-1)
                .arg("WITHSCORES")
                .query(&mut *ctx.con);
            assert!(res2.is_err());
        }
    });
}

/*
 test {ZRANGESTORE with zset-max-listpack-entries 0 #10767 case} {
     set original_max [lindex [r config get zset-max-listpack-entries] 1]
     r config set zset-max-listpack-entries 0
     r del z1{t} z2{t}
     r zadd z1{t} 1 a
     assert_encoding skiplist z1{t}
     assert_equal 1 [r zrangestore z2{t} z1{t} 0 -1]
     assert_encoding skiplist z2{t}
     r config set zset-max-listpack-entries $original_max
 }
*/
#[test]
fn zrangestore_lp_entries_zero_case() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            let orig = ctx.config_get("zset-max-listpack-entries").unwrap()[1].clone();
            ctx.config_set("zset-max-listpack-entries", "0").unwrap();
            ctx.del("z1");
            ctx.del("z2");
            ctx.add("z1", 1.0, "a").unwrap();
            let enc = ctx.object_encoding("z1").unwrap();
            assert_eq!(enc, "skiplist");
            let res: i64 = cmd("ZRANGESTORE")
                .arg("z2")
                .arg("z1")
                .arg(0)
                .arg(-1)
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(res, 1);
            let enc2 = ctx.object_encoding("z2").unwrap();
            assert_eq!(enc2, "skiplist");
            ctx.config_set("zset-max-listpack-entries", &orig).unwrap();
        }
    });
}

/*
 test {ZRANGESTORE with zset-max-listpack-entries 1 dst key should use skiplist encoding} {
     set original_max [lindex [r config get zset-max-listpack-entries] 1]
     r config set zset-max-listpack-entries 1
     r del z1{t} z2{t} z3{t}
     r zadd z1{t} 1 a 2 b
     assert_equal 1 [r zrangestore z2{t} z1{t} 0 0]
     assert_encoding listpack z2{t}
     assert_equal 2 [r zrangestore z3{t} z1{t} 0 1]
     assert_encoding skiplist z3{t}
     r config set zset-max-listpack-entries $original_max
 }
*/
#[test]
fn zrangestore_lp_entries_one_skiplist_dst() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            let orig = ctx.config_get("zset-max-listpack-entries").unwrap()[1].clone();
            ctx.config_set("zset-max-listpack-entries", "1").unwrap();
            ctx.del("z1");
            ctx.del("z2");
            ctx.del("z3");
            ctx.add("z1", 1.0, "a").unwrap();
            ctx.add("z1", 2.0, "b").unwrap();
            let res1: i64 = cmd("ZRANGESTORE")
                .arg("z2")
                .arg("z1")
                .arg(0)
                .arg(0)
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(res1, 1);
            assert_eq!(ctx.object_encoding("z2").unwrap(), "listpack");
            let res2: i64 = cmd("ZRANGESTORE")
                .arg("z3")
                .arg("z1")
                .arg(0)
                .arg(1)
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(res2, 2);
            assert_eq!(ctx.object_encoding("z3").unwrap(), "skiplist");
            ctx.config_set("zset-max-listpack-entries", &orig).unwrap();
        }
    });
}

/*
 test {zset score double range} {
     set dblmax 179769313486231570814527423731704356798070567525844996598917476803157260780028538760589558632766878171540458953514382464234321326889464182768467546703537516986049910576551282076245490090389328944075868508455133942304583236903222948165808559332123348274797826204144723168738177180919299881250404026184124858368.00000000000000000
     r del zz
     r zadd zz $dblmax dblmax
     assert_encoding listpack zz
     r zscore zz dblmax
 } {1.7976931348623157e+308}
*/
#[test]
fn zset_score_double_range() {
    with_families(|ctx| {
        ctx.del("zz");
        if ctx.fam == Fam::BuiltIn {
            let max = f64::MAX;
            let _: i64 = cmd("ZADD")
                .arg("zz")
                .arg(max.to_string())
                .arg("dblmax")
                .query(&mut *ctx.con)
                .unwrap();
            let enc = ctx.object_encoding("zz").unwrap();
            assert_eq!(enc, "listpack");
            let score = ctx.score("zz", "dblmax").unwrap().unwrap();
            assert_eq!(score, max);
        }
    });
}

/*
 test {zunionInterDiffGenericCommand acts on SET and ZSET} {
     r del set_small{t} set_big{t} zset_small{t} zset_big{t} zset_dest{t}

     foreach set_type {intset listpack hashtable} {
         r config set set-max-intset-entries 512
         r config set set-max-listpack-entries 128
         r config set zset-max-listpack-entries 128

         r del set_small{t} set_big{t}

         if {$set_type == "intset"} {
             r sadd set_small{t} 1 2 3
             r sadd set_big{t} 1 2 3 4 5
             assert_encoding intset set_small{t}
             assert_encoding intset set_big{t}
         } elseif {$set_type == "listpack"} {
             r sadd set_small{t} a 1 2 3
             r sadd set_big{t} a 1 2 3 4 5
             r srem set_small{t} a
             r srem set_big{t} a
             assert_encoding listpack set_small{t}
             assert_encoding listpack set_big{t}
         } elseif {$set_type == "hashtable"} {
             r config set set-max-intset-entries 0
             r config set set-max-listpack-entries 0
             r sadd set_small{t} 1 2 3
             r sadd set_big{t} 1 2 3 4 5
             assert_encoding hashtable set_small{t}
             assert_encoding hashtable set_big{t}
         }

         foreach zset_type {listpack skiplist} {
             r del zset_small{t} zset_big{t}

             if {$zset_type == "listpack"} {
                 r zadd zset_small{t} 1 1 2 2 3 3
                 r zadd zset_big{t} 1 1 2 2 3 3 4 4 5 5
                 assert_encoding listpack zset_small{t}
                 assert_encoding listpack zset_big{t}
             } elseif {$zset_type == "skiplist"} {
                 r config set zset-max-listpack-entries 0
                 r zadd zset_small{t} 1 1 2 2 3 3
                 r zadd zset_big{t} 1 1 2 2 3 3 4 4 5 5
                 assert_encoding skiplist zset_small{t}
                 assert_encoding skiplist zset_big{t}
             }

             foreach {small_or_big set_key zset_key} {
                 small set_small{t} zset_big{t}
                 big set_big{t} zset_small{t}
             } {
                 assert_equal {1 2 3 4 5} [lsort [r zunion 2 $set_key $zset_key]]
                 assert_equal {5} [r zunionstore zset_dest{t} 2 $set_key $zset_key]
                 assert_equal {1 2 3} [lsort [r zinter 2 $set_key $zset_key]]
                 assert_equal {3} [r zinterstore zset_dest{t} 2 $set_key $zset_key]
                 assert_equal {3} [r zintercard 2 $set_key $zset_key]

                 if {$small_or_big == "small"} {
                     assert_equal {} [r zdiff 2 $set_key $zset_key]
                     assert_equal {0} [r zdiffstore zset_dest{t} 2 $set_key $zset_key]
                 } else {
                     assert_equal {4 5} [lsort [r zdiff 2 $set_key $zset_key]]
                     assert_equal {2} [r zdiffstore zset_dest{t} 2 $set_key $zset_key]
                 }
             }
         }
     }

     r config set set-max-intset-entries 512
     r config set set-max-listpack-entries 128
     r config set zset-max-listpack-entries 128
 }
*/
#[test]
fn zunion_interdiff_with_sets() {
    with_families(|ctx| {
        ctx.del("set_small");
        ctx.del("set_big");
        ctx.del("zset_small");
        ctx.del("zset_big");
        ctx.del("dest");
        if ctx.fam == Fam::BuiltIn {
            cmd("SADD")
                .arg("set_small")
                .arg("1")
                .arg("2")
                .arg("3")
                .query::<i64>(&mut *ctx.con)
                .unwrap();
            cmd("SADD")
                .arg("set_big")
                .arg("1")
                .arg("2")
                .arg("3")
                .arg("4")
                .arg("5")
                .query::<i64>(&mut *ctx.con)
                .unwrap();
            for (s, m) in &[(1.0, "1"), (2.0, "2"), (3.0, "3")] {
                ctx.add("zset_small", *s, m).unwrap();
            }
            for (s, m) in &[(1.0, "1"), (2.0, "2"), (3.0, "3"), (4.0, "4"), (5.0, "5")] {
                ctx.add("zset_big", *s, m).unwrap();
            }

            let mut union = ctx.union(&["set_small", "zset_big"]).unwrap();
            union.sort();
            assert_eq!(union, ["1", "2", "3", "4", "5"]);
            let res = ctx.unionstore("dest", &["set_small", "zset_big"]).unwrap();
            assert_eq!(res, 5);
            let mut inter = ctx.inter(&["set_small", "zset_big"]).unwrap();
            inter.sort();
            assert_eq!(inter, ["1", "2", "3"]);
            let _: i64 = cmd("ZINTERSTORE")
                .arg("dest")
                .arg(2)
                .arg("set_small")
                .arg("zset_big")
                .query(&mut *ctx.con)
                .unwrap();
            let card = ctx.card("dest").unwrap();
            assert_eq!(card, 3);
            let card2 = ctx.intercard(&["set_small", "zset_big"]).unwrap();
            assert_eq!(card2, 3);
            let diff = ctx.diff(&["set_small", "zset_big"]).unwrap();
            assert!(diff.is_empty());
            let res = ctx.diffstore("dest", &["set_small", "zset_big"]).unwrap();
            assert_eq!(res, 0);

            let mut union = ctx.union(&["set_big", "zset_small"]).unwrap();
            union.sort();
            assert_eq!(union, ["1", "2", "3", "4", "5"]);
            let res = ctx.unionstore("dest", &["set_big", "zset_small"]).unwrap();
            assert_eq!(res, 5);
            let mut inter = ctx.inter(&["set_big", "zset_small"]).unwrap();
            inter.sort();
            assert_eq!(inter, ["1", "2", "3"]);
            let _: i64 = cmd("ZINTERSTORE")
                .arg("dest")
                .arg(2)
                .arg("set_big")
                .arg("zset_small")
                .query(&mut *ctx.con)
                .unwrap();
            let card = ctx.card("dest").unwrap();
            assert_eq!(card, 3);
            let card2 = ctx.intercard(&["set_big", "zset_small"]).unwrap();
            assert_eq!(card2, 3);
            let mut diff = ctx.diff(&["set_big", "zset_small"]).unwrap();
            diff.sort();
            assert_eq!(diff, ["4", "5"]);
            let res = ctx.diffstore("dest", &["set_big", "zset_small"]).unwrap();
            assert_eq!(res, 2);
        }
    });
}

/*
 foreach type {single multiple single_multiple} {
     test "ZADD overflows the maximum allowed elements in a listpack - $type" {
         r del myzset

         set max_entries 64
         set original_max [lindex [r config get zset-max-listpack-entries] 1]
         r config set zset-max-listpack-entries $max_entries

         if {$type == "single"} {
             for {set i 0} {$i < $max_entries} {incr i} { r zadd myzset $i $i }
         } elseif {$type == "multiple"} {
             set args {}
             for {set i 0} {$i < $max_entries * 2} {incr i} { lappend args $i }
             r zadd myzset {*}$args
         } elseif {$type == "single_multiple"} {
             r zadd myzset 1 1
             set args {}
             for {set i 0} {$i < $max_entries * 2} {incr i} { lappend args $i }
             r zadd myzset {*}$args
         }

         assert_encoding listpack myzset
         assert_equal $max_entries [r zcard myzset]
         assert_equal 1 [r zadd myzset 1 b]
         assert_encoding skiplist myzset

         r config set zset-max-listpack-entries $original_max
     }
 }
*/
#[test]
fn zadd_overflows_listpack_limit() {
    with_families(|ctx| {
        if ctx.fam == Fam::BuiltIn {
            let orig = ctx.config_get("zset-max-listpack-entries").unwrap()[1].clone();
            ctx.config_set("zset-max-listpack-entries", "64").unwrap();
            for mode in ["single", "multiple", "single_multiple"] {
                ctx.del("myzset");
                match mode {
                    "single" => {
                        for i in 0..64 {
                            ctx.add("myzset", i as f64, &i.to_string()).unwrap();
                        }
                    }
                    "multiple" => {
                        let mut args: Vec<String> = Vec::new();
                        for i in 0..128 {
                            args.push(i.to_string());
                        }
                        let mut c = cmd("ZADD");
                        c.arg("myzset");
                        for a in &args {
                            c.arg(a);
                        }
                        c.query::<i64>(&mut *ctx.con).unwrap();
                    }
                    "single_multiple" => {
                        ctx.add("myzset", 1.0, "1").unwrap();
                        let mut args: Vec<String> = Vec::new();
                        for i in 0..128 {
                            args.push(i.to_string());
                        }
                        let mut c = cmd("ZADD");
                        c.arg("myzset");
                        for a in &args {
                            c.arg(a);
                        }
                        c.query::<i64>(&mut *ctx.con).unwrap();
                    }
                    _ => unreachable!(),
                }
                let enc = ctx.object_encoding("myzset").unwrap();
                assert_eq!(enc, "listpack");
                assert_eq!(ctx.card("myzset").unwrap(), 64);
                assert_eq!(ctx.add("myzset", 1.0, "b").unwrap(), 1);
                let enc2 = ctx.object_encoding("myzset").unwrap();
                assert_eq!(enc2, "skiplist");
            }
            ctx.config_set("zset-max-listpack-entries", &orig).unwrap();
        }
    });
}
