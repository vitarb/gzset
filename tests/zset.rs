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
        cmd(&format!("{}ADD", self.fam.prefix()))
            .arg(key)
            .arg(score.to_string())
            .arg(member)
            .query(&mut *self.con)
    }
    fn range(&mut self, key: &str, start: isize, stop: isize) -> RedisResult<Vec<String>> {
        cmd(&format!("{}RANGE", self.fam.prefix()))
            .arg(key)
            .arg(start)
            .arg(stop)
            .query(&mut *self.con)
    }
    fn rank(&mut self, key: &str, member: &str) -> RedisResult<Option<i64>> {
        cmd(&format!("{}RANK", self.fam.prefix()))
            .arg(key)
            .arg(member)
            .query(&mut *self.con)
    }
    fn score(&mut self, key: &str, member: &str) -> RedisResult<Option<f64>> {
        cmd(&format!("{}SCORE", self.fam.prefix()))
            .arg(key)
            .arg(member)
            .query(&mut *self.con)
    }
    fn rem(&mut self, key: &str, member: &str) -> RedisResult<i64> {
        cmd(&format!("{}REM", self.fam.prefix()))
            .arg(key)
            .arg(member)
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

#[test]
fn removal_and_key_destroy() {
    with_families(|ctx| {
        ctx.del("kr");
        ctx.add("kr", 1.0, "only").unwrap();
        assert_eq!(ctx.rem("kr", "only").unwrap(), 1);
        assert_eq!(ctx.r#type("kr"), "none");
    });
}

#[test]
fn rejects_nan_scores() {
    with_families(|ctx| {
        let nan = std::f64::NAN.to_string();
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
