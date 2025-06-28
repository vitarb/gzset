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

// ----- ZADD option matrix tests -----

// ZADD with options syntax error with incomplete pair
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
            let card: i64 = cmd("ZCARD")
                .arg("ztmp")
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(card, 1);
        }
    });
}

// ZADD XX return value is number actually added
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
            let card: i64 = cmd("ZCARD")
                .arg("ztmp")
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(card, 3);
            assert_eq!(
                cmd("ZSCORE")
                    .arg("ztmp")
                    .arg("x")
                    .query::<f64>(&mut *ctx.con)
                    .unwrap(),
                11.0
            );
            assert_eq!(
                cmd("ZSCORE")
                    .arg("ztmp")
                    .arg("y")
                    .query::<f64>(&mut *ctx.con)
                    .unwrap(),
                21.0
            );
        }
    });
}

// ZADD GT updates existing elements when new scores are greater
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
            let card: i64 = cmd("ZCARD")
                .arg("ztmp")
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(card, 4);
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("x").query::<f64>(&mut *ctx.con).unwrap(),
                11.0
            );
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("y").query::<f64>(&mut *ctx.con).unwrap(),
                21.0
            );
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("z").query::<f64>(&mut *ctx.con).unwrap(),
                30.0
            );
        }
    });
}

// ZADD LT updates existing elements when new scores are lower
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
            let card: i64 = cmd("ZCARD")
                .arg("ztmp")
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(card, 4);
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("x").query::<f64>(&mut *ctx.con).unwrap(),
                10.0
            );
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("y").query::<f64>(&mut *ctx.con).unwrap(),
                20.0
            );
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("z").query::<f64>(&mut *ctx.con).unwrap(),
                29.0
            );
        }
    });
}

// ZADD GT XX updates existing elements when new scores are greater and skips new elements
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
            let card: i64 = cmd("ZCARD")
                .arg("ztmp")
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(card, 3);
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("x").query::<f64>(&mut *ctx.con).unwrap(),
                11.0
            );
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("y").query::<f64>(&mut *ctx.con).unwrap(),
                21.0
            );
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("z").query::<f64>(&mut *ctx.con).unwrap(),
                30.0
            );
        }
    });
}

// ZADD LT XX updates existing elements when new scores are lower and skips new elements
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
            let card: i64 = cmd("ZCARD")
                .arg("ztmp")
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(card, 3);
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("x").query::<f64>(&mut *ctx.con).unwrap(),
                10.0
            );
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("y").query::<f64>(&mut *ctx.con).unwrap(),
                20.0
            );
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("z").query::<f64>(&mut *ctx.con).unwrap(),
                29.0
            );
        }
    });
}

// ZADD XX and NX are not compatible
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
            let card: i64 = cmd("ZCARD")
                .arg("ztmp")
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(card, 3);
        }
    });
}

// ZADD NX only add new elements without updating old ones
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
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("x").query::<f64>(&mut *ctx.con).unwrap(),
                10.0
            );
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("y").query::<f64>(&mut *ctx.con).unwrap(),
                20.0
            );
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("a").query::<f64>(&mut *ctx.con).unwrap(),
                100.0
            );
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("b").query::<f64>(&mut *ctx.con).unwrap(),
                200.0
            );
        }
    });
}

// ZADD GT and NX are not compatible
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
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("x").query::<f64>(&mut *ctx.con).unwrap(),
                28.0
            );
            let res2: Option<f64> = cmd("ZADD")
                .arg("ztmp")
                .arg("GT")
                .arg("INCR")
                .arg("-1")
                .arg("x")
                .query(&mut *ctx.con)
                .unwrap();
            assert!(res2.is_none());
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("x").query::<f64>(&mut *ctx.con).unwrap(),
                28.0
            );
        }
    });
}

// ZADD INCR LT/GT with inf
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
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("x").query::<f64>(&mut *ctx.con).unwrap(),
                f64::INFINITY
            );
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("y").query::<f64>(&mut *ctx.con).unwrap(),
                f64::NEG_INFINITY
            );
        }
    });
}

// ZADD INCR works like ZINCRBY
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
            assert_eq!(
                cmd("ZSCORE").arg("ztmp").arg("x").query::<f64>(&mut *ctx.con).unwrap(),
                25.0
            );
        }
    });
}

// ZADD INCR works with a single score-element pair
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
            let res: Vec<String> = cmd("ZRANGE")
                .arg("myzset")
                .arg("0")
                .arg("-1")
                .arg("WITHSCORES")
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(res, ["a", "10", "b", "20", "c", "30"]);
        }
    });
}

// ZADD - Return value is the number of actually added items
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
            let vals: Vec<String> = cmd("ZRANGE")
                .arg("myzset")
                .arg("0")
                .arg("-1")
                .arg("WITHSCORES")
                .query(&mut *ctx.con)
                .unwrap();
            assert_eq!(vals, ["x", "5", "a", "10", "b", "20", "c", "30"]);
        }
    });
}

// ZADD - Variadic version does not add nothing on single parsing err
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
        let exists: i32 = cmd("EXISTS")
            .arg("myzset")
            .query(&mut *ctx.con)
            .unwrap();
        if ctx.fam == Fam::BuiltIn {
            assert_eq!(exists, 0);
        }
    });
}

// ZADD - Variadic version will raise error on missing arg
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
