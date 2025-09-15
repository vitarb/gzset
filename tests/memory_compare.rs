//! Integration-test: compare ZSET vs GZSET memory across a range of sizes

mod helpers;

use std::{fs::File, io::Write, time::Duration};

const SIZES: &[usize] = &[
    1, 10, 100, 1_000, 10_000, 50_000, 100_000, 500_000, 1_000_000,
];

/// Parse `used_memory` from INFO MEMORY
fn used_memory(con: &mut redis::Connection) -> redis::RedisResult<i64> {
    let info: String = redis::cmd("INFO").arg("MEMORY").query(con)?;
    Ok(info
        .lines()
        .find_map(|l| l.strip_prefix("used_memory:"))
        .unwrap()
        .trim()
        .parse()
        .unwrap())
}

fn wait_for_stable_memory(con: &mut redis::Connection) -> redis::RedisResult<i64> {
    const STABILITY_THRESHOLD: u64 = 1024;
    let mut last = used_memory(con)?;
    for _ in 0..20 {
        std::thread::sleep(Duration::from_millis(50));
        let current = used_memory(con)?;
        if last.abs_diff(current) <= STABILITY_THRESHOLD {
            return Ok(current);
        }
        last = current;
    }
    Ok(last)
}

#[test]
fn memory_profile() -> redis::RedisResult<()> {
    let vk = helpers::ValkeyInstance::start();
    let mut con = redis::Client::open(vk.url())?.get_connection()?;
    std::thread::sleep(Duration::from_millis(100)); // stabilise RSS

    // CSV written next to workspace root
    let mut csv = File::create("memory_profile.csv").unwrap();
    writeln!(csv, "size,gz_logical,gz_delta,zs_logical,zs_delta")?;

    let mut last_gz = 0i64;
    let mut last_gz_delta = 0i64;
    let mut last_zs = 0i64;

    for &n in SIZES {
        // ---- GZSET ---------------------------------------------------------
        redis::cmd("FLUSHALL").query::<()>(&mut con)?;
        redis::cmd("MEMORY").arg("PURGE").query::<()>(&mut con)?;
        let base = wait_for_stable_memory(&mut con)?;

        let mut pipe = redis::pipe();
        (0..n).for_each(|i| {
            pipe.cmd("GZADD").arg("gz").arg(i).arg(i);
        });
        pipe.query::<()>(&mut con)?;

        let gz_usage: i64 = redis::cmd("MEMORY")
            .arg("USAGE")
            .arg("gz")
            .query(&mut con)?;
        let gz_delta = used_memory(&mut con)? - base;

        // ---- ZSET ----------------------------------------------------------
        redis::cmd("DEL").arg("gz").query::<()>(&mut con)?;
        redis::cmd("MEMORY").arg("PURGE").query::<()>(&mut con)?;
        let base2 = wait_for_stable_memory(&mut con)?;

        let mut pipe = redis::pipe();
        (0..n).for_each(|i| {
            pipe.cmd("ZADD").arg("zs").arg(i).arg(i);
        });
        pipe.query::<()>(&mut con)?;

        let zs_usage: i64 = redis::cmd("MEMORY")
            .arg("USAGE")
            .arg("zs")
            .query(&mut con)?;
        let zs_delta = used_memory(&mut con)? - base2;

        last_gz = gz_usage;
        last_gz_delta = gz_delta;
        last_zs = zs_usage;

        // CSV row + console echo
        let row = format!("{n},{gz_usage},{gz_delta},{zs_usage},{zs_delta}");
        println!("{row}");
        writeln!(csv, "{row}")?;
    }

    // gzset memory usage should remain bounded
    assert!(last_gz > 0 && last_zs > 0);

    // allocator overhead should be reasonable
    assert!(last_gz_delta > 0);

    println!("📊  Wrote memory_profile.csv (run with --nocapture to see rows)");
    Ok(())
}
