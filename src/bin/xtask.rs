use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::{
    env,
    path::Path,
    process::{Command, Stdio},
    thread,
    time::Duration,
};

const DEFAULT_PORT: u16 = 6379;

fn occupant_info(port: u16) -> Option<(u32, String)> {
    #[cfg(target_os = "linux")]
    {
        let out = Command::new("lsof")
            .args(["-i", &format!(":{port}"), "-sTCP:LISTEN", "-t"])
            .output()
            .ok()?;
        let pid = String::from_utf8_lossy(&out.stdout)
            .lines()
            .next()?
            .trim()
            .parse::<u32>()
            .ok()?;
        let exe = std::fs::read_link(format!("/proc/{pid}/exe")).ok()?;
        Some((pid, exe.display().to_string()))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let out = Command::new("lsof")
            .args(["-i", &format!(":{port}")])
            .output()
            .ok()?;
        let line = String::from_utf8_lossy(&out.stdout)
            .lines()
            .skip(1)
            .next()?;
        let mut parts = line.split_whitespace();
        let cmd = parts.next()?.to_string();
        let pid = parts.next()?.parse().ok()?;
        Some((pid, cmd))
    }
}

#[derive(Parser)]
#[command(author, version, about)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Build libgzset and launch valkey-server with the module pre-loaded.
    StartValkey {
        /// debug (default) or release
        #[arg(long, default_value = "debug")]
        profile: Profile,
        /// Optional fixed port. If omitted an unused one is picked automatically.
        #[arg(long)]
        port: Option<u16>,
        /// Kill any existing valkey on port 6379 before starting
        #[arg(long)]
        force_kill: bool,
        /// Extra arguments forwarded verbatim to valkey-server
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
}

#[derive(clap::ValueEnum, Clone)]
enum Profile {
    Debug,
    Release,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.cmd {
        Cmd::StartValkey {
            profile,
            port,
            force_kill,
            args,
        } => start_valkey(profile, port, force_kill, &args),
    }
}

fn start_valkey(
    profile: Profile,
    port_opt: Option<u16>,
    force_kill: bool,
    extra_args: &[String],
) -> Result<()> {
    let profile_flag = match profile {
        Profile::Debug => "debug",
        Profile::Release => "release",
    };

    // 1) Build the module ----------------------------------------------------
    let mut build = Command::new("cargo");
    build.arg("build").arg("--package=gzset");
    if matches!(profile, Profile::Release) {
        build.arg("--release");
    }
    anyhow::ensure!(build.status()?.success(), "cargo build failed");

    // 2) Resolve full path to the .so/.dylib/.dll ----------------------------
    let so_name = format!(
        "{}gzset{}",
        env::consts::DLL_PREFIX,
        env::consts::DLL_SUFFIX
    );
    let so_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(profile_flag)
        .join(&so_name);
    anyhow::ensure!(so_path.exists(), "module not found at {so_path:?}");

    // 3) Stop previous server if requested -----------------------------------
    if force_kill && port_opt.unwrap_or(DEFAULT_PORT) == DEFAULT_PORT {
        if let Some((pid, exe)) = occupant_info(DEFAULT_PORT) {
            eprintln!("=> terminating process on port {DEFAULT_PORT}: PID {pid} ({exe})");
            let _ = Command::new("valkey-cli")
                .arg("-p")
                .arg(DEFAULT_PORT.to_string())
                .arg("shutdown")
                .arg("nosave")
                .status();
            let _ = Command::new("kill").arg("-9").arg(pid.to_string()).status();
            for _ in 0..10 {
                if portpicker::is_free(DEFAULT_PORT) {
                    break;
                }
                thread::sleep(Duration::from_millis(100));
            }
        }
    }

    // 4) Pick a port ----------------------------------------------------------
    let port = if let Some(p) = port_opt {
        p
    } else if portpicker::is_free(DEFAULT_PORT) {
        DEFAULT_PORT
    } else {
        let fallback = portpicker::pick_unused_port().expect("no free ports");
        if let Some((pid, exe)) = occupant_info(DEFAULT_PORT) {
            eprintln!(
                "*** WARNING: port {DEFAULT_PORT} in use by PID {pid} ({exe}). Using port {fallback}"
            );
        } else {
            eprintln!("*** WARNING: port {DEFAULT_PORT} in use. Using port {fallback}");
        }
        fallback
    };

    // 5) Spawn valkey-server --------------------------------------------------
    let mut cmd = Command::new("valkey-server");
    cmd.arg("--port")
        .arg(port.to_string())
        .arg("--loadmodule")
        .arg(&so_path)
        .arg("--save")
        .arg("") // disable RDB
        .arg("--daemonize")
        .arg("no")
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    // pass-through additional flags
    cmd.args(extra_args);

    let mut child = cmd.spawn().context("failed to start valkey-server")?;

    // 5) Wait until server is up (health probe)
    for _ in 0..50u8 {
        if redis::Client::open(format!("redis://127.0.0.1:{port}"))
            .and_then(|c| c.get_connection())
            .and_then(|mut con| redis::cmd("PING").query::<String>(&mut con))
            .map(|p| p == "PONG")
            .unwrap_or(false)
        {
            println!("=> launching valkey-server on port {port}");
            println!("=> module path         {}", so_path.display());
            println!("=> redis url           redis://127.0.0.1:{port}");
            println!("⇧ press Ctrl-C to stop");
            let status = child.wait()?;
            anyhow::bail!("valkey-server exited with status {status}");
        }
        thread::sleep(Duration::from_millis(100));
    }
    let _ = child.kill();
    anyhow::bail!("valkey-server failed to start");
}
