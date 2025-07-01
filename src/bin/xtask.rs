use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::{
    env,
    path::Path,
    process::{Command, Stdio},
    thread,
    time::Duration,
};

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
            args,
        } => start_valkey(profile, port, &args),
    }
}

fn start_valkey(profile: Profile, port_opt: Option<u16>, extra_args: &[String]) -> Result<()> {
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

    // 3) Pick a port ----------------------------------------------------------
    let port = port_opt.unwrap_or_else(|| portpicker::pick_unused_port().expect("no free ports"));

    // 4) Spawn valkey-server --------------------------------------------------
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
    for _ in 0..20u8 {
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

