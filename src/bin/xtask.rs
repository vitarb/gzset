use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use std::{
    env,
    fs::{self, File},
    io::ErrorKind,
    mem,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
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
        let stdout = String::from_utf8_lossy(&out.stdout);
        let line = stdout.lines().skip(1).next()?;
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
    /// Build gzset, start valkey-server, capture a perf profile, and emit flame.svg.
    Flame {
        /// debug or release (same as StartValkey)
        #[arg(long, default_value = "release")]
        profile: Profile,
        /// Optional fixed port. If omitted an unused one is picked automatically.
        #[arg(long)]
        port: Option<u16>,
        /// Seconds to record with perf
        #[arg(long, default_value_t = 20)]
        duration: u64,
        /// Output directory (svg will be written here as flame.svg)
        #[arg(long)]
        out_dir: Option<String>,
        /// If true, stop the server after profiling (default true)
        #[arg(long, default_value_t = true)]
        shutdown: bool,
        /// Extra args forwarded verbatim to valkey-server
        #[arg(trailing_var_arg = true)]
        args: Vec<String>,
    },
}

#[derive(clap::ValueEnum, Clone, Copy)]
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
        Cmd::Flame {
            profile,
            port,
            duration,
            out_dir,
            shutdown,
            args,
        } => flame_valkey(profile, port, duration, out_dir, shutdown, &args),
    }
}

impl Profile {
    fn artifact_dir(self) -> &'static str {
        match self {
            Profile::Debug => "debug",
            Profile::Release => "release",
        }
    }
}

fn build_module(profile: Profile, extra_rustflags: Option<&str>) -> Result<()> {
    let mut build = Command::new("cargo");
    build.arg("build").arg("--package=gzset");
    if matches!(profile, Profile::Release) {
        build.arg("--release");
    }
    build.arg("--features").arg("redis-module");

    if let Some(flags) = extra_rustflags {
        #[cfg(target_os = "linux")]
        {
            let mut rustflags = env::var("RUSTFLAGS").unwrap_or_default();
            if !rustflags.is_empty() {
                rustflags.push(' ');
            }
            rustflags.push_str(flags);
            build.env("RUSTFLAGS", rustflags);
        }
        #[cfg(not(target_os = "linux"))]
        {
            let _ = flags;
        }
    }

    anyhow::ensure!(build.status()?.success(), "cargo build failed");
    Ok(())
}

fn resolve_module_path(profile: Profile) -> Result<PathBuf> {
    let so_name = format!(
        "{}gzset{}",
        env::consts::DLL_PREFIX,
        env::consts::DLL_SUFFIX
    );
    let so_path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(profile.artifact_dir())
        .join(&so_name);
    anyhow::ensure!(so_path.exists(), "module not found at {so_path:?}");
    Ok(so_path)
}

fn spawn_valkey(
    profile: Profile,
    port_opt: Option<u16>,
    force_kill: bool,
    extra_args: &[String],
) -> Result<(Child, u16, PathBuf)> {
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

    let so_path = resolve_module_path(profile)?;

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
    cmd.args(extra_args);

    let mut child = cmd.spawn().context("failed to start valkey-server")?;

    for _ in 0..50u8 {
        if redis::Client::open(format!("redis://127.0.0.1:{port}"))
            .and_then(|c| c.get_connection())
            .and_then(|mut con| redis::cmd("PING").query::<String>(&mut con))
            .map(|p| p == "PONG")
            .unwrap_or(false)
        {
            return Ok((child, port, so_path));
        }
        thread::sleep(Duration::from_millis(100));
    }
    let _ = child.kill();
    anyhow::bail!("valkey-server failed to start");
}

fn start_valkey(
    profile: Profile,
    port_opt: Option<u16>,
    force_kill: bool,
    extra_args: &[String],
) -> Result<()> {
    build_module(profile, None)?;
    let (mut child, port, so_path) = spawn_valkey(profile, port_opt, force_kill, extra_args)?;

    println!("=> launching valkey-server on port {port}");
    println!("=> module path         {}", so_path.display());
    println!("=> redis url           redis://127.0.0.1:{port}");
    println!("⇧ press Ctrl-C to stop");

    let status = child.wait()?;
    anyhow::bail!("valkey-server exited with status {status}");
}

fn flame_valkey(
    profile: Profile,
    port_opt: Option<u16>,
    duration: u64,
    out_dir: Option<String>,
    shutdown: bool,
    extra_args: &[String],
) -> Result<()> {
    if !cfg!(target_os = "linux") {
        anyhow::bail!("perf profiling is only supported on Linux");
    }

    build_module(profile, Some("-C force-frame-pointers=yes"))?;
    let (mut child, port, so_path) = spawn_valkey(profile, port_opt, false, extra_args)?;
    let pid = child.id();

    println!("=> valkey-server PID {pid}");
    println!("=> module path         {}", so_path.display());
    println!("=> redis url           redis://127.0.0.1:{port}");

    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let default_dir = Path::new("target")
        .join("flame")
        .join(timestamp.to_string());
    let out_path = out_dir.map(PathBuf::from).unwrap_or(default_dir);
    fs::create_dir_all(&out_path).context("failed to create output directory")?;

    let mut perf_data = out_path.join("perf.data");
    let perf_cmd_str =
        format!("perf record -F 999 -g --call-graph dwarf -p {pid} -- sleep {duration}");
    println!("=> running: {perf_cmd_str}");

    let mut perf_cmd = Command::new("perf");
    perf_cmd
        .arg("record")
        .args(["-F", "999", "-g", "--call-graph", "dwarf", "-p"])
        .arg(pid.to_string())
        .arg("--")
        .arg("sleep")
        .arg(duration.to_string())
        .current_dir(&out_path)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    let perf_status = perf_cmd.status().map_err(|err| {
        if err.kind() == ErrorKind::NotFound {
            anyhow::anyhow!(
                "perf not found. Install it via: sudo apt-get install linux-tools-common linux-tools-generic"
            )
        } else {
            err.into()
        }
    })?;
    anyhow::ensure!(
        perf_status.success(),
        "perf record failed with status {perf_status}"
    );
    perf_data = fs::canonicalize(&perf_data).unwrap_or(perf_data);
    println!("=> perf data saved to {}", perf_data.display());

    let mut script = Command::new("perf")
        .arg("script")
        .current_dir(&out_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|err| {
            if err.kind() == ErrorKind::NotFound {
                anyhow::anyhow!(
                    "perf not found. Install it via: sudo apt-get install linux-tools-common linux-tools-generic"
                )
            } else {
                err.into()
            }
        })?;

    let script_stdout = script
        .stdout
        .take()
        .context("failed to capture perf script stdout")?;

    let flame_svg = out_path.join("flame.svg");
    let flame_file = File::create(&flame_svg).context("failed to create flame.svg")?;
    let inferno = Command::new("inferno-flamegraph")
        .stdin(Stdio::from(script_stdout))
        .stdout(Stdio::from(flame_file))
        .spawn();

    let mut inferno = match inferno {
        Ok(child) => child,
        Err(err) if err.kind() == ErrorKind::NotFound => {
            let _ = script.wait();
            let _ = fs::remove_file(&flame_svg);
            anyhow::bail!(
                "inferno-flamegraph not found. Install it with `cargo install inferno`. perf.data saved at {}",
                perf_data.display()
            );
        }
        Err(err) => return Err(err.into()),
    };

    let inferno_status = inferno.wait()?;
    let script_status = script.wait()?;
    anyhow::ensure!(
        script_status.success(),
        "perf script failed with status {script_status}"
    );
    anyhow::ensure!(
        inferno_status.success(),
        "inferno-flamegraph failed with status {inferno_status}"
    );

    let flame_svg = fs::canonicalize(&flame_svg).unwrap_or(flame_svg);
    println!("=> flamegraph written to {}", flame_svg.display());
    println!("=> open with: xdg-open {}", flame_svg.display());
    println!(
        "=> If stacks look flat, ensure kernel.perf_event_paranoid allows perf: sudo sysctl kernel.perf_event_paranoid=1 (or lower)."
    );
    println!(
        "=> Also confirm Cargo.toml has debug=1 and frame pointers are enabled (set by this command)."
    );

    if shutdown {
        let status = Command::new("valkey-cli")
            .arg("-p")
            .arg(port.to_string())
            .arg("shutdown")
            .arg("nosave")
            .status();
        match status {
            Ok(code) if code.success() => {
                let _ = child.wait();
            }
            _ => {
                eprintln!("=> valkey-cli shutdown failed; terminating PID {pid}");
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    } else {
        println!("=> leaving valkey-server running on port {port} (PID {pid})");
        mem::forget(child);
    }

    Ok(())
}
