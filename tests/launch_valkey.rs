use std::process::Command;
use std::time::{Duration, Instant};

fn valkey_in_path() -> bool {
    which::which("valkey-server").is_ok()
}

#[test]
fn launch_valkey() -> Result<(), Box<dyn std::error::Error>> {
    if !valkey_in_path() {
        eprintln!("valkey-server not found in PATH; skipping");
        return Ok(());
    }
    let port = portpicker::pick_unused_port().expect("no free ports");
    let mut child = Command::new("cargo")
        .arg("run")
        .arg("--bin")
        .arg("xtask")
        .arg("--")
        .arg("start-valkey")
        .arg("--profile")
        .arg("debug")
        .arg("--port")
        .arg(port.to_string())
        .spawn()?;

    let start = Instant::now();
    let client = redis::Client::open(format!("redis://127.0.0.1:{port}"))?;
    loop {
        if let Ok(mut con) = client.get_connection() {
            if let Ok(pong) = redis::cmd("PING").query::<String>(&mut con) {
                if pong == "PONG" {
                    break;
                }
            }
        }
        if start.elapsed() > Duration::from_secs(30) {
            child.kill().ok();
            panic!("valkey-server did not start in time");
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    Command::new("valkey-cli")
        .arg("-p")
        .arg(port.to_string())
        .arg("shutdown")
        .arg("nosave")
        .status()?;
    let status = child.wait()?;
    assert!(!status.success());
    Ok(())
}
