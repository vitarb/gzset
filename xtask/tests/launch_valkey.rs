use std::{process::Command, thread, time::Duration};

#[test]
fn launch_valkey() -> Result<(), Box<dyn std::error::Error>> {
    if Command::new("valkey-server").arg("--version").output().is_err() {
        eprintln!("valkey-server not found in PATH; skipping test");
        return Ok(());
    }

    let port = portpicker::pick_unused_port().expect("no free ports");
    let mut cmd = Command::new("cargo");
    cmd.args([
        "run",
        "-p",
        "xtask",
        "--",
        "start-valkey",
        "--profile",
        "debug",
        "--port",
        &port.to_string(),
    ]);
    let mut child = cmd.spawn()?;

    let mut connected = false;
    for _ in 0..40 {
        if let Ok(client) = redis::Client::open(format!("redis://127.0.0.1:{port}")) {
            if let Ok(mut conn) = client.get_connection() {
                if let Ok(resp) = redis::cmd("PING").query::<String>(&mut conn) {
                    if resp == "PONG" {
                        connected = true;
                        break;
                    }
                }
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert!(connected, "valkey-server did not respond to PING");

    #[cfg(unix)]
    {
        use nix::sys::signal::{self, Signal};
        use nix::unistd::Pid;
        signal::kill(Pid::from_raw(child.id() as i32), Signal::SIGINT)?;
    }
    #[cfg(windows)]
    {
        child.kill()?;
    }

    let _ = child.wait()?;
    Ok(())
}
