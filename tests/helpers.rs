use std::process::{Child, Command};
use std::{thread, time::Duration};

pub struct ValkeyInstance {
    pub child: Child,
    pub port: u16,
}

impl ValkeyInstance {
    pub fn start() -> Self {
        let port = portpicker::pick_unused_port().expect("no free ports");
        let so_path = {
            let debug = "target/debug/libgzset.so";
            let release = "target/release/libgzset.so";

            // If neither build artifact exists, attempt to build the module.
            if !std::path::Path::new(debug).exists()
                && !std::path::Path::new(release).exists()
            {
                let status = Command::new("cargo")
                    .arg("build")
                    .status()
                    .expect("failed to run cargo build");
                assert!(status.success(), "cargo build failed");
            }

            let path = if std::path::Path::new(release).exists() {
                release
            } else {
                debug
            };
            assert!(std::path::Path::new(path).exists(), "{} not built", path);
            std::fs::canonicalize(path).unwrap()
        };

        let child = Command::new("valkey-server")
            .arg("--port").arg(port.to_string())
            .arg("--loadmodule").arg(so_path)
            .arg("--save").arg("")
            .arg("--daemonize").arg("no")
            .spawn()
            .expect("failed to spawn valkey");

        thread::sleep(Duration::from_millis(300));
        Self { child, port }
    }

    pub fn url(&self) -> String {
        format!("redis://127.0.0.1:{}", self.port)
    }
}

impl Drop for ValkeyInstance {
    fn drop(&mut self) {
        let _ = Command::new("valkey-cli")
            .arg("-p").arg(self.port.to_string())
            .arg("shutdown").arg("nosave")
            .status();
        let _ = self.child.wait();
    }
}
