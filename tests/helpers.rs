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
            use std::env::consts::{DLL_PREFIX, DLL_SUFFIX};

            let candidates = [
                format!("target/release/{}gzset{}", DLL_PREFIX, DLL_SUFFIX),
                format!("target/debug/{}gzset{}", DLL_PREFIX, DLL_SUFFIX),
            ];

            let mut path = candidates
                .iter()
                .find(|p| std::path::Path::new(p).exists())
                .cloned();

            if path.is_none() {
                let status = Command::new("cargo")
                    .arg("build")
                    .status()
                    .expect("failed to run cargo build");
                assert!(status.success(), "cargo build failed");

                path = candidates
                    .iter()
                    .find(|p| std::path::Path::new(p).exists())
                    .cloned();
            }

            let path = path.expect("libgzset.so not built");
            std::fs::canonicalize(path).unwrap()
        };

        let child = Command::new("valkey-server")
            .arg("--port")
            .arg(port.to_string())
            .arg("--loadmodule")
            .arg(so_path)
            .arg("--save")
            .arg("")
            .arg("--daemonize")
            .arg("no")
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
            .arg("-p")
            .arg(self.port.to_string())
            .arg("shutdown")
            .arg("nosave")
            .status();
        let _ = self.child.wait();
    }
}
