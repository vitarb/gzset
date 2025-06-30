use std::process::{Child, Command};
use std::{fs, path::Path};
use std::{thread, time::Duration};

pub fn latest_so_path() -> std::path::PathBuf {
    use std::env::consts::{DLL_PREFIX, DLL_SUFFIX};

    let debug = format!("target/debug/{DLL_PREFIX}gzset{DLL_SUFFIX}");
    let release = format!("target/release/{DLL_PREFIX}gzset{DLL_SUFFIX}");

    if !Path::new(&debug).exists() {
        assert!(Command::new("cargo")
            .arg("build")
            .status()
            .expect("failed to run cargo build")
            .success());
    }

    let meta_dbg = fs::metadata(&debug).unwrap();
    let meta_rel = fs::metadata(&release).ok();

    match meta_rel {
        Some(m_rel) if m_rel.modified().unwrap() > meta_dbg.modified().unwrap() => {
            Path::new(&release).canonicalize().unwrap()
        }
        _ => Path::new(&debug).canonicalize().unwrap(),
    }
}

pub struct ValkeyInstance {
    pub child: Child,
    pub port: u16,
}

impl ValkeyInstance {
    #[allow(dead_code)]
    pub fn start() -> Self {
        let port = portpicker::pick_unused_port().expect("no free ports");
        let so_path = latest_so_path();

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

    #[allow(dead_code)]
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
