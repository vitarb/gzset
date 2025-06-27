#[test]
fn build_so() {
    use std::env::consts::{DLL_PREFIX, DLL_SUFFIX};

    let debug = format!("target/debug/{}gzset{}", DLL_PREFIX, DLL_SUFFIX);
    let exists = std::path::Path::new(&debug).exists()
        || std::process::Command::new("cargo")
            .arg("build")
            .status()
            .expect("cargo build")
            .success();
    assert!(exists, "failed to build module");
}
