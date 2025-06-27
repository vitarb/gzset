#[test]
fn build_so() {
    use std::env::consts::{DLL_PREFIX, DLL_SUFFIX};

    let candidates = [
        format!("target/release/{}gzset{}", DLL_PREFIX, DLL_SUFFIX),
        format!("target/debug/{}gzset{}", DLL_PREFIX, DLL_SUFFIX),
    ];

    let mut found = candidates.iter().any(|c| std::path::Path::new(c).exists());

    if !found {
        let status = std::process::Command::new("cargo")
            .arg("build")
            .status()
            .expect("cargo build");
        assert!(status.success(), "cargo build failed");
        found = candidates.iter().any(|c| std::path::Path::new(c).exists());
    }

    assert!(found, "failed to build module");
}
