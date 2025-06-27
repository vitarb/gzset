#[test]
fn build_so() {
    assert!(
        std::path::Path::new("target/debug/libgzset.so").exists()
            || std::process::Command::new("cargo")
                .args(["build"])
                .status()
                .expect("cargo build")
                .success(),
        "failed to build module",
    );
}
