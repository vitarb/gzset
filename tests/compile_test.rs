#[test]
fn occupant_info_compiles() {
    let out = std::process::Command::new("rustc")
        .args(["tests/compile/occupant.rs", "-o", "/tmp/occupant_test"])
        .output()
        .expect("failed to run rustc");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}
