mod helpers;

#[test]
fn build_so() {
    let so_path = helpers::latest_so_path();
    assert!(
        std::path::Path::new(&so_path).exists(),
        "failed to build module"
    );
}
