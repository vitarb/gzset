fn main() {
    println!("cargo:rustc-check-cfg=cfg(reply_double_default)");
    if std::env::var("CARGO_CFG_TARGET_OS")
        .map(|target| target == "linux")
        .unwrap_or(false)
    {
        println!("cargo:rustc-cfg=reply_double_default");
    }
}
