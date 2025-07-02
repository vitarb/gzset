fn occupant_info(out: std::process::Output) -> Option<(u32, String)> {
    let stdout = String::from_utf8_lossy(&out.stdout);
    let line = stdout.lines().skip(1).next()?;
    let mut parts = line.split_whitespace();
    let cmd = parts.next()?.to_string();
    let pid = parts.next()?.parse().ok()?;
    Some((pid, cmd))
}
fn main() {}
