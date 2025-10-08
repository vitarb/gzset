use std::{
    fmt::Display,
    fs::{create_dir_all, OpenOptions},
    io::{BufWriter, Write},
    path::Path,
    sync::Mutex,
};

use once_cell::sync::Lazy;

static LOCK: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
const BASE_DIR: &str = "target/bench-mem";
const MEMORY_FILE: &str = "memory.csv";
const STRUCTURAL_FILE: &str = "memory_structural.csv";

pub fn record_mem<K: Display>(bench_id: K, bytes: usize) {
    record_line(MEMORY_FILE, bench_id, bytes);
}

pub fn record_structural_mem<K: Display>(bench_id: K, bytes: usize) {
    record_line(STRUCTURAL_FILE, bench_id, bytes);
}

fn record_line<K: Display>(file: &str, bench_id: K, bytes: usize) {
    let bench_id = bench_id.to_string();
    let _guard = LOCK.lock().unwrap();
    let base = Path::new(BASE_DIR);
    if let Err(err) = create_dir_all(base) {
        eprintln!("failed to create metric directory: {err}");
        return;
    }
    let path = base.join(file);
    let existed = path.exists();
    let file = match OpenOptions::new().create(true).append(true).open(&path) {
        Ok(file) => file,
        Err(err) => {
            eprintln!("failed to open metric csv {}: {err}", path.display());
            return;
        }
    };
    let mut writer = BufWriter::new(file);
    if !existed {
        if let Err(err) = writeln!(writer, "bench_id,bytes") {
            eprintln!(
                "failed to write metric header for {}: {err}",
                path.display()
            );
            return;
        }
    }
    if let Err(err) = writeln!(writer, "{bench_id},{bytes}") {
        eprintln!("failed to record metric row for {}: {err}", path.display());
    }
}
