use gzset::ScoreSet;
use std::os::raw::c_void;

extern "C" {
    fn gzset_mem_usage(value: *const c_void) -> usize;
}

#[test]
fn mem_bytes_tracking() {
    let mut set = ScoreSet::default();
    let mut prev = set.mem_bytes();
    for i in 0..1000 {
        let m = format!("m{:0200}", i);
        assert!(set.insert(i as f64, &m));
        let now = set.mem_bytes();
        assert!(now >= prev, "mem_bytes should not decrease on insert");
        prev = now;
    }
    unsafe {
        let usage = gzset_mem_usage((&set as *const ScoreSet) as *const c_void);
        let mb = set.mem_bytes();
        assert!(
            (usage as f64) <= (mb as f64 * 1.2) && (usage as f64) >= (mb as f64 * 0.8),
            "usage {usage} mem_bytes {mb}"
        );
    }
    for i in 0..1000 {
        let m = format!("m{:0200}", i);
        assert!(set.remove(&m));
        let now = set.mem_bytes();
        assert!(now <= prev, "mem_bytes should not increase on remove");
        prev = now;
    }
}
