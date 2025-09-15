use gzset::ScoreSet;
use std::os::raw::c_void;

extern "C" {
    fn gzset_mem_usage(value: *const c_void) -> usize;
}

#[test]
fn mem_bytes_tracking() {
    let mut set = Box::new(ScoreSet::default());
    for i in 0..10 {
        let m = format!("m{i}");
        assert!(set.insert(0.0, &m));
    }

    let before_mem = set.mem_bytes();
    let before_usage = unsafe { gzset_mem_usage((&*set as *const ScoreSet) as *const c_void) };

    for i in 0..6 {
        let m = format!("m{i}");
        assert!(set.remove(&m));
    }

    let after_mem = set.mem_bytes();
    let after_usage = unsafe { gzset_mem_usage((&*set as *const ScoreSet) as *const c_void) };

    assert!(
        after_mem < before_mem,
        "mem_bytes should shrink after removals: before {before_mem} after {after_mem}"
    );
    assert!(
        after_usage < before_usage,
        "usage should shrink after removals: before {before_usage} after {after_usage}"
    );

    for i in 6..10 {
        let m = format!("m{i}");
        assert!(set.remove(&m));
    }
    assert!(set.is_empty());
}
