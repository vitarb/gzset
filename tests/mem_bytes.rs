use gzset::{MemberId, ScoreSet};
use std::mem::size_of;
use std::os::raw::c_void;

extern "C" {
    fn gzset_mem_usage(value: *const c_void) -> usize;
}

const fn size_class(bytes: usize) -> usize {
    if bytes <= 512 {
        (bytes + 7) & !7
    } else {
        bytes.next_power_of_two()
    }
}

#[test]
fn mem_bytes_tracking() {
    const SHRINK_THRESHOLD: usize = 64;
    let total = SHRINK_THRESHOLD * 2;

    let mut set = Box::new(ScoreSet::default());
    for i in 0..total {
        let m = format!("m{i}");
        assert!(set.insert(0.0, &m));
    }

    let before_mem = set.mem_bytes();
    let before_usage = unsafe { gzset_mem_usage((&*set as *const ScoreSet) as *const c_void) };

    let remaining = SHRINK_THRESHOLD / 2;
    for i in 0..(total - remaining) {
        let m = format!("m{i}");
        assert!(set.remove(&m));
    }

    let after_mem = set.mem_bytes();
    let after_usage = unsafe { gzset_mem_usage((&*set as *const ScoreSet) as *const c_void) };

    assert!(
        after_mem < before_mem,
        "mem_bytes should shrink after removals: before {before_mem} after {after_mem}"
    );
    let initial_bytes = total * size_of::<MemberId>();
    let remaining_bytes = remaining * size_of::<MemberId>();
    let allowed_growth = size_class(initial_bytes).saturating_sub(size_class(remaining_bytes));
    assert!(
        after_usage <= before_usage
            || after_usage.saturating_sub(before_usage) <= allowed_growth,
        "usage should not grow significantly after removals: before {before_usage} after {after_usage} (allowed {allowed_growth})",
    );

    for i in (total - remaining)..total {
        let m = format!("m{i}");
        assert!(set.remove(&m));
    }
    assert!(set.is_empty());
}
