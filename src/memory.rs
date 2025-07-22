use crate::score_set::ScoreSet;
use crate::sets;
use std::mem::size_of;
use std::os::raw::c_void;

#[derive(Debug)]
pub struct ScoreSetRef {
    pub key: String,
}

#[no_mangle]
pub unsafe extern "C" fn gzset_free(value: *mut c_void) {
    if value.is_null() {
        return;
    }
    let key_ref = Box::from_raw(value as *mut ScoreSetRef);
    let key = key_ref.key.clone();
    std::thread::spawn(move || {
        sets::with_write(None, &key, |set| {
            set.members.clear();
            set.by_score.clear();
        });
    });
    drop(key_ref);
}

/// Approximate heap usage of a ScoreSet.
fn estimate_score_set_usage(set: &ScoreSet) -> usize {
    let mut total = size_of::<ScoreSet>();

    // Base storage for members map
    total += set.members.len() * size_of::<(String, ordered_float::OrderedFloat<f64>)>();
    for m in set.members.keys() {
        total += m.len();
    }

    // Simple overhead allowance per entry
    total += set.members.len() * 14;

    total
}

/// `MEMORY USAGE` callback for gzset keys.
#[no_mangle]
pub unsafe extern "C" fn gzset_mem_usage(value: *const c_void) -> usize {
    std::panic::catch_unwind(|| {
        if value.is_null() {
            return 0usize;
        }
        let key_ref = &*(value as *const ScoreSetRef);
        sets::with_read(&key_ref.key, estimate_score_set_usage)
    })
    .unwrap_or(0)
}
