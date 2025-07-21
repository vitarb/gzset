use crate::score_set::ScoreSet;
use crate::sets;
use std::collections::BTreeSet;
use std::mem::size_of;
use std::os::raw::c_void;

#[derive(Debug)]
pub struct ScoreSetRef {
    pub key: String,
}

#[no_mangle]
pub unsafe extern "C" fn gzset_free(value: *mut c_void) {
    if !value.is_null() {
        let key_ref = Box::from_raw(value as *mut ScoreSetRef);
        sets::remove(&key_ref.key);
    }
}

/// Approximate heap usage of a ScoreSet.
fn estimate_score_set_usage(set: &ScoreSet) -> usize {
    let mut total = size_of::<ScoreSet>();

    // BTreeMap nodes (by_score) + each BTreeSet structure
    total += set.by_score.len() * size_of::<(ordered_float::OrderedFloat<f64>, BTreeSet<String>)>();

    for bset in set.by_score.values() {
        total += size_of::<BTreeSet<String>>();
        total += bset.len() * size_of::<String>();
        for m in bset {
            total += m.capacity();
        }
    }

    // FxHashMap buckets (members)
    total += set.members.capacity() * size_of::<(String, ordered_float::OrderedFloat<f64>)>();
    for m in set.members.keys() {
        total += m.capacity();
    }

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
