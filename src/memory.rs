use crate::score_set::ScoreSet;
use redis_module::raw::RedisModule_MallocSize;
use std::mem::size_of;
use std::os::raw::c_void;

const BTREE_NODE_CAP: usize = 11;

#[inline]
const fn size_class(bytes: usize) -> usize {
    if bytes <= 512 {
        (bytes + 7) & !7
    } else {
        bytes.next_power_of_two()
    }
}

#[inline]
fn btree_nodes(elem: usize) -> usize {
    elem.div_ceil(BTREE_NODE_CAP)
}

#[inline]
unsafe fn ms(ptr: *const c_void) -> usize {
    if let Some(f) = RedisModule_MallocSize {
        f(ptr as *mut _)
    } else {
        0
    }
}

#[no_mangle]
pub unsafe extern "C" fn gzset_free(value: *mut c_void) {
    if !value.is_null() {
        drop(Box::from_raw(value as *mut ScoreSet));
    }
}

unsafe fn heap_size_of_score_set(set: &ScoreSet) -> usize {
    let mut total = ms(set as *const _ as *const _);

    total += set.mem_bytes();

    // The layout of internal BTreeMap nodes is not public, so we approximate their
    // overhead assuming each internal node stores `BTREE_NODE_CAP + 1` child pointers.
    let internal_nodes = btree_nodes(set.by_score.len()).saturating_sub(1);
    if internal_nodes > 0 {
        total += internal_nodes * size_class((BTREE_NODE_CAP + 1) * size_of::<*const ()>());
    }

    #[cfg(feature = "fast-hash")]
    {
        let table = set.pool.map.raw_table();
        if table.capacity() > 0 {
            let (ptr, _) = table.allocation_info();
            total += ms(ptr.as_ptr().cast());
            let buckets = table.buckets();
            total += size_class(16 + buckets);
        }
    }
    #[cfg(not(feature = "fast-hash"))]
    {
        if set.pool.map.capacity() > 0 {
            let buckets = set.pool.map.capacity();
            total += size_class(16 + buckets);
        }
    }
    for key in set.pool.map.keys() {
        total += ms(key.as_ptr().cast());
    }
    if set.pool.strings.capacity() > 0 {
        total += ms(set.pool.strings.as_ptr() as *const _);
    }
    if set.pool.free_ids.capacity() > 0 {
        total += ms(set.pool.free_ids.as_ptr() as *const _);
    }

    total
}

#[no_mangle]
pub unsafe extern "C" fn gzset_mem_usage(value: *const c_void) -> usize {
    if value.is_null() {
        return 0;
    }
    heap_size_of_score_set(&*(value as *const ScoreSet))
}
