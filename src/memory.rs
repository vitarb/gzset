use crate::score_set::ScoreSet;
use redis_module::raw::RedisModule_MallocSize;
use std::mem::size_of;
use std::os::raw::c_void;

const BTREE_NODE_CAP: usize = 11;
const BTREE_NODE_HDR: usize = 48; // matches score_set.rs approximation

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

    // tracked by ScoreSet::mem_bytes (buckets, member table, by_score BTreeMap)
    total += set.mem_bytes();

    // Add the auxiliary by_score_sizes BTreeMap overhead (keys + values).
    // We mirror the approximation used in score_set.rs.
    let sizes_nodes = btree_nodes(set.by_score_sizes.len());
    if sizes_nodes > 0 {
        let node_bytes = BTREE_NODE_HDR
            + BTREE_NODE_CAP * (size_of::<ordered_float::OrderedFloat<f64>>() + size_of::<usize>());
        total += sizes_nodes * size_class(node_bytes);
    }

    let table = set.pool.map.raw_table();
    if table.buckets() > 0 {
        let (ptr, layout) = table.allocation_info();
        let table_bytes = ms(ptr.as_ptr().cast());
        if table_bytes > 0 {
            total += table_bytes;
        } else {
            total += size_class(layout.size());
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
