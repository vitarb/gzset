use crate::score_set::ScoreSet;
use ordered_float::OrderedFloat;
use redis_module::raw::RedisModule_MallocSize;
use std::collections::BTreeSet;
use std::mem::size_of;
use std::os::raw::c_void;

const BTREE_NODE_CAP: usize = 11;
const BTREE_NODE_HDR: usize = 48;
const EXTRA_PER_ELEM: usize = 24;

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
fn map_node_bytes<K, V>() -> usize {
    BTREE_NODE_HDR + BTREE_NODE_CAP * (size_of::<K>() + size_of::<V>())
}

#[inline]
fn set_node_bytes<K>() -> usize {
    BTREE_NODE_HDR + BTREE_NODE_CAP * size_of::<K>()
}

#[inline]
unsafe fn ms(ptr: *const c_void) -> usize {
    RedisModule_MallocSize.unwrap()(ptr as *mut _)
}

#[no_mangle]
pub unsafe extern "C" fn gzset_free(value: *mut c_void) {
    if !value.is_null() {
        drop(Box::from_raw(value as *mut ScoreSet));
    }
}

unsafe fn heap_size_of_score_set(set: &ScoreSet) -> usize {
    let mut total = ms(set as *const _ as *const _);

    let table = set.members.raw_table();
    if table.capacity() > 0 {
        let (ptr, _) = table.allocation_info();
        total += ms(ptr.as_ptr().cast());
        let buckets = table.buckets();
        total += size_class(16 + buckets);
    }

    for s in set.members.keys() {
        total += ms(s.as_ptr() as *const _);
    }

    for bucket in set.by_score.values() {
        for s in bucket {
            total += ms(s.as_ptr() as *const _);
        }
    }

    total += EXTRA_PER_ELEM * set.members.len();

    let map_nodes = btree_nodes(set.by_score.len());
    total += map_nodes * size_class(map_node_bytes::<OrderedFloat<f64>, BTreeSet<String>>());
    let internal_nodes = map_nodes.saturating_sub(1);
    if internal_nodes > 0 {
        total += internal_nodes * size_class((BTREE_NODE_CAP + 1) * size_of::<*const ()>());
    }
    for bucket in set.by_score.values() {
        total += btree_nodes(bucket.len()) * size_class(set_node_bytes::<String>());
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
