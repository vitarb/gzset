use crate::{
    buckets::{Bucket, BucketStore},
    pool::IndexEntry,
    score_set::ScoreSet,
};
use redis_module::raw::RedisModule_MallocSize;
use std::mem::size_of;
use std::os::raw::c_void;

#[inline]
const fn size_class(bytes: usize) -> usize {
    if bytes <= 512 {
        (bytes + 7) & !7
    } else {
        bytes.next_power_of_two()
    }
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

    let table = &set.pool.table;
    if table.buckets() > 0 {
        let (ptr, layout) = table.allocation_info();
        let table_bytes = ms(ptr.as_ptr().cast());
        if table_bytes > 0 {
            total += table_bytes;
        } else {
            total += size_class(layout.size());
        }
    }
    if set.pool.index.capacity() > 0 {
        total += size_class(set.pool.index.capacity() * size_of::<Option<IndexEntry>>());
    }
    if set.pool.free_ids.capacity() > 0 {
        total += size_class(set.pool.free_ids.capacity() * size_of::<crate::pool::MemberId>());
    }
    for chunk in &set.pool.arena {
        let chunk_bytes = ms(chunk.as_ptr() as *const _);
        if chunk_bytes > 0 {
            total += chunk_bytes;
        } else {
            total += size_class(chunk.len());
        }
    }

    // BucketStore containers (not the spilled capacity; that's in set.mem_bytes()).
    let bs: &BucketStore = &set.bucket_store;

    let buckets_cap = bs.buckets.capacity();
    if buckets_cap > 0 {
        let ptr = bs.buckets.as_ptr() as *const c_void;
        let alloc_bytes = ms(ptr);
        if alloc_bytes > 0 {
            total += alloc_bytes;
        } else {
            let elem_size = size_of::<Option<Bucket>>();
            total += size_class(buckets_cap * elem_size);
        }
    }

    let free_cap = bs.free.capacity();
    if free_cap > 0 {
        let ptr = bs.free.as_ptr() as *const c_void;
        let alloc_bytes = ms(ptr);
        if alloc_bytes > 0 {
            total += alloc_bytes;
        } else {
            total += size_class(free_cap * size_of::<crate::buckets::BucketId>());
        }
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
