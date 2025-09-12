// SPDX-License-Identifier: MIT OR Apache-2.0
//
// This file is based on components from the `hashbrown` crate.
// The `hashbrown` crate is dual licensed under Apache-2.0 or MIT.
// This file may not be copied, modified, or distributed except
// according to those terms.

use hashbrown::raw::RawTable;
// MemberId is an internal u32 index; using FxHasher here is safe and fast.
use rustc_hash::FxHasher;
use std::hash::{BuildHasher, BuildHasherDefault};

use crate::pool::MemberId;

type BuildFxHasher = BuildHasherDefault<FxHasher>;

/// O(1) insert/update, remove, lookup. No hashing of the value.
pub struct CompactTable {
    table: RawTable<(MemberId, f64)>,
}

impl CompactTable {
    #[inline]
    pub fn new() -> Self {
        Self {
            table: RawTable::with_capacity(0),
        }
    }

    #[inline]
    fn hash(id: MemberId) -> u64 {
        // MemberId is not attacker-controlled; FxHasher is acceptable.
        BuildFxHasher::default().hash_one(id)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.table.len()
    }

    #[inline]
    pub fn insert(&mut self, id: MemberId, score: f64) -> bool {
        let hash = Self::hash(id);
        if let Some((_, s)) = self.table.get_mut(hash, |(k, _)| *k == id) {
            *s = score;
            false
        } else {
            self.table
                .insert(hash, (id, score), |(k, _)| Self::hash(*k));
            true
        }
    }

    #[inline]
    pub fn remove(&mut self, id: MemberId) -> bool {
        let hash = Self::hash(id);
        self.table.remove_entry(hash, |(k, _)| *k == id).is_some()
    }

    #[inline]
    pub fn get(&self, id: MemberId) -> Option<f64> {
        let hash = Self::hash(id);
        self.table.get(hash, |(k, _)| *k == id).map(|(_, s)| *s)
    }

    #[inline]
    #[allow(dead_code)]
    pub fn iter(&self) -> impl Iterator<Item = (MemberId, f64)> + '_ {
        // SAFETY: all elements in the table are initialized
        unsafe { self.table.iter() }.map(|bucket| {
            // SAFETY: buckets returned by `iter` contain valid elements
            let &(id, score) = unsafe { bucket.as_ref() };
            (id, score)
        })
    }

    #[inline]
    pub(crate) fn raw_table(&self) -> &RawTable<(MemberId, f64)> {
        &self.table
    }

    #[cfg(test)]
    #[inline]
    pub fn shrink_to_fit(&mut self) {
        self.table.shrink_to(0, |(k, _)| Self::hash(*k));
    }
}

impl Default for CompactTable {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bucket(hash: u64, buckets: usize) -> usize {
        hash as usize & (buckets - 1)
    }

    #[test]
    fn collision_handling() {
        let mut t = CompactTable::new();
        assert!(t.insert(1, 1.0));
        let buckets = t.raw_table().buckets();
        let target_bucket = bucket(CompactTable::hash(1), buckets);
        let mut other = 2u32;
        while bucket(CompactTable::hash(other), buckets) != target_bucket {
            other += 1;
        }
        assert!(t.insert(other, 2.0));
        assert_eq!(t.len(), 2);
        assert_eq!(t.get(1), Some(1.0));
        assert_eq!(t.get(other), Some(2.0));
    }

    #[test]
    fn remove_reinsert_recycling() {
        let mut t = CompactTable::new();
        assert!(t.insert(1, 1.0));
        assert!(t.remove(1));
        assert_eq!(t.len(), 0);
        assert!(t.insert(1, 2.0));
        assert_eq!(t.get(1), Some(2.0));
    }

    #[test]
    fn resize_grow_shrink() {
        let mut t = CompactTable::new();
        for i in 0..1000u32 {
            assert!(t.insert(i, i as f64));
        }
        assert_eq!(t.len(), 1000);
        for i in 0..1000u32 {
            assert_eq!(t.get(i), Some(i as f64));
        }
        for i in 0..1000u32 {
            assert!(t.remove(i));
        }
        assert_eq!(t.len(), 0);
        t.shrink_to_fit();
        for i in 0..100u32 {
            assert!(t.insert(i, i as f64));
        }
        for i in 0..100u32 {
            assert_eq!(t.get(i), Some(i as f64));
        }
    }

    #[test]
    fn iterator_returns_all() {
        let mut t = CompactTable::new();
        for i in 0..10u32 {
            assert!(t.insert(i, i as f64));
        }
        let mut items: Vec<_> = t.iter().collect();
        items.sort_by_key(|(id, _)| *id);
        assert_eq!(items.len(), 10);
        for (id, score) in items {
            assert_eq!(score, id as f64);
        }
    }
}
