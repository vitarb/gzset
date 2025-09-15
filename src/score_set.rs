use ordered_float::OrderedFloat;
use smallvec::SmallVec;
use std::{collections::BTreeMap, convert::TryFrom, mem::size_of};

use crate::{
    compact_table::CompactTable,
    pool::{MemberId, StringPool},
};

type Bucket = SmallVec<[MemberId; 4]>;

/// Buckets shrink back to inline storage once they contain at most this many members.
const BUCKET_SHRINK_THRESHOLD: usize = 4;

const BTREE_NODE_CAP: usize = 11;
const BTREE_NODE_HDR: usize = 48;

#[inline]
const fn size_class(bytes: usize) -> usize {
    if bytes <= 512 {
        (bytes + 7) & !7
    } else {
        bytes.next_power_of_two()
    }
}

#[derive(Default)]
pub struct ScoreSet {
    pub(crate) by_score: BTreeMap<OrderedFloat<f64>, Bucket>,
    pub(crate) by_score_sizes: BTreeMap<OrderedFloat<f64>, usize>,
    pub(crate) members: CompactTable,
    pub(crate) pool: StringPool,
    mem_bytes: usize,
    #[cfg(test)]
    mem_breakdown: MemBreakdown,
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default)]
pub struct MemBreakdown {
    pub score_map: usize,
    pub buckets: usize,
    pub member_table: usize,
    pub strings: usize,
}

#[cfg(test)]
impl MemBreakdown {
    #[inline]
    pub fn structural(&self) -> usize {
        self.score_map + self.buckets + self.member_table
    }

    #[inline]
    pub fn total(&self) -> usize {
        self.structural() + self.strings
    }
}

#[derive(Clone, Debug)]
pub struct ScoreIter<'a> {
    pool: &'a StringPool,
    front_outer: std::collections::btree_map::Iter<'a, OrderedFloat<f64>, Bucket>,
    front_current: Option<(std::slice::Iter<'a, MemberId>, OrderedFloat<f64>)>,
    back_outer: std::iter::Rev<std::collections::btree_map::Iter<'a, OrderedFloat<f64>, Bucket>>,
    back_current: Option<(
        std::iter::Rev<std::slice::Iter<'a, MemberId>>,
        OrderedFloat<f64>,
    )>,
    remaining_front_skip: usize,
    remaining_back_skip: usize,
    yielded_front: usize,
    yielded_back: usize,
    total: usize,
}

impl<'a> ScoreIter<'a> {
    fn new(
        map: &'a BTreeMap<OrderedFloat<f64>, Bucket>,
        pool: &'a StringPool,
        start: usize,
        stop: usize,
        len: usize,
    ) -> Self {
        Self {
            pool,
            front_outer: map.iter(),
            front_current: None,
            back_outer: map.iter().rev(),
            back_current: None,
            remaining_front_skip: start,
            remaining_back_skip: len - 1 - stop,
            yielded_front: 0,
            yielded_back: 0,
            total: stop - start + 1,
        }
    }

    fn empty(map: &'a BTreeMap<OrderedFloat<f64>, Bucket>, pool: &'a StringPool) -> Self {
        Self {
            pool,
            front_outer: map.iter(),
            front_current: None,
            back_outer: map.iter().rev(),
            back_current: None,
            remaining_front_skip: 0,
            remaining_back_skip: 0,
            yielded_front: 0,
            yielded_back: 0,
            total: 0,
        }
    }

    #[inline]
    fn remaining(&self) -> usize {
        self.total - self.yielded_front - self.yielded_back
    }
}

impl<'a> Iterator for ScoreIter<'a> {
    type Item = (&'a str, f64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining() == 0 {
            return None;
        }
        loop {
            if let Some((ref mut iter, score)) = self.front_current {
                for id in iter.by_ref() {
                    if self.remaining_front_skip > 0 {
                        self.remaining_front_skip -= 1;
                        continue;
                    }
                    self.yielded_front += 1;
                    let member = self.pool.get(*id);
                    return Some((member, score.0));
                }
                self.front_current = None;
            }
            match self.front_outer.next() {
                Some((score, bucket)) => {
                    self.front_current = Some((bucket.iter(), *score));
                }
                None => return None,
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.remaining();
        (rem, Some(rem))
    }
}

impl<'a> DoubleEndedIterator for ScoreIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.remaining() == 0 {
            return None;
        }
        loop {
            if let Some((ref mut iter, score)) = self.back_current {
                for id in iter.by_ref() {
                    if self.remaining_back_skip > 0 {
                        self.remaining_back_skip -= 1;
                        continue;
                    }
                    self.yielded_back += 1;
                    let member = self.pool.get(*id);
                    return Some((member, score.0));
                }
                self.back_current = None;
            }
            match self.back_outer.next() {
                Some((score, bucket)) => {
                    self.back_current = Some((bucket.iter().rev(), *score));
                }
                None => return None,
            }
        }
    }
}

impl<'a> ExactSizeIterator for ScoreIter<'a> {
    fn len(&self) -> usize {
        self.remaining()
    }
}

impl ScoreSet {
    #[inline]
    pub fn mem_bytes(&self) -> usize {
        self.mem_bytes
    }

    #[cfg(test)]
    #[inline]
    pub fn debug_mem_breakdown(&self) -> MemBreakdown {
        self.mem_breakdown
    }

    #[inline]
    fn member_table_bytes(table: &CompactTable) -> usize {
        let raw = table.raw_table();
        if raw.capacity() == 0 {
            0
        } else {
            let (_, layout) = raw.allocation_info();
            layout.size() + size_class(16 + raw.buckets())
        }
    }

    #[inline]
    fn score_map_bytes(map: &BTreeMap<OrderedFloat<f64>, Bucket>) -> usize {
        if map.is_empty() {
            0
        } else {
            Self::btree_nodes(map.len())
                * size_class(Self::map_node_bytes::<OrderedFloat<f64>, Bucket>())
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
    fn maybe_shrink_bucket(bucket: &mut Bucket) -> isize {
        if bucket.spilled() && bucket.len() <= BUCKET_SHRINK_THRESHOLD {
            let bytes = bucket.capacity() * size_of::<MemberId>();
            bucket.shrink_to_fit();
            let bytes = isize::try_from(bytes).expect("bucket shrink delta overflow");
            -bytes
        } else {
            0
        }
    }

    #[inline]
    fn apply_bucket_mem_delta(&mut self, delta: isize) {
        if delta == 0 {
            return;
        }
        if delta > 0 {
            let bytes = delta as usize;
            self.mem_bytes += bytes;
            #[cfg(test)]
            {
                self.mem_breakdown.buckets += bytes;
            }
        } else {
            let bytes = (-delta) as usize;
            self.mem_bytes -= bytes;
            #[cfg(test)]
            {
                self.mem_breakdown.buckets -= bytes;
            }
        }
    }

    pub fn insert(&mut self, score: f64, member: &str) -> bool {
        let key = OrderedFloat(score);
        let is_new = self.pool.lookup(member).is_none();
        let prev_table = Self::member_table_bytes(&self.members);
        let prev_map = Self::score_map_bytes(&self.by_score);
        let id = self.pool.intern(member);
        let mut bucket_delta: isize = 0;
        let inserted = {
            let name = self.pool.get(id);
            let old = self.members.get(id);
            if !self.members.insert(id, score) {
                if let Some(old_score) = old {
                    let old_key = OrderedFloat(old_score);
                    if old_key == key {
                        return false;
                    }
                    if let Some(bucket) = self.by_score.get_mut(&old_key) {
                        if let Ok(pos) = bucket.binary_search_by(|&m| self.pool.get(m).cmp(name)) {
                            bucket.remove(pos);
                        }
                        if let Some(sz) = self.by_score_sizes.get_mut(&old_key) {
                            *sz -= 1;
                            if *sz == 0 {
                                self.by_score_sizes.remove(&old_key);
                            }
                        }
                        if bucket.is_empty() {
                            self.by_score.remove(&old_key);
                        } else {
                            bucket_delta = Self::maybe_shrink_bucket(bucket);
                        }
                    }
                }
            }
            let new_table = Self::member_table_bytes(&self.members);
            self.mem_bytes += new_table - prev_table;
            #[cfg(test)]
            {
                self.mem_breakdown.member_table += new_table - prev_table;
            }
            if is_new {
                #[cfg(test)]
                {
                    self.mem_breakdown.strings += member.len();
                }
            }
            let bucket = self.by_score.entry(key).or_default();
            let spilled_before = bucket.spilled();
            match bucket.binary_search_by(|&m| self.pool.get(m).cmp(name)) {
                Ok(_) => false,
                Err(pos) => {
                    bucket.insert(pos, id);
                    *self.by_score_sizes.entry(key).or_insert(0) += 1;
                    if !spilled_before && bucket.spilled() {
                        let bytes = bucket.capacity() * size_of::<MemberId>();
                        self.mem_bytes += bytes;
                        #[cfg(test)]
                        {
                            self.mem_breakdown.buckets += bytes;
                        }
                    }
                    let new_map = Self::score_map_bytes(&self.by_score);
                    if new_map >= prev_map {
                        let delta = new_map - prev_map;
                        self.mem_bytes += delta;
                        #[cfg(test)]
                        {
                            self.mem_breakdown.score_map += delta;
                        }
                    } else {
                        let delta = prev_map - new_map;
                        self.mem_bytes -= delta;
                        #[cfg(test)]
                        {
                            self.mem_breakdown.score_map -= delta;
                        }
                    }
                    true
                }
            }
        };
        if bucket_delta != 0 {
            self.apply_bucket_mem_delta(bucket_delta);
        }
        inserted
    }

    pub fn remove(&mut self, member: &str) -> bool {
        let id = match self.pool.lookup(member) {
            Some(id) => id,
            None => return false,
        };
        let score = match self.members.get(id) {
            Some(s) => OrderedFloat(s),
            None => return false,
        };
        let prev_table = Self::member_table_bytes(&self.members);
        let prev_map = Self::score_map_bytes(&self.by_score);
        if self.members.remove(id) {
            let mut bucket_delta: isize = 0;
            if let Some(bucket) = self.by_score.get_mut(&score) {
                if let Ok(pos) = bucket.binary_search_by(|&m| self.pool.get(m).cmp(member)) {
                    bucket.remove(pos);
                }
                if let Some(sz) = self.by_score_sizes.get_mut(&score) {
                    *sz -= 1;
                    if *sz == 0 {
                        self.by_score_sizes.remove(&score);
                    }
                }
                if bucket.is_empty() {
                    self.by_score.remove(&score);
                } else {
                    bucket_delta = Self::maybe_shrink_bucket(bucket);
                }
            }
            if bucket_delta != 0 {
                self.apply_bucket_mem_delta(bucket_delta);
            }
            let new_table = Self::member_table_bytes(&self.members);
            if new_table >= prev_table {
                let delta = new_table - prev_table;
                self.mem_bytes += delta;
                #[cfg(test)]
                {
                    self.mem_breakdown.member_table += delta;
                }
            } else {
                let delta = prev_table - new_table;
                self.mem_bytes -= delta;
                #[cfg(test)]
                {
                    self.mem_breakdown.member_table -= delta;
                }
            }
            let new_map = Self::score_map_bytes(&self.by_score);
            if new_map >= prev_map {
                let delta = new_map - prev_map;
                self.mem_bytes += delta;
                #[cfg(test)]
                {
                    self.mem_breakdown.score_map += delta;
                }
            } else {
                let delta = prev_map - new_map;
                self.mem_bytes -= delta;
                #[cfg(test)]
                {
                    self.mem_breakdown.score_map -= delta;
                }
            }
            if self.pool.remove(member).is_some() {
                #[cfg(test)]
                {
                    self.mem_breakdown.strings -= member.len();
                }
            }
            true
        } else {
            false
        }
    }

    pub fn score(&self, member: &str) -> Option<f64> {
        let id = self.pool.lookup(member)?;
        self.members.get(id)
    }

    pub fn rank(&self, member: &str) -> Option<usize> {
        let id = self.pool.lookup(member)?;
        let score_key = OrderedFloat(self.members.get(id)?);
        let bucket = self.by_score.get(&score_key)?;
        let pos = bucket
            .binary_search_by(|&m| self.pool.get(m).cmp(member))
            .ok()?;
        let mut idx = 0usize;
        for (_, sz) in self.by_score_sizes.range(..score_key) {
            idx += *sz;
        }
        Some(idx + pos)
    }

    pub fn select_by_rank(&self, mut r: usize) -> (&str, f64) {
        for (score, bucket) in &self.by_score {
            if r < bucket.len() {
                let id = bucket[r];
                return (self.pool.get(id), score.0);
            }
            r -= bucket.len();
        }
        unreachable!("rank out of bounds");
    }

    pub fn iter_range(&self, start: isize, stop: isize) -> ScoreIter<'_> {
        let len = self.members.len() as isize;
        if len == 0 {
            return ScoreIter::empty(&self.by_score, &self.pool);
        }
        let mut start = if start < 0 { len + start } else { start };
        let mut stop = if stop < 0 { len + stop } else { stop };
        if start < 0 {
            start = 0;
        }
        if stop < 0 {
            return ScoreIter::empty(&self.by_score, &self.pool);
        }
        if stop >= len {
            stop = len - 1;
        }
        if start > stop {
            return ScoreIter::empty(&self.by_score, &self.pool);
        }
        ScoreIter::new(
            &self.by_score,
            &self.pool,
            start as usize,
            stop as usize,
            len as usize,
        )
    }

    pub fn range_iter(&self, start: isize, stop: isize) -> Vec<(f64, String)> {
        self.iter_range(start, stop)
            .map(|(m, s)| (s, m.to_owned()))
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.members.len() == 0
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn iter_all(&self) -> impl Iterator<Item = (&str, f64)> + '_ {
        let pool = &self.pool;
        self.by_score
            .iter()
            .flat_map(move |(score, bucket)| bucket.iter().map(move |id| (pool.get(*id), score.0)))
    }

    pub fn iter_from<'a>(
        &'a self,
        score: OrderedFloat<f64>,
        member: &'a str,
        exclusive: bool,
    ) -> impl Iterator<Item = (&'a str, f64)> + 'a {
        use std::cell::Cell;
        let pool = &self.pool;
        let first = Cell::new(true);
        self.by_score.range(score..).flat_map(move |(s, bucket)| {
            let start_idx = if first.get() {
                first.set(false);
                if *s == score {
                    match bucket.binary_search_by(|&m| pool.get(m).cmp(member)) {
                        Ok(pos) => {
                            if exclusive {
                                pos + 1
                            } else {
                                pos
                            }
                        }
                        Err(pos) => pos,
                    }
                } else {
                    0
                }
            } else {
                0
            };
            bucket[start_idx..]
                .iter()
                .map(move |id| (pool.get(*id), s.0))
        })
    }

    #[cfg(any(test, feature = "bench"))]
    pub fn all_items(&self) -> Vec<(f64, String)> {
        let mut out = Vec::new();
        for (score, bucket) in &self.by_score {
            for id in bucket {
                out.push((score.0, self.pool.get(*id).to_owned()));
            }
        }
        out
    }

    #[cfg(any(test, feature = "bench"))]
    pub fn member_names(&self) -> Vec<String> {
        self.members
            .iter()
            .map(|(id, _)| self.pool.get(id).to_owned())
            .collect()
    }

    #[cfg(any(test, feature = "bench"))]
    pub fn members_with_scores(&self) -> Vec<(String, f64)> {
        self.members
            .iter()
            .map(|(id, sc)| (self.pool.get(id).to_owned(), sc))
            .collect()
    }

    pub fn contains(&self, member: &str) -> bool {
        self.pool
            .lookup(member)
            .is_some_and(|id| self.members.get(id).is_some())
    }

    pub fn pop_one(&mut self, min: bool) -> Option<(String, f64)> {
        let prev_map = Self::score_map_bytes(&self.by_score);
        let (score_key, id, shrink_delta) = {
            let mut entry = if min {
                self.by_score.first_entry()?
            } else {
                self.by_score.last_entry()?
            };
            let score_key = *entry.key();
            let bucket = entry.get_mut();
            let id = if min {
                bucket.remove(0)
            } else {
                bucket.pop().expect("bucket must contain member")
            };
            let shrink_delta = if bucket.is_empty() {
                entry.remove_entry();
                0
            } else {
                Self::maybe_shrink_bucket(bucket)
            };
            (score_key, id, shrink_delta)
        };

        self.apply_bucket_mem_delta(shrink_delta);

        if let Some(sz) = self.by_score_sizes.get_mut(&score_key) {
            *sz -= 1;
            if *sz == 0 {
                self.by_score_sizes.remove(&score_key);
            }
        }

        let prev_table = Self::member_table_bytes(&self.members);
        let removed = self.members.remove(id);
        debug_assert!(removed, "member removed from score map must exist in table");
        let new_table = Self::member_table_bytes(&self.members);
        if new_table >= prev_table {
            let delta = new_table - prev_table;
            self.mem_bytes += delta;
            #[cfg(test)]
            {
                self.mem_breakdown.member_table += delta;
            }
        } else {
            let delta = prev_table - new_table;
            self.mem_bytes -= delta;
            #[cfg(test)]
            {
                self.mem_breakdown.member_table -= delta;
            }
        }

        let name = self.pool.get(id).to_owned();
        if self.pool.remove(&name).is_some() {
            #[cfg(test)]
            {
                self.mem_breakdown.strings -= name.len();
            }
        }

        let new_map = Self::score_map_bytes(&self.by_score);
        if new_map >= prev_map {
            let delta = new_map - prev_map;
            self.mem_bytes += delta;
            #[cfg(test)]
            {
                self.mem_breakdown.score_map += delta;
            }
        } else {
            let delta = prev_map - new_map;
            self.mem_bytes -= delta;
            #[cfg(test)]
            {
                self.mem_breakdown.score_map -= delta;
            }
        }

        Some((name, score_key.0))
    }

    pub fn pop_n(&mut self, min: bool, n: usize) -> Vec<(String, f64)> {
        let mut out = Vec::with_capacity(n.min(self.len()));
        for _ in 0..n {
            match self.pop_one(min) {
                Some(item) => out.push(item),
                None => break,
            }
        }
        out
    }

    #[doc(hidden)]
    pub fn bucket_capacity_for_test(&self, score: f64) -> Option<usize> {
        self.by_score
            .get(&OrderedFloat(score))
            .map(|b| b.capacity())
    }

    #[cfg(any(test, feature = "bench"))]
    pub fn pop_all(&mut self, min: bool) -> Vec<String> {
        let total = self.len();
        self.pop_n(min, total)
            .into_iter()
            .map(|(member, _)| member)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::gzset_mem_usage;
    use redis_module::raw::RedisModule_MallocSize;
    use std::os::raw::c_void;

    #[inline]
    unsafe fn ms(ptr: *const c_void) -> usize {
        if let Some(f) = RedisModule_MallocSize {
            f(ptr as *mut _)
        } else {
            0
        }
    }

    unsafe fn expected_usage(set: &ScoreSet) -> usize {
        let mut total = ms(set as *const _ as *const _);
        let breakdown = set.debug_mem_breakdown();
        debug_assert_eq!(set.mem_bytes(), breakdown.structural());
        total += breakdown.structural();

        let sizes_nodes = ScoreSet::btree_nodes(set.by_score_sizes.len());
        if sizes_nodes > 0 {
            total +=
                sizes_nodes * size_class(ScoreSet::map_node_bytes::<OrderedFloat<f64>, usize>());
        }

        #[cfg(feature = "fast-hash")]
        {
            let table = set.pool.map.raw_table();
            if table.capacity() > 0 {
                let (ptr, _) = table.allocation_info();
                total += ms(ptr.as_ptr().cast());
                total += size_class(16 + table.buckets());
            }
        }
        #[cfg(not(feature = "fast-hash"))]
        {
            if set.pool.map.capacity() > 0 {
                total += size_class(16 + set.pool.map.capacity());
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

    #[test]
    fn mem_usage_matches_breakdown() {
        let mut set = Box::new(ScoreSet::default());
        for i in 0..5 {
            assert!(set.insert(0.0, &format!("m{i}")));
        }
        for i in 5..105 {
            assert!(set.insert(i as f64, &format!("m{i}")));
        }
        unsafe {
            let usage = gzset_mem_usage((&*set as *const ScoreSet) as *const c_void);
            let breakdown = expected_usage(set.as_ref());
            let diff = usage as isize - breakdown as isize;
            assert!(diff.abs() < 1024, "usage {usage} breakdown {breakdown}");
        }
        for i in 5..105 {
            assert!(set.remove(&format!("m{i}")));
        }
        assert!(set.remove("m0"));
        unsafe {
            let usage = gzset_mem_usage((&*set as *const ScoreSet) as *const c_void);
            let breakdown = expected_usage(set.as_ref());
            let diff = usage as isize - breakdown as isize;
            assert!(diff.abs() < 1024, "usage {usage} breakdown {breakdown}");
        }
        for i in 1..5 {
            assert!(set.remove(&format!("m{i}")));
        }
        unsafe {
            let usage = gzset_mem_usage((&*set as *const ScoreSet) as *const c_void);
            let breakdown = expected_usage(set.as_ref());
            let diff = usage as isize - breakdown as isize;
            assert!(diff.abs() < 1024, "usage {usage} breakdown {breakdown}");
        }
    }

    #[test]
    fn pop_updates_internal_state() {
        let mut set = Box::new(ScoreSet::default());
        let items = [
            (1.0, "a1"),
            (1.0, "a2"),
            (1.0, "a3"),
            (1.0, "a4"),
            (1.0, "a5"),
            (2.0, "b1"),
            (2.0, "b2"),
            (3.0, "c1"),
            (4.0, "d1"),
        ];
        for (score, member) in items {
            assert!(set.insert(score, member));
        }
        let initial_len = set.len();
        let initial_mem = set.mem_bytes();
        let initial_usage = unsafe { gzset_mem_usage((&*set as *const ScoreSet) as *const c_void) };

        let popped = set.pop_n(true, 3);
        assert_eq!(popped.len(), 3);
        assert_eq!(
            popped,
            vec![
                ("a1".to_string(), 1.0),
                ("a2".to_string(), 1.0),
                ("a3".to_string(), 1.0)
            ]
        );
        assert_eq!(set.len(), initial_len - popped.len());
        assert!(set.mem_bytes() < initial_mem);
        let usage_after = unsafe { gzset_mem_usage((&*set as *const ScoreSet) as *const c_void) };
        let has_malloc = unsafe { RedisModule_MallocSize }.is_some();
        if has_malloc {
            assert!(
                usage_after < initial_usage,
                "usage {usage_after} initial {initial_usage}"
            );
        }

        let set_ref = set.as_ref();
        for (score, bucket) in &set_ref.by_score {
            let sz = set_ref
                .by_score_sizes
                .get(score)
                .copied()
                .unwrap_or_default();
            assert_eq!(bucket.len(), sz, "score {score:?}");
        }

        let remaining = set.range_iter(0, -1);
        for (idx, (_, member)) in remaining.iter().enumerate() {
            assert_eq!(set.rank(member), Some(idx));
        }

        while set.pop_one(true).is_some() {}
        assert!(set.is_empty());
        let set_ref = set.as_ref();
        assert!(set_ref.by_score.is_empty());
        assert!(set_ref.by_score_sizes.is_empty());
    }

    fn bucket_shrink_mem_on_pop(min: bool) {
        let mut set = ScoreSet::default();
        let total = super::BUCKET_SHRINK_THRESHOLD * 2;
        for i in 0..total {
            let member = format!("m{i}");
            assert!(set.insert(1.0, &member));
        }
        let initial_cap = set
            .bucket_capacity_for_test(1.0)
            .expect("bucket should exist");
        assert!(
            initial_cap > super::BUCKET_SHRINK_THRESHOLD,
            "expected spill before pops"
        );

        let before_mem = set.mem_bytes();
        let before_buckets = set.debug_mem_breakdown().buckets;
        assert!(before_buckets > 0, "bucket accounting should reflect spill");

        for _ in 0..super::BUCKET_SHRINK_THRESHOLD {
            assert!(set.pop_one(min).is_some());
        }

        assert_eq!(set.len(), super::BUCKET_SHRINK_THRESHOLD);

        let after_mem = set.mem_bytes();
        let after_buckets = set.debug_mem_breakdown().buckets;
        assert!(
            after_mem < before_mem,
            "mem_bytes should shrink: before {before_mem} after {after_mem}"
        );
        assert!(
            after_buckets < before_buckets,
            "bucket breakdown should shrink: before {before_buckets} after {after_buckets}"
        );
        assert_eq!(after_buckets, 0, "bucket accounting should return inline");
        assert_eq!(
            set.bucket_capacity_for_test(1.0),
            Some(super::BUCKET_SHRINK_THRESHOLD)
        );
    }

    #[test]
    fn bucket_shrink_updates_mem_on_min_pop() {
        bucket_shrink_mem_on_pop(true);
    }

    #[test]
    fn bucket_shrink_updates_mem_on_max_pop() {
        bucket_shrink_mem_on_pop(false);
    }
}
