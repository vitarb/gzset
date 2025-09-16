use ordered_float::OrderedFloat;
use smallvec::SmallVec;
use std::{collections::BTreeMap, convert::TryFrom, mem::size_of};

use crate::pool::{MemberId, StringPool};

type Bucket = SmallVec<[MemberId; 4]>;

/// Buckets shrink back to inline storage once they contain at most this many members.
const BUCKET_SHRINK_THRESHOLD: usize = 4;

const BTREE_NODE_CAP: usize = 11;
const BTREE_NODE_HDR: usize = 48;

const EMPTY_SCORE: f64 = f64::NAN;

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
    pub(crate) scores: Vec<f64>,
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
    fn get_score_by_id(&self, id: MemberId) -> Option<f64> {
        let idx = id as usize;
        if idx >= self.scores.len() {
            return None;
        }
        let score = self.scores[idx];
        if score.is_nan() {
            None
        } else {
            Some(score)
        }
    }

    #[inline]
    fn scores_bytes(scores: &Vec<f64>) -> usize {
        scores.capacity() * size_of::<f64>()
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
        let prev_scores = Self::scores_bytes(&self.scores);
        let prev_map = Self::score_map_bytes(&self.by_score);
        let id = self.pool.intern(member);
        let idx = id as usize;
        let old_score = self.get_score_by_id(id);
        if self.scores.len() <= idx {
            self.scores.resize(idx + 1, EMPTY_SCORE);
        }
        let new_scores = Self::scores_bytes(&self.scores);
        if new_scores >= prev_scores {
            let delta = new_scores - prev_scores;
            self.mem_bytes += delta;
            #[cfg(test)]
            {
                self.mem_breakdown.member_table += delta;
            }
        } else {
            let delta = prev_scores - new_scores;
            self.mem_bytes -= delta;
            #[cfg(test)]
            {
                self.mem_breakdown.member_table -= delta;
            }
        }
        if is_new {
            #[cfg(test)]
            {
                self.mem_breakdown.strings += member.len();
            }
        }

        let mut bucket_delta: isize = 0;
        let name = self.pool.get(id);
        if let Some(old_score) = old_score {
            let old_key = OrderedFloat(old_score);
            if old_key == key {
                return false;
            }
            if let Some(bucket) = self.by_score.get_mut(&old_key) {
                if let Ok(pos) = bucket.binary_search_by(|&m| self.pool.get(m).cmp(name)) {
                    bucket.remove(pos);
                }
                if bucket.is_empty() {
                    self.by_score.remove(&old_key);
                } else {
                    bucket_delta = Self::maybe_shrink_bucket(bucket);
                }
            }
        }

        self.scores[idx] = score;

        let bucket = self.by_score.entry(key).or_default();
        let spilled_before = bucket.spilled();
        let inserted = match bucket.binary_search_by(|&m| self.pool.get(m).cmp(name)) {
            Ok(_) => false,
            Err(pos) => {
                bucket.insert(pos, id);
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
        let score = match self.get_score_by_id(id) {
            Some(s) => OrderedFloat(s),
            None => return false,
        };
        let prev_scores = Self::scores_bytes(&self.scores);
        let prev_map = Self::score_map_bytes(&self.by_score);
        let mut bucket_delta: isize = 0;
        if let Some(bucket) = self.by_score.get_mut(&score) {
            if let Ok(pos) = bucket.binary_search_by(|&m| self.pool.get(m).cmp(member)) {
                bucket.remove(pos);
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

        let idx = id as usize;
        if idx < self.scores.len() {
            self.scores[idx] = EMPTY_SCORE;
        }

        let new_scores = Self::scores_bytes(&self.scores);
        if new_scores >= prev_scores {
            let delta = new_scores - prev_scores;
            self.mem_bytes += delta;
            #[cfg(test)]
            {
                self.mem_breakdown.member_table += delta;
            }
        } else {
            let delta = prev_scores - new_scores;
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
    }

    pub fn score(&self, member: &str) -> Option<f64> {
        let id = self.pool.lookup(member)?;
        self.get_score_by_id(id)
    }

    pub fn rank(&self, member: &str) -> Option<usize> {
        let id = self.pool.lookup(member)?;
        let score_key = OrderedFloat(self.get_score_by_id(id)?);
        let bucket = self.by_score.get(&score_key)?;
        let pos = bucket
            .binary_search_by(|&m| self.pool.get(m).cmp(member))
            .ok()?;
        let prefix = self
            .by_score
            .range(..score_key)
            .map(|(_, bucket)| bucket.len())
            .sum::<usize>();
        Some(prefix + pos)
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
        let len = self.pool.len() as isize;
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
        self.pool.len() == 0
    }

    pub fn len(&self) -> usize {
        self.pool.len()
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
        self.pool.map.keys().map(|name| name.to_string()).collect()
    }

    #[cfg(any(test, feature = "bench"))]
    pub fn members_with_scores(&self) -> Vec<(String, f64)> {
        let mut out = Vec::new();
        for (name, id) in &self.pool.map {
            if let Some(score) = self.get_score_by_id(*id) {
                out.push((name.to_string(), score));
            }
        }
        out
    }

    pub fn contains(&self, member: &str) -> bool {
        self.pool
            .lookup(member)
            .is_some_and(|id| self.get_score_by_id(id).is_some())
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

        let prev_scores = Self::scores_bytes(&self.scores);
        let idx = id as usize;
        debug_assert!(
            self.get_score_by_id(id).is_some(),
            "member removed from score map must exist in scores table"
        );
        if idx < self.scores.len() {
            self.scores[idx] = EMPTY_SCORE;
        }
        let new_scores = Self::scores_bytes(&self.scores);
        if new_scores >= prev_scores {
            let delta = new_scores - prev_scores;
            self.mem_bytes += delta;
            #[cfg(test)]
            {
                self.mem_breakdown.member_table += delta;
            }
        } else {
            let delta = prev_scores - new_scores;
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
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use redis_module::raw::RedisModule_MallocSize;
    use std::collections::HashSet;
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

    fn assert_rank_matches(set: &ScoreSet, seed: u64, round: usize, stage: &str) {
        let mut expected_rank = 0usize;
        let mut iter_total = 0usize;
        for (score, bucket) in &set.by_score {
            assert!(
                !bucket.is_empty(),
                "seed {seed} round {round} stage {stage} score {score:?} has empty bucket",
            );
            for id in bucket {
                let member = set.pool.get(*id);
                let actual = set.rank(member).unwrap_or_else(|| {
                    panic!(
                        "seed {seed} round {round} stage {stage} missing rank for member {member}"
                    )
                });
                assert_eq!(
                    actual,
                    expected_rank,
                    "seed {seed} round {round} stage {stage} member {member} expected rank {expected_rank} got {actual}",
                );
                expected_rank += 1;
            }
            iter_total += bucket.len();
        }

        assert_eq!(
            expected_rank,
            set.len(),
            "seed {seed} round {round} stage {stage} iterated members {expected_rank} != len {}",
            set.len(),
        );

        assert_eq!(
            iter_total,
            set.len(),
            "seed {seed} round {round} stage {stage} iter total {iter_total} != len {}",
            set.len(),
        );
    }

    #[test]
    fn rank_remains_correct_under_churn() {
        const SEEDS: [u64; 4] = [0, 1, 2, 3];
        for &seed in &SEEDS {
            let mut rng = StdRng::seed_from_u64(seed);
            let mut set = ScoreSet::default();
            let mut members = Vec::new();
            let mut next_id = 0usize;

            for round in 0..5 {
                let insert_count = rng.gen_range(50..=150);
                for _ in 0..insert_count {
                    let member = format!("m{seed}_{next_id}");
                    next_id += 1;
                    let score = rng.gen_range(-5000.0..5000.0);
                    assert!(set.insert(score, &member));
                    members.push(member);
                }
                assert_eq!(
                    members.len(),
                    set.len(),
                    "seed {seed} round {round} stage after_initial_insert member tracking diverged",
                );
                assert_rank_matches(&set, seed, round, "after_initial_insert");

                if !members.is_empty() {
                    let removals = rng.gen_range(0..=members.len().min(40));
                    for _ in 0..removals {
                        let idx = rng.gen_range(0..members.len());
                        let member = members.swap_remove(idx);
                        assert!(set.remove(&member));
                    }
                }
                assert_eq!(
                    members.len(),
                    set.len(),
                    "seed {seed} round {round} stage after_remove member tracking diverged",
                );
                assert_rank_matches(&set, seed, round, "after_remove");

                if !members.is_empty() {
                    let min_pop = rng.gen_range(0..=members.len().min(40));
                    if min_pop > 0 {
                        let popped = set.pop_n(true, min_pop);
                        let popped_names: HashSet<String> =
                            popped.into_iter().map(|(name, _)| name).collect();
                        members.retain(|m| !popped_names.contains(m));
                    }
                }
                assert_eq!(
                    members.len(),
                    set.len(),
                    "seed {seed} round {round} stage after_pop_min member tracking diverged",
                );
                assert_rank_matches(&set, seed, round, "after_pop_min");

                if !members.is_empty() {
                    let max_pop = rng.gen_range(0..=members.len().min(40));
                    if max_pop > 0 {
                        let popped = set.pop_n(false, max_pop);
                        let popped_names: HashSet<String> =
                            popped.into_iter().map(|(name, _)| name).collect();
                        members.retain(|m| !popped_names.contains(m));
                    }
                }
                assert_eq!(
                    members.len(),
                    set.len(),
                    "seed {seed} round {round} stage after_pop_max member tracking diverged",
                );
                assert_rank_matches(&set, seed, round, "after_pop_max");

                let additional = rng.gen_range(0..=100);
                for _ in 0..additional {
                    let member = format!("m{seed}_{next_id}");
                    next_id += 1;
                    let score = rng.gen_range(-5000.0..5000.0);
                    assert!(set.insert(score, &member));
                    members.push(member);
                }
                assert_eq!(
                    members.len(),
                    set.len(),
                    "seed {seed} round {round} stage after_insert_more member tracking diverged",
                );
                assert_rank_matches(&set, seed, round, "after_insert_more");
            }
        }
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
        let mut total_members = 0usize;
        for (score, bucket) in &set_ref.by_score {
            assert!(
                !bucket.is_empty(),
                "score {score:?} should not have empty bucket",
            );
            total_members += bucket.len();
        }
        assert_eq!(total_members, set_ref.len());

        let remaining = set.range_iter(0, -1);
        for (idx, (_, member)) in remaining.iter().enumerate() {
            assert_eq!(set.rank(member), Some(idx));
        }

        while set.pop_one(true).is_some() {}
        assert!(set.is_empty());
        let set_ref = set.as_ref();
        assert!(set_ref.by_score.is_empty());
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
