use ordered_float::OrderedFloat;
use smallvec::SmallVec;
use std::{
    collections::{btree_map::Entry, BTreeMap},
    convert::TryFrom,
    mem::size_of,
};

use crate::buckets::{BucketRef, BucketStore};
use crate::pool::{MemberId, StringPool};

/// Buckets trim their heap capacity once they contain at most this many members.
const BUCKET_SHRINK_THRESHOLD: usize = 64;
/// Local buffers for pop operations use the same inline capacity so future
/// tuning keeps the thresholds in lockstep.
const BUCKET_INLINE_CAPACITY: usize = BUCKET_SHRINK_THRESHOLD;

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

pub struct ScoreSet {
    pub(crate) by_score: BTreeMap<OrderedFloat<f64>, BucketRef>,
    pub(crate) bucket_store: BucketStore,
    pub(crate) scores: Vec<f64>,
    pub(crate) pool: StringPool,
    mem_bytes: usize,
    #[cfg(test)]
    mem_breakdown: MemBreakdown,
}

impl Default for ScoreSet {
    fn default() -> Self {
        Self {
            by_score: BTreeMap::new(),
            bucket_store: BucketStore::new(),
            scores: Vec::new(),
            pool: StringPool::default(),
            mem_bytes: 0,
            #[cfg(test)]
            mem_breakdown: MemBreakdown::default(),
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
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
struct InlineIter {
    value: Option<MemberId>,
}

impl InlineIter {
    fn new(id: MemberId) -> Self {
        Self { value: Some(id) }
    }

    fn next(&mut self) -> Option<MemberId> {
        self.value.take()
    }
}

#[derive(Clone, Debug)]
enum FrontState<'a> {
    Slice(std::slice::Iter<'a, MemberId>),
    Inline(InlineIter),
}

impl<'a> FrontState<'a> {
    fn next(&mut self) -> Option<MemberId> {
        match self {
            Self::Slice(iter) => iter.next().copied(),
            Self::Inline(inline) => inline.next(),
        }
    }
}

#[derive(Clone, Debug)]
enum BackState<'a> {
    Slice(std::iter::Rev<std::slice::Iter<'a, MemberId>>),
    Inline(InlineIter),
}

impl<'a> BackState<'a> {
    fn next(&mut self) -> Option<MemberId> {
        match self {
            Self::Slice(iter) => iter.next().copied(),
            Self::Inline(inline) => inline.next(),
        }
    }
}

pub struct RangeIterFwd<'a> {
    pool: &'a StringPool,
    store: &'a BucketStore,
    outer: std::collections::btree_map::Iter<'a, OrderedFloat<f64>, BucketRef>,
    cur: Option<(std::slice::Iter<'a, MemberId>, OrderedFloat<f64>)>,
    remaining_skip: usize,
    remaining_take: usize,
}

impl<'a> RangeIterFwd<'a> {
    fn new(
        map: &'a BTreeMap<OrderedFloat<f64>, BucketRef>,
        store: &'a BucketStore,
        pool: &'a StringPool,
        skip: usize,
        take: usize,
    ) -> Self {
        Self {
            pool,
            store,
            outer: map.iter(),
            cur: None,
            remaining_skip: skip,
            remaining_take: take,
        }
    }

    fn empty(
        map: &'a BTreeMap<OrderedFloat<f64>, BucketRef>,
        store: &'a BucketStore,
        pool: &'a StringPool,
    ) -> Self {
        Self::new(map, store, pool, 0, 0)
    }

    #[inline]
    fn remaining(&self) -> usize {
        self.remaining_take
    }
}

impl<'a> Iterator for RangeIterFwd<'a> {
    type Item = (&'a str, f64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_take == 0 {
            return None;
        }
        loop {
            if let Some((ref mut iter, score)) = self.cur {
                for &id in iter.by_ref() {
                    if self.remaining_skip > 0 {
                        self.remaining_skip -= 1;
                        continue;
                    }
                    self.remaining_take -= 1;
                    let member = self.pool.get(id);
                    return Some((member, score.0));
                }
                self.cur = None;
            }
            let Some((score, bucket_ref)) = self.outer.next() else {
                self.remaining_take = 0;
                return None;
            };
            match *bucket_ref {
                BucketRef::Inline1(member) => {
                    if self.remaining_skip > 0 {
                        self.remaining_skip -= 1;
                        continue;
                    }
                    self.remaining_take -= 1;
                    let member = self.pool.get(member);
                    return Some((member, score.0));
                }
                BucketRef::Handle(bucket_id) => {
                    let mut slice = self.store.slice(bucket_id);
                    if slice.is_empty() {
                        continue;
                    }
                    if self.remaining_skip >= slice.len() {
                        self.remaining_skip -= slice.len();
                        continue;
                    }
                    if self.remaining_skip > 0 {
                        let skip = self.remaining_skip;
                        self.remaining_skip = 0;
                        slice = &slice[skip..];
                    }
                    if slice.is_empty() {
                        continue;
                    }
                    self.cur = Some((slice.iter(), *score));
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let rem = self.remaining();
        (rem, Some(rem))
    }
}

impl<'a> ExactSizeIterator for RangeIterFwd<'a> {
    fn len(&self) -> usize {
        self.remaining()
    }
}

struct IterFromFwd<'a> {
    pool: &'a StringPool,
    store: &'a BucketStore,
    outer: std::collections::btree_map::Range<'a, OrderedFloat<f64>, BucketRef>,
    cur: Option<(std::slice::Iter<'a, MemberId>, OrderedFloat<f64>)>,
    inline_first: Option<(&'a str, f64)>,
}

impl<'a> IterFromFwd<'a> {
    fn new(
        map: &'a BTreeMap<OrderedFloat<f64>, BucketRef>,
        store: &'a BucketStore,
        pool: &'a StringPool,
        score: OrderedFloat<f64>,
        member: &'a str,
        exclusive: bool,
    ) -> Self {
        use std::cmp::Ordering;

        let mut outer = map.range(score..);
        let mut cur = None;
        let mut inline_first = None;

        if let Some((s_key, bucket_ref)) = outer.next() {
            if *s_key == score {
                match *bucket_ref {
                    BucketRef::Inline1(mid) => {
                        let name = pool.get(mid);
                        let cmp = name.cmp(member);
                        if !(cmp == Ordering::Less || (cmp == Ordering::Equal && exclusive)) {
                            inline_first = Some((name, s_key.0));
                        }
                    }
                    BucketRef::Handle(bucket_id) => {
                        let slice = store.slice(bucket_id);
                        if !slice.is_empty() {
                            let pos = match slice.binary_search_by(|&m| pool.get(m).cmp(member)) {
                                Ok(p) => {
                                    if exclusive {
                                        p + 1
                                    } else {
                                        p
                                    }
                                }
                                Err(p) => p,
                            };
                            if pos < slice.len() {
                                let slice = &slice[pos..];
                                cur = Some((slice.iter(), *s_key));
                            }
                        }
                    }
                }
            } else {
                match *bucket_ref {
                    BucketRef::Inline1(mid) => {
                        let name = pool.get(mid);
                        inline_first = Some((name, s_key.0));
                    }
                    BucketRef::Handle(bucket_id) => {
                        let slice = store.slice(bucket_id);
                        if !slice.is_empty() {
                            cur = Some((slice.iter(), *s_key));
                        }
                    }
                }
            }
        }

        Self {
            pool,
            store,
            outer,
            cur,
            inline_first,
        }
    }
}

impl<'a> Iterator for IterFromFwd<'a> {
    type Item = (&'a str, f64);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some((name, score)) = self.inline_first.take() {
            return Some((name, score));
        }
        loop {
            if let Some((iter, score)) = &mut self.cur {
                if let Some(&mid) = iter.next() {
                    return Some((self.pool.get(mid), score.0));
                }
                self.cur = None;
            }
            let (score, bucket_ref) = self.outer.next()?;
            match *bucket_ref {
                BucketRef::Inline1(mid) => {
                    return Some((self.pool.get(mid), score.0));
                }
                BucketRef::Handle(bucket_id) => {
                    let slice = self.store.slice(bucket_id);
                    if slice.is_empty() {
                        continue;
                    }
                    self.cur = Some((slice.iter(), *score));
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct ScoreIter<'a> {
    pool: &'a StringPool,
    store: &'a BucketStore,
    front_outer: std::collections::btree_map::Iter<'a, OrderedFloat<f64>, BucketRef>,
    front_current: Option<(FrontState<'a>, OrderedFloat<f64>)>,
    back_outer: std::iter::Rev<std::collections::btree_map::Iter<'a, OrderedFloat<f64>, BucketRef>>,
    back_current: Option<(BackState<'a>, OrderedFloat<f64>)>,
    remaining_front_skip: usize,
    remaining_back_skip: usize,
    yielded_front: usize,
    yielded_back: usize,
    total: usize,
}

impl<'a> ScoreIter<'a> {
    fn new(
        map: &'a BTreeMap<OrderedFloat<f64>, BucketRef>,
        store: &'a BucketStore,
        pool: &'a StringPool,
        start: usize,
        stop: usize,
        len: usize,
    ) -> Self {
        Self {
            pool,
            store,
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

    fn empty(
        map: &'a BTreeMap<OrderedFloat<f64>, BucketRef>,
        store: &'a BucketStore,
        pool: &'a StringPool,
    ) -> Self {
        Self {
            pool,
            store,
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
            if let Some((ref mut state, score)) = self.front_current {
                while let Some(id) = state.next() {
                    if self.remaining_front_skip > 0 {
                        self.remaining_front_skip -= 1;
                        continue;
                    }
                    self.yielded_front += 1;
                    let member = self.pool.get(id);
                    return Some((member, score.0));
                }
                self.front_current = None;
            }
            match self.front_outer.next() {
                Some((score, bucket_ref)) => {
                    let state = match *bucket_ref {
                        BucketRef::Inline1(member) => FrontState::Inline(InlineIter::new(member)),
                        BucketRef::Handle(bucket_id) => {
                            let slice = self.store.slice(bucket_id);
                            FrontState::Slice(slice.iter())
                        }
                    };
                    self.front_current = Some((state, *score));
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
            if let Some((ref mut state, score)) = self.back_current {
                while let Some(id) = state.next() {
                    if self.remaining_back_skip > 0 {
                        self.remaining_back_skip -= 1;
                        continue;
                    }
                    self.yielded_back += 1;
                    let member = self.pool.get(id);
                    return Some((member, score.0));
                }
                self.back_current = None;
            }
            match self.back_outer.next() {
                Some((score, bucket_ref)) => {
                    let state = match *bucket_ref {
                        BucketRef::Inline1(member) => BackState::Inline(InlineIter::new(member)),
                        BucketRef::Handle(bucket_id) => {
                            let slice = self.store.slice(bucket_id);
                            BackState::Slice(slice.iter().rev())
                        }
                    };
                    self.back_current = Some((state, *score));
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

    /// Drop trailing EMPTY_SCORE slots and shrink capacity if it's far above the new length.
    fn compact_scores_tail(&mut self) {
        let mut new_len = self.scores.len();
        while new_len > 0 && self.scores[new_len - 1].is_nan() {
            new_len -= 1;
        }
        if new_len < self.scores.len() {
            self.scores.truncate(new_len);
            if self.scores.capacity() > new_len.saturating_mul(2) {
                self.scores.shrink_to_fit();
            }
        }
    }

    #[inline]
    fn scores_bytes(scores: &Vec<f64>) -> usize {
        scores.capacity() * size_of::<f64>()
    }

    #[inline]
    fn clear_score_slot(&mut self, member_id: MemberId) {
        debug_assert!(
            self.get_score_by_id(member_id).is_some(),
            "member removed from score map must exist in scores table",
        );
        let idx = member_id as usize;
        if idx < self.scores.len() {
            self.scores[idx] = EMPTY_SCORE;
        }
    }

    #[inline]
    fn account_removed_string(&mut self, removed_len: Option<usize>) {
        #[cfg(test)]
        if let Some(len) = removed_len {
            self.mem_breakdown.strings -= len;
        }
        #[cfg(not(test))]
        {
            let _ = removed_len;
        }
    }

    #[inline]
    fn score_map_bytes(map: &BTreeMap<OrderedFloat<f64>, BucketRef>) -> usize {
        if map.is_empty() {
            0
        } else {
            Self::btree_nodes(map.len())
                * size_class(Self::map_node_bytes::<OrderedFloat<f64>, BucketRef>())
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
            if let Some(bucket_ref) = self.by_score.get(&old_key).copied() {
                match bucket_ref {
                    BucketRef::Inline1(existing) => {
                        debug_assert_eq!(
                            existing, id,
                            "inline bucket must contain relocating member",
                        );
                        self.by_score.remove(&old_key);
                    }
                    BucketRef::Handle(bucket_id) => {
                        let (removed, delta, now_empty) =
                            self.bucket_store
                                .remove_by_name(bucket_id, name, |m| self.pool.get(m));
                        if removed {
                            bucket_delta += delta;
                            if now_empty {
                                let (freed, free_delta) =
                                    self.bucket_store.free_if_empty(bucket_id);
                                debug_assert!(freed, "empty bucket must be freed");
                                bucket_delta += free_delta;
                                self.by_score.remove(&old_key);
                            } else if self.bucket_store.len(bucket_id) == 1 {
                                let (remaining, delta_single) =
                                    self.bucket_store.take_singleton(bucket_id);
                                bucket_delta += delta_single;
                                self.by_score.insert(old_key, BucketRef::Inline1(remaining));
                            } else {
                                bucket_delta += self
                                    .bucket_store
                                    .maybe_shrink(bucket_id, BUCKET_SHRINK_THRESHOLD);
                            }
                        }
                    }
                }
            }
        }

        self.scores[idx] = score;

        let inserted = match self.by_score.entry(key) {
            Entry::Occupied(mut entry) => match *entry.get() {
                BucketRef::Inline1(existing_id) => {
                    // Pre-allocate for exactly the two elements we're about to insert.
                    let bucket_id = self.bucket_store.alloc_with(2);
                    let prealloc_bytes = self.bucket_store.capacity_bytes(bucket_id);
                    if prealloc_bytes > 0 {
                        bucket_delta +=
                            isize::try_from(prealloc_bytes).expect("bucket prealloc overflow");
                    }
                    let (_, delta_existing, _spilled_before, _spilled_after, _pos) = self
                        .bucket_store
                        .insert_sorted(bucket_id, existing_id, |m| self.pool.get(m));
                    bucket_delta += delta_existing;
                    let (did_insert, delta_new, _sb, _sa, _p) =
                        self.bucket_store
                            .insert_sorted(bucket_id, id, |m| self.pool.get(m));
                    bucket_delta += delta_new;
                    entry.insert(BucketRef::Handle(bucket_id));
                    did_insert
                }
                BucketRef::Handle(bucket_id) => {
                    let (did_insert, delta, _spilled_before, _spilled_after, _pos) = self
                        .bucket_store
                        .insert_sorted(bucket_id, id, |m| self.pool.get(m));
                    bucket_delta += delta;
                    did_insert
                }
            },
            Entry::Vacant(entry) => {
                entry.insert(BucketRef::Inline1(id));
                true
            }
        };

        if inserted {
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
        }
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
        match self.by_score.entry(score) {
            Entry::Occupied(mut entry) => match *entry.get() {
                BucketRef::Inline1(mid) => {
                    debug_assert_eq!(mid, id, "inline bucket must contain member when removing");
                    entry.remove();
                }
                BucketRef::Handle(bucket_id) => {
                    let (removed, delta, now_empty) =
                        self.bucket_store
                            .remove_by_name(bucket_id, member, |m| self.pool.get(m));
                    debug_assert!(removed, "member must exist in bucket when removing");
                    bucket_delta += delta;
                    if removed {
                        if now_empty {
                            let (freed, free_delta) = self.bucket_store.free_if_empty(bucket_id);
                            debug_assert!(freed, "empty bucket must be freed");
                            bucket_delta += free_delta;
                            entry.remove();
                        } else if self.bucket_store.len(bucket_id) == 1 {
                            let (remaining, delta_single) =
                                self.bucket_store.take_singleton(bucket_id);
                            bucket_delta += delta_single;
                            entry.insert(BucketRef::Inline1(remaining));
                        } else {
                            bucket_delta += self
                                .bucket_store
                                .maybe_shrink(bucket_id, BUCKET_SHRINK_THRESHOLD);
                        }
                    }
                }
            },
            Entry::Vacant(_) => return false,
        }
        if bucket_delta != 0 {
            self.apply_bucket_mem_delta(bucket_delta);
        }

        let idx = id as usize;
        if idx < self.scores.len() {
            self.scores[idx] = EMPTY_SCORE;
        }
        // Try to reclaim tail capacity if we just cleared the last live slot(s).
        self.compact_scores_tail();

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
        let bucket_ref = *self.by_score.get(&score_key)?;
        let pos = match bucket_ref {
            BucketRef::Inline1(mid) => {
                if mid == id {
                    Some(0)
                } else {
                    None
                }
            }
            BucketRef::Handle(bucket_id) => self
                .bucket_store
                .slice(bucket_id)
                .binary_search_by(|&m| self.pool.get(m).cmp(member))
                .ok(),
        }?;
        let prefix = self
            .by_score
            .range(..score_key)
            .map(|(_, bref)| match *bref {
                BucketRef::Inline1(_) => 1,
                BucketRef::Handle(bucket_id) => self.bucket_store.len(bucket_id),
            })
            .sum::<usize>();
        Some(prefix + pos)
    }

    pub fn select_by_rank(&self, mut r: usize) -> (&str, f64) {
        for (score, bucket_ref) in &self.by_score {
            match *bucket_ref {
                BucketRef::Inline1(mid) => {
                    if r == 0 {
                        return (self.pool.get(mid), score.0);
                    }
                    r -= 1;
                }
                BucketRef::Handle(bucket_id) => {
                    let bucket = self.bucket_store.slice(bucket_id);
                    if r < bucket.len() {
                        let id = bucket[r];
                        return (self.pool.get(id), score.0);
                    }
                    r -= bucket.len();
                }
            }
        }
        unreachable!("rank out of bounds");
    }

    pub fn iter_range(&self, start: isize, stop: isize) -> ScoreIter<'_> {
        let len = self.pool.len() as isize;
        if len == 0 {
            return ScoreIter::empty(&self.by_score, &self.bucket_store, &self.pool);
        }
        let mut start = if start < 0 { len + start } else { start };
        let mut stop = if stop < 0 { len + stop } else { stop };
        if start < 0 {
            start = 0;
        }
        if stop < 0 {
            return ScoreIter::empty(&self.by_score, &self.bucket_store, &self.pool);
        }
        if stop >= len {
            stop = len - 1;
        }
        if start > stop {
            return ScoreIter::empty(&self.by_score, &self.bucket_store, &self.pool);
        }
        ScoreIter::new(
            &self.by_score,
            &self.bucket_store,
            &self.pool,
            start as usize,
            stop as usize,
            len as usize,
        )
    }

    pub fn iter_range_fwd(&self, start: isize, stop: isize) -> RangeIterFwd<'_> {
        let len = self.pool.len() as isize;
        if len == 0 {
            return RangeIterFwd::empty(&self.by_score, &self.bucket_store, &self.pool);
        }
        let mut start = if start < 0 { len + start } else { start };
        let mut stop = if stop < 0 { len + stop } else { stop };
        if start < 0 {
            start = 0;
        }
        if stop < 0 {
            return RangeIterFwd::empty(&self.by_score, &self.bucket_store, &self.pool);
        }
        if stop >= len {
            stop = len - 1;
        }
        if start > stop {
            return RangeIterFwd::empty(&self.by_score, &self.bucket_store, &self.pool);
        }
        RangeIterFwd::new(
            &self.by_score,
            &self.bucket_store,
            &self.pool,
            start as usize,
            (stop - start + 1) as usize,
        )
    }

    pub fn range_iter(&self, start: isize, stop: isize) -> Vec<(f64, String)> {
        self.iter_range_fwd(start, stop)
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
        self.iter_range_fwd(0, self.len() as isize - 1)
    }

    pub fn iter_from<'a>(
        &'a self,
        score: OrderedFloat<f64>,
        member: &'a str,
        exclusive: bool,
    ) -> impl Iterator<Item = (&'a str, f64)> + 'a {
        IterFromFwd::new(
            &self.by_score,
            &self.bucket_store,
            &self.pool,
            score,
            member,
            exclusive,
        )
    }

    #[cfg(any(test, feature = "bench"))]
    pub fn all_items(&self) -> Vec<(f64, String)> {
        let mut out = Vec::new();
        for (score, bucket_ref) in &self.by_score {
            match *bucket_ref {
                BucketRef::Inline1(mid) => {
                    out.push((score.0, self.pool.get(mid).to_owned()));
                }
                BucketRef::Handle(bucket_id) => {
                    let bucket = self.bucket_store.slice(bucket_id);
                    for id in bucket {
                        out.push((score.0, self.pool.get(*id).to_owned()));
                    }
                }
            }
        }
        out
    }

    #[cfg(any(test, feature = "bench"))]
    pub fn member_names(&self) -> Vec<String> {
        self.pool.iter().map(|(name, _)| name.to_owned()).collect()
    }

    #[cfg(any(test, feature = "bench"))]
    pub fn members_with_scores(&self) -> Vec<(String, f64)> {
        let mut out = Vec::new();
        for (name, id) in self.pool.iter() {
            if let Some(score) = self.get_score_by_id(id) {
                out.push((name.to_owned(), score));
            }
        }
        out
    }

    pub fn contains(&self, member: &str) -> bool {
        self.pool
            .lookup(member)
            .is_some_and(|id| self.get_score_by_id(id).is_some())
    }

    pub fn peek_pop_count(&self, min: bool, n: usize) -> usize {
        if n == 0 || self.is_empty() {
            return 0;
        }

        let mut remaining = n;
        let mut total = 0usize;

        if min {
            for bucket_ref in self.by_score.values() {
                if remaining == 0 {
                    break;
                }
                let bucket_len = match bucket_ref {
                    BucketRef::Inline1(_) => 1,
                    BucketRef::Handle(bucket_id) => self.bucket_store.len(*bucket_id),
                };
                let take = bucket_len.min(remaining);
                total += take;
                remaining -= take;
                if remaining == 0 {
                    break;
                }
            }
        } else {
            for bucket_ref in self.by_score.values().rev() {
                if remaining == 0 {
                    break;
                }
                let bucket_len = match bucket_ref {
                    BucketRef::Inline1(_) => 1,
                    BucketRef::Handle(bucket_id) => self.bucket_store.len(*bucket_id),
                };
                let take = bucket_len.min(remaining);
                total += take;
                remaining -= take;
                if remaining == 0 {
                    break;
                }
            }
        }

        total
    }

    pub fn pop_one_visit<F>(&mut self, min: bool, mut visit: F) -> bool
    where
        F: FnMut(&str, f64),
    {
        self.pop_n_visit(min, 1, |name, score| visit(name, score)) != 0
    }

    pub fn pop_one(&mut self, min: bool) -> Option<(String, f64)> {
        let mut out = None;
        let popped = self.pop_one_visit(min, |name, score| {
            out = Some((name.to_owned(), score));
        });
        if popped {
            debug_assert!(out.is_some());
        } else {
            debug_assert!(out.is_none());
        }
        out
    }

    pub fn pop_n_visit<F>(&mut self, min: bool, n: usize, mut visit: F) -> usize
    where
        F: FnMut(&str, f64),
    {
        if n == 0 {
            return 0;
        }
        let mut emitted = 0usize;
        let mut prev_scores: Option<usize> = None;
        let mut member_buffer: SmallVec<[MemberId; BUCKET_INLINE_CAPACITY]> = SmallVec::new();

        while emitted < n {
            let (score_key, bucket_ref) = if min {
                match self.by_score.first_key_value() {
                    Some((score, bucket_ref)) => (*score, *bucket_ref),
                    None => break,
                }
            } else {
                match self.by_score.last_key_value() {
                    Some((score, bucket_ref)) => (*score, *bucket_ref),
                    None => break,
                }
            };

            if prev_scores.is_none() {
                prev_scores = Some(Self::scores_bytes(&self.scores));
            }

            let prev_map = Self::score_map_bytes(&self.by_score);
            let score = score_key.0;
            match bucket_ref {
                BucketRef::Inline1(member_id) => {
                    {
                        let name = self.pool.get(member_id);
                        visit(name, score);
                    }
                    self.clear_score_slot(member_id);
                    let removed = self.pool.remove_by_id(member_id);
                    self.account_removed_string(removed);
                    self.by_score.remove(&score_key);
                    emitted += 1;
                }
                BucketRef::Handle(bucket_id) => {
                    let remaining = n - emitted;
                    let bucket_len = self.bucket_store.len(bucket_id);
                    let to_take = remaining.min(bucket_len);
                    member_buffer.clear();
                    {
                        let bucket = self.bucket_store.slice(bucket_id);
                        if min {
                            member_buffer.extend(bucket.iter().take(to_take).copied());
                        } else {
                            member_buffer.extend(bucket.iter().rev().take(to_take).copied());
                        }
                    }

                    if member_buffer.is_empty() {
                        break;
                    }

                    for &member_id in &member_buffer {
                        {
                            let name = self.pool.get(member_id);
                            visit(name, score);
                        }
                        self.clear_score_slot(member_id);
                        let removed = self.pool.remove_by_id(member_id);
                        self.account_removed_string(removed);
                    }

                    let popped_here = member_buffer.len();
                    emitted += popped_here;

                    let (now_empty, mut bucket_delta) = if min {
                        self.bucket_store.advance_front_k(
                            bucket_id,
                            popped_here,
                            BUCKET_SHRINK_THRESHOLD,
                        )
                    } else {
                        self.bucket_store.drain_back_k(
                            bucket_id,
                            popped_here,
                            BUCKET_SHRINK_THRESHOLD,
                        )
                    };

                    if now_empty {
                        self.by_score.remove(&score_key);
                    } else if self.bucket_store.len(bucket_id) == 1 {
                        let (remaining_member, delta_single) =
                            self.bucket_store.take_singleton(bucket_id);
                        bucket_delta += delta_single;
                        if let Some(entry) = self.by_score.get_mut(&score_key) {
                            *entry = BucketRef::Inline1(remaining_member);
                        }
                    }

                    if bucket_delta != 0 {
                        self.apply_bucket_mem_delta(bucket_delta);
                    }
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
        }

        if let Some(prev_scores) = prev_scores {
            self.compact_scores_tail();
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
        }

        emitted
    }

    pub fn pop_n(&mut self, min: bool, n: usize) -> Vec<(String, f64)> {
        let mut out = Vec::with_capacity(n.min(self.len()));
        self.pop_n_visit(min, n, |name, score| {
            out.push((name.to_owned(), score));
        });
        out
    }

    #[doc(hidden)]
    pub fn bucket_capacity_for_test(&self, score: f64) -> Option<usize> {
        match self.by_score.get(&OrderedFloat(score))? {
            BucketRef::Inline1(_) => Some(1),
            BucketRef::Handle(id) => {
                let bytes = self.bucket_store.capacity_bytes(*id);
                Some(if bytes == 0 {
                    0
                } else {
                    bytes / size_of::<MemberId>()
                })
            }
        }
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
    use crate::buckets::{Bucket, BucketRef, BucketStore};
    use crate::memory::gzset_mem_usage;
    use crate::pool::{IndexEntry, MemberId};
    use ordered_float::OrderedFloat;
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use redis_module::raw::RedisModule_MallocSize;
    use std::collections::HashSet;
    use std::mem::size_of;
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
            total += size_class(set.pool.free_ids.capacity() * size_of::<MemberId>());
        }
        for chunk in &set.pool.arena {
            let chunk_bytes = ms(chunk.as_ptr() as *const _);
            if chunk_bytes > 0 {
                total += chunk_bytes;
            } else {
                total += size_class(chunk.len());
            }
        }

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

    fn assert_rank_matches(set: &ScoreSet, seed: u64, round: usize, stage: &str) {
        let mut expected_rank = 0usize;
        let mut iter_total = 0usize;
        for (score, bucket_ref) in &set.by_score {
            match *bucket_ref {
                BucketRef::Inline1(mid) => {
                    let member = set.pool.get(mid);
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
                    iter_total += 1;
                }
                BucketRef::Handle(bucket_id) => {
                    let bucket = set.bucket_store.slice(bucket_id);
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
            }
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
    fn unique_scores_do_not_allocate_store() {
        let mut set = ScoreSet::default();
        let total = 10_000;
        for i in 0..total {
            let member = format!("m{i}");
            assert!(set.insert(i as f64, &member));
        }
        assert_eq!(set.bucket_store.buckets.len(), 0);
        assert!(set
            .by_score
            .values()
            .all(|bucket_ref| matches!(bucket_ref, BucketRef::Inline1(_))));
    }

    #[test]
    fn handle_reverts_to_inline_after_removal() {
        let mut set = ScoreSet::default();
        assert!(set.insert(1.0, "a"));
        assert!(set.insert(1.0, "b"));
        let bucket_ref = *set
            .by_score
            .get(&OrderedFloat(1.0))
            .expect("score should exist");
        let bucket_id = match bucket_ref {
            BucketRef::Handle(id) => id,
            BucketRef::Inline1(_) => panic!("expected handle after inserting two members"),
        };
        assert!(set.remove("a"));
        let new_ref = *set
            .by_score
            .get(&OrderedFloat(1.0))
            .expect("score should remain present");
        match new_ref {
            BucketRef::Inline1(mid) => assert_eq!(set.pool.get(mid), "b"),
            BucketRef::Handle(_) => panic!("bucket should convert back to inline"),
        }
        match set.bucket_store.buckets.get(bucket_id as usize) {
            Some(slot) => assert!(slot.is_none(), "bucket slot should be freed"),
            None => assert!(
                set.bucket_store.buckets.len() <= bucket_id as usize,
                "bucket vector should be truncated or empty"
            ),
        }
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
    fn compacts_bucket_store_after_freeing_tail() {
        let mut set = Box::new(ScoreSet::default());
        let bucket_count = 128usize;
        for score in 0..bucket_count {
            let member_a = format!("m{score}_a");
            let member_b = format!("m{score}_b");
            assert!(set.insert(score as f64, &member_a));
            assert!(set.insert(score as f64, &member_b));
        }
        assert_eq!(set.bucket_store.buckets.len(), bucket_count);
        let before_capacity = set.bucket_store.buckets.capacity();
        assert!(before_capacity >= bucket_count);
        let before_usage = unsafe { gzset_mem_usage((&*set as *const ScoreSet) as *const c_void) };

        for score in (bucket_count / 2..bucket_count).rev() {
            let member_a = format!("m{score}_a");
            let member_b = format!("m{score}_b");
            assert!(set.remove(&member_a));
            assert!(set.remove(&member_b));
        }

        assert_eq!(set.bucket_store.buckets.len(), bucket_count / 2);
        let after_capacity = set.bucket_store.buckets.capacity();
        assert!(
            after_capacity < before_capacity,
            "capacity should shrink: before {before_capacity} after {after_capacity}"
        );
        let after_usage = unsafe { gzset_mem_usage((&*set as *const ScoreSet) as *const c_void) };
        assert!(
            after_usage < before_usage,
            "mem usage should shrink: before {before_usage} after {after_usage}"
        );
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
        for (score, bucket_ref) in &set_ref.by_score {
            match *bucket_ref {
                BucketRef::Inline1(_) => {
                    total_members += 1;
                }
                BucketRef::Handle(bucket_id) => {
                    let bucket = set_ref.bucket_store.slice(bucket_id);
                    assert!(
                        !bucket.is_empty(),
                        "score {score:?} should not have empty bucket",
                    );
                    total_members += bucket.len();
                }
            }
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

    #[test]
    fn pop_n_matches_pop_one_mem_delta() {
        let mut streaming = ScoreSet::default();
        let mut sequential = ScoreSet::default();
        let items = [
            (1.0, "a1"),
            (1.0, "a2"),
            (1.0, "a3"),
            (2.0, "b1"),
            (2.0, "b2"),
            (3.0, "c1"),
        ];
        for (score, member) in items {
            assert!(streaming.insert(score, member));
            assert!(sequential.insert(score, member));
        }

        let count = 4usize;
        let popped_streaming = streaming.pop_n(true, count);
        let mut popped_sequential = Vec::new();
        for _ in 0..count {
            if let Some(item) = sequential.pop_one(true) {
                popped_sequential.push(item);
            }
        }

        assert_eq!(popped_streaming, popped_sequential);
        assert_eq!(streaming.len(), sequential.len());
        assert_eq!(streaming.mem_bytes(), sequential.mem_bytes());
        assert_eq!(
            streaming.debug_mem_breakdown(),
            sequential.debug_mem_breakdown()
        );
        assert!(!streaming.by_score.contains_key(&OrderedFloat(1.0)));
        let two_ref = streaming.by_score.get(&OrderedFloat(2.0)).copied();
        assert!(matches!(two_ref, Some(BucketRef::Inline1(_))));
    }

    fn bucket_shrink_mem_on_pop(min: bool) {
        let mut set = ScoreSet::default();
        let total = super::BUCKET_SHRINK_THRESHOLD * 2;
        for i in 0..total {
            let member = format!("m{i}");
            assert!(set.insert(1.0, &member));
        }
        let bucket_ref = *set
            .by_score
            .get(&OrderedFloat(1.0))
            .expect("bucket should exist");
        let bucket_id = match bucket_ref {
            BucketRef::Handle(id) => id,
            BucketRef::Inline1(_) => panic!("bucket should spill when over threshold"),
        };
        let initial_bytes = set.bucket_store.capacity_bytes(bucket_id);
        assert!(initial_bytes > 0, "expected spill before pops");
        let initial_cap = initial_bytes / size_of::<MemberId>();
        assert!(
            initial_cap > super::BUCKET_SHRINK_THRESHOLD,
            "expected spill before pops",
        );

        let before_mem = set.mem_bytes();
        let before_buckets = set.debug_mem_breakdown().buckets;
        assert_eq!(
            before_buckets, initial_bytes,
            "bucket accounting should reflect spill"
        );

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
        let remaining_ref = set
            .by_score
            .get(&OrderedFloat(1.0))
            .expect("bucket should remain present");
        let remaining_bytes = match remaining_ref {
            BucketRef::Inline1(_) => 0,
            BucketRef::Handle(id) => set.bucket_store.capacity_bytes(*id),
        };
        assert!(
            remaining_bytes <= super::BUCKET_SHRINK_THRESHOLD * size_of::<MemberId>(),
            "remaining capacity should be bounded by threshold",
        );
        assert_eq!(
            after_buckets, remaining_bytes,
            "bucket accounting should match remaining spill",
        );
        let mem_drop = before_mem - after_mem;
        assert!(
            mem_drop >= initial_bytes - remaining_bytes,
            "bucket shrink should free at least previous spill: drop {mem_drop} initial {initial_bytes} remaining {remaining_bytes}"
        );
        assert_eq!(
            before_buckets - after_buckets,
            initial_bytes - remaining_bytes,
            "bucket breakdown should match freed bytes",
        );
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

    #[test]
    fn repeated_min_pops_from_single_bucket() {
        let mut set = ScoreSet::default();
        let total = super::BUCKET_SHRINK_THRESHOLD * 8;
        let names: Vec<String> = (0..total).map(|i| format!("member-{i:05}")).collect();
        for name in &names {
            assert!(set.insert(1.0, name));
        }

        let score_key = OrderedFloat(1.0);
        let bucket_ref = *set
            .by_score
            .get(&score_key)
            .expect("bucket should exist after inserts");
        let bucket_id = match bucket_ref {
            BucketRef::Handle(id) => id,
            BucketRef::Inline1(_) => panic!("bucket must spill for repeated pops"),
        };
        let initial_bucket_bytes = set.bucket_store.capacity_bytes(bucket_id);
        assert!(
            initial_bucket_bytes > super::BUCKET_SHRINK_THRESHOLD * size_of::<MemberId>(),
            "expected spilled capacity before pops",
        );

        let mut expected_index = 0usize;
        let mut prev_bucket_bytes = set.debug_mem_breakdown().buckets;

        while expected_index < total {
            let mut popped_name = None;
            let emitted = set.pop_n_visit(true, 1, |name, score| {
                assert_eq!(score, 1.0);
                popped_name = Some(name.to_owned());
            });
            if emitted == 0 {
                break;
            }
            let name = popped_name.expect("pop should emit a member");
            assert_eq!(
                name, names[expected_index],
                "unexpected member order after {expected_index} pops",
            );
            expected_index += 1;

            let remaining = total - expected_index;
            assert_eq!(set.len(), remaining, "len mismatch after pops");

            let buckets_bytes = set.debug_mem_breakdown().buckets;
            assert!(
                buckets_bytes <= prev_bucket_bytes,
                "bucket usage should be non-increasing",
            );
            prev_bucket_bytes = buckets_bytes;

            if remaining == 0 {
                assert!(!set.by_score.contains_key(&score_key));
                assert_eq!(buckets_bytes, 0);
                break;
            }

            if remaining == 1 {
                let entry = set
                    .by_score
                    .get(&score_key)
                    .copied()
                    .expect("score entry must exist while remaining members > 0");
                assert!(matches!(entry, BucketRef::Inline1(_)));
                assert_eq!(buckets_bytes, 0);
            } else {
                let entry = set
                    .by_score
                    .get(&score_key)
                    .copied()
                    .expect("score entry must exist while remaining members > 1");
                match entry {
                    BucketRef::Handle(id) => {
                        let cap_bytes = set.bucket_store.capacity_bytes(id);
                        if remaining > super::BUCKET_SHRINK_THRESHOLD {
                            assert_eq!(
                                cap_bytes, initial_bucket_bytes,
                                "capacity should remain until shrink threshold",
                            );
                        } else if remaining == super::BUCKET_SHRINK_THRESHOLD {
                            assert!(
                                cap_bytes >= super::BUCKET_SHRINK_THRESHOLD * size_of::<MemberId>(),
                                "capacity should stay at or above threshold before shrinking",
                            );
                        } else {
                            assert!(
                                cap_bytes <= super::BUCKET_SHRINK_THRESHOLD * size_of::<MemberId>(),
                                "capacity should shrink near threshold",
                            );
                        }
                        assert_eq!(
                            buckets_bytes, cap_bytes,
                            "bucket accounting should match capacity",
                        );
                    }
                    BucketRef::Inline1(_) => {
                        panic!("bucket should remain spilled while more than one member remains");
                    }
                }
            }

            if remaining > 0 {
                let next_name = &names[expected_index];
                assert_eq!(
                    set.rank(next_name),
                    Some(0),
                    "next member should stay at front",
                );
            }
        }

        assert_eq!(expected_index, total);
        assert!(set.is_empty());
    }
}
