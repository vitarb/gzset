use ordered_float::OrderedFloat;
use smallvec::SmallVec;
use std::collections::BTreeMap;

use crate::{
    compact_table::CompactTable,
    pool::{MemberId, StringPool},
};

type Bucket = SmallVec<[MemberId; 4]>;

#[derive(Default)]
pub struct ScoreSet {
    pub(crate) by_score: BTreeMap<OrderedFloat<f64>, Bucket>,
    pub(crate) members: CompactTable,
    pub(crate) pool: StringPool,
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
    pub fn insert(&mut self, score: f64, member: &str) -> bool {
        let key = OrderedFloat(score);
        let id = self.pool.intern(member);
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
                    if bucket.is_empty() {
                        self.by_score.remove(&old_key);
                    } else if bucket.spilled() && bucket.len() <= 4 {
                        bucket.shrink_to_fit();
                    }
                }
            }
        }
        let bucket = self.by_score.entry(key).or_default();
        match bucket.binary_search_by(|&m| self.pool.get(m).cmp(name)) {
            Ok(_) => false,
            Err(pos) => {
                bucket.insert(pos, id);
                true
            }
        }
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
        if self.members.remove(id) {
            if let Some(bucket) = self.by_score.get_mut(&score) {
                if let Ok(pos) = bucket.binary_search_by(|&m| self.pool.get(m).cmp(member)) {
                    bucket.remove(pos);
                }
                if bucket.is_empty() {
                    self.by_score.remove(&score);
                } else if bucket.spilled() && bucket.len() <= 4 {
                    bucket.shrink_to_fit();
                }
            }
            self.pool.remove(member);
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
        let target = OrderedFloat(self.members.get(id)?);
        let mut idx = 0usize;
        for (score, bucket) in &self.by_score {
            if *score == target {
                for m in bucket {
                    if *m == id {
                        return Some(idx);
                    }
                    idx += 1;
                }
            } else {
                idx += bucket.len();
            }
        }
        None
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

    pub fn all_items(&self) -> Vec<(f64, String)> {
        let mut out = Vec::new();
        for (score, bucket) in &self.by_score {
            for id in bucket {
                out.push((score.0, self.pool.get(*id).to_owned()));
            }
        }
        out
    }

    pub fn member_names(&self) -> Vec<String> {
        self.members
            .iter()
            .map(|(id, _)| self.pool.get(id).to_owned())
            .collect()
    }

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

    #[doc(hidden)]
    pub fn bucket_capacity_for_test(&self, score: f64) -> Option<usize> {
        self.by_score
            .get(&OrderedFloat(score))
            .map(|b| b.capacity())
    }

    #[cfg(any(test, feature = "bench"))]
    pub fn pop_all(&mut self, min: bool) -> Vec<String> {
        let mut out = Vec::new();
        while !self.by_score.is_empty() {
            let mut entry = if min {
                self.by_score.first_entry().unwrap()
            } else {
                self.by_score.last_entry().unwrap()
            };
            let bucket = entry.get_mut();
            let id = if min {
                bucket.remove(0)
            } else {
                bucket.pop().unwrap()
            };
            if bucket.is_empty() {
                entry.remove_entry();
            } else if bucket.spilled() && bucket.len() <= 4 {
                bucket.shrink_to_fit();
            }
            self.members.remove(id);
            let name = self.pool.get(id).to_owned();
            self.pool.remove(&name);
            out.push(name);
        }
        out
    }
}
