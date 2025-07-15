use ordered_float::OrderedFloat;
use rustc_hash::FxHashMap;
use std::collections::{BTreeMap, BTreeSet};

pub type FastHashMap<K, V> = FxHashMap<K, V>;

#[derive(Default)]
pub struct ScoreSet {
    pub(crate) by_score: BTreeMap<OrderedFloat<f64>, BTreeSet<String>>,
    pub(crate) members: FastHashMap<String, OrderedFloat<f64>>,
}

#[derive(Clone, Debug)]
pub struct ScoreIter<'a> {
    front_outer:
        std::collections::btree_map::Iter<'a, OrderedFloat<f64>, BTreeSet<String>>,
    front_current: Option<(
        std::collections::btree_set::Iter<'a, String>,
        OrderedFloat<f64>,
    )>,
    back_outer:
        std::iter::Rev<std::collections::btree_map::Iter<'a, OrderedFloat<f64>, BTreeSet<String>>>,
    back_current: Option<(
        std::collections::btree_set::Iter<'a, String>,
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
        map: &'a BTreeMap<OrderedFloat<f64>, BTreeSet<String>>,
        start: usize,
        stop: usize,
        len: usize,
    ) -> Self {
        Self {
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

    fn empty(map: &'a BTreeMap<OrderedFloat<f64>, BTreeSet<String>>) -> Self {
        Self {
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
                for member in iter.by_ref() {
                    if self.remaining_front_skip > 0 {
                        self.remaining_front_skip -= 1;
                        continue;
                    }
                    self.yielded_front += 1;
                    return Some((member.as_str(), score.0));
                }
                self.front_current = None;
            }
            match self.front_outer.next() {
                Some((score, set)) => {
                    self.front_current = Some((set.iter(), *score));
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
                for member in iter.by_ref().rev() {
                    if self.remaining_back_skip > 0 {
                        self.remaining_back_skip -= 1;
                        continue;
                    }
                    self.yielded_back += 1;
                    return Some((member.as_str(), score.0));
                }
                self.back_current = None;
            }
            match self.back_outer.next() {
                Some((score, set)) => {
                    self.back_current = Some((set.iter(), *score));
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
        match self.members.insert(member.to_owned(), key) {
            Some(old) if old == key => return false,
            Some(old) => {
                if let Some(set) = self.by_score.get_mut(&old) {
                    set.remove(member);
                    if set.is_empty() {
                        self.by_score.remove(&old);
                    }
                }
            }
            None => {}
        }
        self
            .by_score
            .entry(key)
            .or_default()
            .insert(member.to_owned());
        true
    }

    pub fn remove(&mut self, member: &str) -> bool {
        if let Some(score) = self.members.remove(member) {
            if let Some(set) = self.by_score.get_mut(&score) {
                set.remove(member);
                if set.is_empty() {
                    self.by_score.remove(&score);
                }
            }
            true
        } else {
            false
        }
    }

    pub fn score(&self, member: &str) -> Option<f64> {
        self.members.get(member).map(|s| s.0)
    }

    pub fn rank(&self, member: &str) -> Option<usize> {
        let target = *self.members.get(member)?;
        let mut idx = 0usize;
        for (score, set) in &self.by_score {
            if *score == target {
                for m in set {
                    if m == member {
                        return Some(idx);
                    }
                    idx += 1;
                }
            } else {
                idx += set.len();
            }
        }
        None
    }

    pub fn iter_range(&self, start: isize, stop: isize) -> ScoreIter<'_> {
        let len = self.members.len() as isize;
        if len == 0 {
            return ScoreIter::empty(&self.by_score);
        }
        let mut start = if start < 0 { len + start } else { start };
        let mut stop = if stop < 0 { len + stop } else { stop };
        if start < 0 {
            start = 0;
        }
        if stop < 0 {
            return ScoreIter::empty(&self.by_score);
        }
        if stop >= len {
            stop = len - 1;
        }
        if start > stop {
            return ScoreIter::empty(&self.by_score);
        }
        ScoreIter::new(&self.by_score, start as usize, stop as usize, len as usize)
    }

    pub fn range_iter(&self, start: isize, stop: isize) -> Vec<(f64, String)> {
        self
            .iter_range(start, stop)
            .map(|(m, s)| (s, m.to_owned()))
            .collect()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn all_items(&self) -> Vec<(f64, String)> {
        let mut out = Vec::new();
        for (score, set) in &self.by_score {
            for m in set {
                out.push((score.0, m.clone()));
            }
        }
        out
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
            let set = entry.get_mut();
            let member = if min {
                let m = set.iter().next().unwrap().clone();
                set.take(&m).unwrap()
            } else {
                let m = set.iter().next_back().unwrap().clone();
                set.take(&m).unwrap()
            };
            let empty = set.is_empty();
            if empty {
                entry.remove_entry();
            }
            self.members.remove(&member);
            out.push(member);
        }
        out
    }
}
