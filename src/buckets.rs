use std::{convert::TryFrom, mem::size_of};

use crate::pool::MemberId;

pub type BucketId = u32;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum BucketRef {
    /// Exactly one member, stored inline in the score map.
    Inline1(MemberId),
    /// Index into [`BucketStore`].
    Handle(BucketId),
}

#[derive(Debug, Default)]
pub struct Bucket {
    data: Vec<MemberId>,
    head: usize,
}

impl Bucket {
    fn with_capacity(min_cap: usize) -> Self {
        Self {
            data: Vec::with_capacity(min_cap),
            head: 0,
        }
    }

    fn len(&self) -> usize {
        self.data.len().saturating_sub(self.head)
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn capacity(&self) -> usize {
        self.data.capacity()
    }

    fn head(&self) -> usize {
        self.head
    }

    fn as_slice(&self) -> &[MemberId] {
        debug_assert!(self.head <= self.data.len(), "bucket head beyond buffer");
        &self.data[self.head..]
    }

    fn insert_at(&mut self, pos: usize, member: MemberId) {
        let len = self.len();
        debug_assert!(pos <= len, "insert position out of bounds");
        if pos == 0 && self.head > 0 {
            let new_head = self.head - 1;
            self.data[new_head] = member;
            self.head = new_head;
            return;
        }

        let idx = self.head + pos;
        if idx == self.data.len() {
            self.data.push(member);
        } else {
            self.data.insert(idx, member);
        }
    }

    fn remove_at(&mut self, pos: usize) -> MemberId {
        let len = self.len();
        debug_assert!(pos < len, "remove position out of bounds");
        if pos == 0 {
            let idx = self.head;
            let value = self.data[idx];
            self.head += 1;
            if self.head >= self.data.len() {
                self.data.clear();
                self.head = 0;
            }
            return value;
        }

        let idx = self.head + pos;
        if pos + 1 == len {
            debug_assert_eq!(idx + 1, self.data.len(), "tail removal must pop last");
            let value = self
                .data
                .pop()
                .expect("bucket len tracked pop requires element");
            if self.head >= self.data.len() {
                self.data.clear();
                self.head = 0;
            }
            value
        } else {
            self.data.remove(idx)
        }
    }

    fn advance_front(&mut self, k: usize) -> usize {
        let len = self.len();
        let take = k.min(len);
        if take == 0 {
            return 0;
        }
        self.head += take;
        if self.head >= self.data.len() {
            self.data.clear();
            self.head = 0;
        }
        take
    }

    fn drain_back(&mut self, k: usize) -> usize {
        let len = self.len();
        let take = k.min(len);
        if take == 0 {
            return 0;
        }
        let new_len = self.data.len() - take;
        self.data.truncate(new_len);
        if self.head >= self.data.len() {
            self.data.clear();
            self.head = 0;
        }
        take
    }

    fn compact_head(&mut self) {
        if self.head > 0 {
            debug_assert!(self.head <= self.data.len(), "bucket head beyond data len");
            self.data.drain(..self.head);
            self.head = 0;
        }
    }

    fn maybe_compact(&mut self, shrink_threshold: usize) -> isize {
        if self.is_empty() {
            self.data.clear();
            self.head = 0;
            return 0;
        }

        let cap_before = self.data.capacity();
        let total_len = self.data.len();
        let len = self.len();
        debug_assert!(self.head <= total_len, "bucket head beyond buffer");

        if self.head >= shrink_threshold && (len <= shrink_threshold || self.head > total_len / 2) {
            self.compact_head();
        }

        let len_after = self.len();
        if len_after <= shrink_threshold {
            self.data.shrink_to_fit();
        }

        let cap_after = self.data.capacity();
        if cap_after < cap_before {
            let bytes = (cap_before - cap_after) * size_of::<MemberId>();
            -isize::try_from(bytes).expect("bucket shrink delta overflow")
        } else {
            0
        }
    }
}

#[derive(Default, Debug)]
pub struct BucketStore {
    pub(crate) buckets: Vec<Option<Bucket>>,
    pub(crate) free: Vec<BucketId>,
}

impl BucketStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn bucket(&self, id: BucketId) -> &Bucket {
        self.buckets
            .get(id as usize)
            .and_then(|slot| slot.as_ref())
            .expect("invalid bucket id")
    }

    fn bucket_mut(&mut self, id: BucketId) -> &mut Bucket {
        self.buckets
            .get_mut(id as usize)
            .and_then(|slot| slot.as_mut())
            .expect("invalid bucket id")
    }

    fn alloc_inner(&mut self, min_cap: usize) -> BucketId {
        if let Some(id) = self.free.pop() {
            let slot = self
                .buckets
                .get_mut(id as usize)
                .expect("reused bucket id out of bounds");
            debug_assert!(slot.is_none(), "reused bucket slot must be empty");
            *slot = Some(Bucket::with_capacity(min_cap));
            id
        } else {
            let idx = self.buckets.len();
            let id = BucketId::try_from(idx).expect("too many buckets allocated");
            self.buckets.push(Some(Bucket::with_capacity(min_cap)));
            id
        }
    }

    #[inline]
    #[allow(dead_code)]
    pub fn alloc(&mut self) -> BucketId {
        self.alloc_inner(0)
    }

    /// Allocate a new bucket with at least `min_cap` capacity.
    #[inline]
    pub fn alloc_with(&mut self, min_cap: usize) -> BucketId {
        self.alloc_inner(min_cap)
    }

    pub fn free_if_empty(&mut self, id: BucketId) -> (bool, isize) {
        let buckets_len = self.buckets.len();
        let is_last = (id as usize) + 1 == buckets_len;
        let slot = self
            .buckets
            .get_mut(id as usize)
            .expect("invalid bucket id");
        if let Some(bucket) = slot {
            if bucket.is_empty() {
                let spilled_bytes = bucket.capacity() * size_of::<MemberId>();
                *slot = None;
                self.free.push(id);
                if is_last {
                    self.drop_trailing_empty();
                }
                let delta = if spilled_bytes == 0 {
                    0
                } else {
                    -isize::try_from(spilled_bytes).expect("bucket spill free delta overflow")
                };
                return (true, delta);
            }
        }
        (false, 0)
    }

    pub fn slice(&self, id: BucketId) -> &[MemberId] {
        self.bucket(id).as_slice()
    }

    pub fn insert_sorted<'a, F>(
        &mut self,
        id: BucketId,
        member: MemberId,
        cmp_name: F,
    ) -> (bool, isize, bool, bool, usize)
    where
        F: Fn(MemberId) -> &'a str,
    {
        let bucket = self.bucket_mut(id);
        let cap_before = bucket.capacity();
        let spilled_before = cap_before > 0;
        let member_name = cmp_name(member);
        match bucket
            .as_slice()
            .binary_search_by(|&m| cmp_name(m).cmp(member_name))
        {
            Ok(pos) => (false, 0, spilled_before, bucket.capacity() > 0, pos),
            Err(pos) => {
                bucket.insert_at(pos, member);
                let cap_after = bucket.capacity();
                let delta = if cap_after > cap_before {
                    let bytes = (cap_after - cap_before) * size_of::<MemberId>();
                    isize::try_from(bytes).expect("bucket spill delta overflow")
                } else {
                    0
                };
                (true, delta, spilled_before, cap_after > 0, pos)
            }
        }
    }

    pub fn remove_by_name<'a, F>(
        &mut self,
        id: BucketId,
        name: &str,
        cmp_name: F,
    ) -> (bool, isize, bool)
    where
        F: Fn(MemberId) -> &'a str,
    {
        let bucket = self.bucket_mut(id);
        match bucket
            .as_slice()
            .binary_search_by(|&m| cmp_name(m).cmp(name))
        {
            Ok(pos) => {
                bucket.remove_at(pos);
                (true, 0, bucket.is_empty())
            }
            Err(_) => (false, 0, false),
        }
    }

    pub fn take_singleton(&mut self, id: BucketId) -> (MemberId, isize) {
        let is_last = (id as usize) + 1 == self.buckets.len();
        let slot = self
            .buckets
            .get_mut(id as usize)
            .expect("invalid bucket id");
        let bucket = slot.take().expect("bucket must exist");
        debug_assert_eq!(bucket.len(), 1, "take_singleton requires len == 1");
        let member = bucket.as_slice()[0];
        let spilled_bytes = bucket.capacity() * size_of::<MemberId>();
        let delta = if spilled_bytes == 0 {
            0
        } else {
            -isize::try_from(spilled_bytes).expect("bucket spill free delta overflow")
        };
        self.free.push(id);
        if is_last {
            self.drop_trailing_empty();
        }
        (member, delta)
    }

    fn drop_trailing_empty(&mut self) -> usize {
        let old_len = self.buckets.len();
        while matches!(self.buckets.last(), Some(None)) {
            self.buckets.pop();
        }
        let new_len = self.buckets.len();
        if new_len < old_len {
            self.buckets.shrink_to_fit();
            self.free.retain(|&id| (id as usize) < new_len);
            if self.free.len() * 4 < self.free.capacity() {
                self.free.shrink_to_fit();
            }
        }
        new_len
    }

    pub fn maybe_shrink(&mut self, id: BucketId, threshold: usize) -> isize {
        let bucket = self.bucket_mut(id);
        bucket.maybe_compact(threshold)
    }

    pub fn capacity_bytes(&self, id: BucketId) -> usize {
        self.bucket(id).capacity() * size_of::<MemberId>()
    }

    pub fn len(&self, id: BucketId) -> usize {
        self.bucket(id).len()
    }

    pub fn advance_front_k(
        &mut self,
        id: BucketId,
        k: usize,
        shrink_threshold: usize,
    ) -> (bool, isize) {
        let remaining;
        {
            let bucket = self.bucket_mut(id);
            let take = bucket.advance_front(k);
            if take == 0 {
                return (false, 0);
            }
            if !bucket.is_empty() {
                let head = bucket.head();
                if head >= shrink_threshold {
                    let total_len = bucket.data.len();
                    let len = bucket.len();
                    if len <= shrink_threshold || head > total_len / 2 {
                        bucket.compact_head();
                    }
                }
            }
            remaining = bucket.len();
        }

        if remaining == 0 {
            let (freed, delta) = self.free_if_empty(id);
            debug_assert!(freed, "emptied bucket must be freed");
            (true, delta)
        } else if remaining == 1 {
            (false, 0)
        } else {
            let delta = self.maybe_shrink(id, shrink_threshold);
            (false, delta)
        }
    }

    #[allow(dead_code)]
    pub fn drain_front_k(
        &mut self,
        id: BucketId,
        k: usize,
        shrink_threshold: usize,
    ) -> (bool, isize) {
        self.advance_front_k(id, k, shrink_threshold)
    }

    pub fn drain_back_k(
        &mut self,
        id: BucketId,
        k: usize,
        shrink_threshold: usize,
    ) -> (bool, isize) {
        let remaining;
        {
            let bucket = self.bucket_mut(id);
            let take = bucket.drain_back(k);
            if take == 0 {
                return (false, 0);
            }
            remaining = bucket.len();
        }

        if remaining == 0 {
            let (freed, delta) = self.free_if_empty(id);
            debug_assert!(freed, "emptied bucket must be freed");
            (true, delta)
        } else if remaining == 1 {
            (false, 0)
        } else {
            let delta = self.maybe_shrink(id, shrink_threshold);
            (false, delta)
        }
    }
}
