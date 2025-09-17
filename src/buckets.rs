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

pub type Bucket = Vec<MemberId>;

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

    pub fn alloc(&mut self) -> BucketId {
        if let Some(id) = self.free.pop() {
            let slot = self
                .buckets
                .get_mut(id as usize)
                .expect("reused bucket id out of bounds");
            debug_assert!(slot.is_none(), "reused bucket slot must be empty");
            *slot = Some(Vec::new());
            id
        } else {
            let idx = self.buckets.len();
            let id = BucketId::try_from(idx).expect("too many buckets allocated");
            self.buckets.push(Some(Vec::new()));
            id
        }
    }

    pub fn free_if_empty(&mut self, id: BucketId) -> (bool, isize) {
        let slot = self
            .buckets
            .get_mut(id as usize)
            .expect("invalid bucket id");
        if let Some(bucket) = slot {
            if bucket.is_empty() {
                let spilled_bytes = bucket.capacity() * size_of::<MemberId>();
                *slot = None;
                self.free.push(id);
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
        match bucket.binary_search_by(|&m| cmp_name(m).cmp(member_name)) {
            Ok(pos) => (false, 0, spilled_before, bucket.capacity() > 0, pos),
            Err(pos) => {
                bucket.insert(pos, member);
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
        match bucket.binary_search_by(|&m| cmp_name(m).cmp(name)) {
            Ok(pos) => {
                bucket.remove(pos);
                (true, 0, bucket.is_empty())
            }
            Err(_) => (false, 0, false),
        }
    }

    pub fn take_singleton(&mut self, id: BucketId) -> (MemberId, isize) {
        let slot = self
            .buckets
            .get_mut(id as usize)
            .expect("invalid bucket id");
        let bucket = slot.take().expect("bucket must exist");
        debug_assert_eq!(bucket.len(), 1, "take_singleton requires len == 1");
        let member = bucket[0];
        let spilled_bytes = bucket.capacity() * size_of::<MemberId>();
        let delta = if spilled_bytes == 0 {
            0
        } else {
            -isize::try_from(spilled_bytes).expect("bucket spill free delta overflow")
        };
        self.free.push(id);
        (member, delta)
    }

    pub fn maybe_shrink(&mut self, id: BucketId, threshold: usize) -> isize {
        let bucket = self.bucket_mut(id);
        if bucket.len() <= threshold {
            let cap_before = bucket.capacity();
            bucket.shrink_to_fit();
            let cap_after = bucket.capacity();
            if cap_after < cap_before {
                let bytes = (cap_before - cap_after) * size_of::<MemberId>();
                -isize::try_from(bytes).expect("bucket shrink delta overflow")
            } else {
                0
            }
        } else {
            0
        }
    }

    pub fn capacity_bytes(&self, id: BucketId) -> usize {
        self.bucket(id).capacity() * size_of::<MemberId>()
    }

    pub fn len(&self, id: BucketId) -> usize {
        self.bucket(id).len()
    }
}
