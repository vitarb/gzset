use std::{convert::TryFrom, mem::size_of};

use smallvec::SmallVec;

use crate::pool::MemberId;

pub type BucketId = u32;

const INLINE: usize = 4;

#[derive(Default, Debug)]
pub struct BucketStore {
    buckets: Vec<Option<SmallVec<[MemberId; INLINE]>>>,
    free: Vec<BucketId>,
}

impl BucketStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn bucket(&self, id: BucketId) -> &SmallVec<[MemberId; INLINE]> {
        self.buckets
            .get(id as usize)
            .and_then(|slot| slot.as_ref())
            .expect("invalid bucket id")
    }

    fn bucket_mut(&mut self, id: BucketId) -> &mut SmallVec<[MemberId; INLINE]> {
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
            *slot = Some(SmallVec::new());
            id
        } else {
            let idx = self.buckets.len();
            let id = BucketId::try_from(idx).expect("too many buckets allocated");
            self.buckets.push(Some(SmallVec::new()));
            id
        }
    }

    pub fn free_if_empty(&mut self, id: BucketId) -> bool {
        let slot = self
            .buckets
            .get_mut(id as usize)
            .expect("invalid bucket id");
        if let Some(bucket) = slot {
            if bucket.is_empty() {
                *slot = None;
                self.free.push(id);
                return true;
            }
        }
        false
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
        let spilled_before = bucket.spilled();
        let member_name = cmp_name(member);
        match bucket.binary_search_by(|&m| cmp_name(m).cmp(member_name)) {
            Ok(pos) => (false, 0, spilled_before, bucket.spilled(), pos),
            Err(pos) => {
                bucket.insert(pos, member);
                let spilled_after = bucket.spilled();
                let mut delta = 0isize;
                if !spilled_before && spilled_after {
                    let bytes = bucket.capacity() * size_of::<MemberId>();
                    delta = isize::try_from(bytes).expect("bucket spill delta overflow");
                }
                (true, delta, spilled_before, spilled_after, pos)
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

    pub fn maybe_shrink(&mut self, id: BucketId, threshold: usize) -> isize {
        let bucket = self.bucket_mut(id);
        if bucket.spilled() && bucket.len() <= threshold {
            let bytes = bucket.capacity() * size_of::<MemberId>();
            bucket.shrink_to_fit();
            let bytes = isize::try_from(bytes).expect("bucket shrink delta overflow");
            -bytes
        } else {
            0
        }
    }

    pub fn capacity_bytes(&self, id: BucketId) -> usize {
        let bucket = self.bucket(id);
        if bucket.spilled() {
            bucket.capacity() * size_of::<MemberId>()
        } else {
            0
        }
    }

    pub fn len(&self, id: BucketId) -> usize {
        self.bucket(id).len()
    }
}
