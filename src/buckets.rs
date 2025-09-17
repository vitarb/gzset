use std::{convert::TryFrom, mem, mem::size_of};

use smallvec::SmallVec;

use crate::pool::MemberId;

pub type BucketId = u32;

const INLINE: usize = 4;

#[derive(Debug)]
pub(crate) enum Bucket {
    Inline(SmallVec<[MemberId; INLINE]>),
    Heap(Vec<MemberId>),
}

impl Bucket {
    #[inline]
    fn new_inline() -> Self {
        Self::Inline(SmallVec::new())
    }

    #[inline]
    fn is_heap(&self) -> bool {
        matches!(self, Self::Heap(_))
    }

    #[inline]
    fn len(&self) -> usize {
        match self {
            Self::Inline(inline) => inline.len(),
            Self::Heap(heap) => heap.len(),
        }
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    fn capacity_bytes(&self) -> usize {
        match self {
            Self::Inline(_) => 0,
            Self::Heap(heap) => heap.capacity() * size_of::<MemberId>(),
        }
    }

    #[inline]
    fn as_slice(&self) -> &[MemberId] {
        match self {
            Self::Inline(inline) => inline.as_slice(),
            Self::Heap(heap) => heap.as_slice(),
        }
    }
}

impl Default for Bucket {
    fn default() -> Self {
        Self::new_inline()
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

    pub fn alloc(&mut self) -> BucketId {
        if let Some(id) = self.free.pop() {
            let slot = self
                .buckets
                .get_mut(id as usize)
                .expect("reused bucket id out of bounds");
            debug_assert!(slot.is_none(), "reused bucket slot must be empty");
            *slot = Some(Bucket::new_inline());
            id
        } else {
            let idx = self.buckets.len();
            let id = BucketId::try_from(idx).expect("too many buckets allocated");
            self.buckets.push(Some(Bucket::new_inline()));
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
                let spilled_bytes = bucket.capacity_bytes();
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
        let spilled_before = bucket.is_heap();
        let member_name = cmp_name(member);
        match bucket {
            Bucket::Inline(inline) => match inline
                .binary_search_by(|&m| cmp_name(m).cmp(member_name))
            {
                Ok(pos) => (false, 0, spilled_before, false, pos),
                Err(pos) => {
                    if inline.len() == INLINE {
                        let existing = mem::take(inline);
                        let mut heap = Vec::with_capacity(INLINE * 2);
                        heap.extend(existing);
                        heap.insert(pos, member);
                        let bytes = heap.capacity() * size_of::<MemberId>();
                        let delta = isize::try_from(bytes).expect("bucket spill delta overflow");
                        *bucket = Bucket::Heap(heap);
                        (true, delta, spilled_before, true, pos)
                    } else {
                        inline.insert(pos, member);
                        (true, 0, spilled_before, false, pos)
                    }
                }
            },
            Bucket::Heap(heap) => match heap.binary_search_by(|&m| cmp_name(m).cmp(member_name)) {
                Ok(pos) => (false, 0, spilled_before, true, pos),
                Err(pos) => {
                    let cap_before = heap.capacity();
                    heap.insert(pos, member);
                    let cap_after = heap.capacity();
                    let delta = if cap_after > cap_before {
                        let bytes = (cap_after - cap_before) * size_of::<MemberId>();
                        isize::try_from(bytes).expect("bucket spill delta overflow")
                    } else {
                        0
                    };
                    (true, delta, spilled_before, true, pos)
                }
            },
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
        match bucket {
            Bucket::Inline(inline) => match inline.binary_search_by(|&m| cmp_name(m).cmp(name)) {
                Ok(pos) => {
                    inline.remove(pos);
                    (true, 0, inline.is_empty())
                }
                Err(_) => (false, 0, false),
            },
            Bucket::Heap(heap) => match heap.binary_search_by(|&m| cmp_name(m).cmp(name)) {
                Ok(pos) => {
                    heap.remove(pos);
                    (true, 0, heap.is_empty())
                }
                Err(_) => (false, 0, false),
            },
        }
    }

    pub fn maybe_shrink(&mut self, id: BucketId, threshold: usize) -> isize {
        let bucket = self.bucket_mut(id);
        let limit = threshold.min(INLINE);
        let should_shrink = matches!(bucket, Bucket::Heap(heap) if heap.len() <= limit);
        if should_shrink {
            let old_bucket = mem::take(bucket);
            if let Bucket::Heap(old_heap) = old_bucket {
                let bytes = old_heap.capacity() * size_of::<MemberId>();
                if let Bucket::Inline(inline) = bucket {
                    inline.extend(old_heap);
                } else {
                    unreachable!("bucket must be inline after replacement");
                }
                let bytes = isize::try_from(bytes).expect("bucket shrink delta overflow");
                -bytes
            } else {
                unreachable!("expected heap bucket when shrinking");
            }
        } else {
            0
        }
    }

    pub fn capacity_bytes(&self, id: BucketId) -> usize {
        self.bucket(id).capacity_bytes()
    }

    pub fn len(&self, id: BucketId) -> usize {
        self.bucket(id).len()
    }
}
