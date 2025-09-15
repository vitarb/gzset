#[cfg(not(feature = "fast-hash"))]
use hashbrown::hash_map::DefaultHashBuilder;
use hashbrown::HashMap;
#[cfg(feature = "fast-hash")]
use rustc_hash::FxHasher;
#[cfg(feature = "fast-hash")]
use std::hash::BuildHasherDefault;
use std::ptr::NonNull;

#[cfg(feature = "fast-hash")]
/// FxHasher-based map used only when the `fast-hash` feature is enabled.
type FastHashBuilder = BuildHasherDefault<FxHasher>;
#[cfg(not(feature = "fast-hash"))]
/// Default to `AHash` for DOS-resistant hashing of user-provided names.
type FastHashBuilder = DefaultHashBuilder;
/// Hash map implementation used by the string pool.
pub type FastHashMap<K, V> = HashMap<K, V, FastHashBuilder>;
pub type MemberId = u32;

#[derive(Default, Debug)]
pub struct StringPool {
    pub(crate) map: FastHashMap<Box<str>, MemberId>,
    pub(crate) strings: Vec<Option<NonNull<str>>>,
    pub(crate) free_ids: Vec<MemberId>,
}

impl StringPool {
    pub fn intern(&mut self, s: &str) -> MemberId {
        if let Some(&id) = self.map.get(s) {
            id
        } else {
            let boxed: Box<str> = s.to_owned().into_boxed_str();
            let ptr = NonNull::from(boxed.as_ref());
            let id = if let Some(id) = self.free_ids.pop() {
                self.strings[id as usize] = Some(ptr);
                id
            } else {
                let id = self.strings.len() as MemberId;
                self.strings.push(Some(ptr));
                id
            };
            self.map.insert(boxed, id);
            id
        }
    }

    pub fn lookup(&self, s: &str) -> Option<MemberId> {
        self.map.get(s).copied()
    }

    pub fn get(&self, id: MemberId) -> &str {
        // SAFETY: `self.strings` stores pointers to allocations owned by `self.map`.
        // Entries are cleared when removed, so dereferencing here is valid for live IDs.
        unsafe {
            self.strings[id as usize]
                .as_ref()
                .expect("invalid member id")
                .as_ref()
        }
    }

    pub fn remove(&mut self, s: &str) -> Option<MemberId> {
        if let Some(id) = self.map.remove(s) {
            self.strings[id as usize] = None;
            self.free_ids.push(id);
            Some(id)
        } else {
            None
        }
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn allocated_ids(&self) -> usize {
        self.strings.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{memory::gzset_mem_usage, score_set::ScoreSet};
    use std::os::raw::c_void;

    #[test]
    fn test_stringpool_reuse_and_reclaim() {
        const N: usize = 100;
        let mut pool = StringPool::default();
        let mut ids = Vec::new();
        for i in 0..N {
            ids.push(pool.intern(&format!("m{i}")));
        }
        assert_eq!(pool.len(), N);
        assert_eq!(pool.allocated_ids(), N);

        for i in 0..N {
            assert!(pool.remove(&format!("m{i}")).is_some());
        }
        assert_eq!(pool.len(), 0);
        assert_eq!(pool.allocated_ids(), N);

        let mut ids2 = Vec::new();
        for i in 0..N {
            ids2.push(pool.intern(&format!("m{i}")));
        }
        assert_eq!(pool.len(), N);
        assert_eq!(pool.allocated_ids(), N);
        let mut a = ids.clone();
        let mut b = ids2.clone();
        a.sort_unstable();
        b.sort_unstable();
        assert_eq!(a, b, "ids should be reused");

        // churn test using ScoreSet and memory accounting
        let mut set = ScoreSet::default();
        let members: Vec<String> = (0..10_000).map(|i| format!("x{i}")).collect();
        unsafe {
            let mut baseline = None;
            for _ in 0..5 {
                for (i, m) in members.iter().enumerate() {
                    set.insert(i as f64, m);
                }
                for m in &members {
                    assert!(set.remove(m));
                }
                let usage = gzset_mem_usage((&set as *const ScoreSet) as *const c_void);
                baseline = match baseline {
                    None => Some(usage),
                    Some(b) => {
                        assert!(
                            (usage as f64) <= (b as f64 * 1.1)
                                && (usage as f64) >= (b as f64 * 0.9),
                            "usage {usage} baseline {b}"
                        );
                        Some(b)
                    }
                };
            }
        }
    }
}
