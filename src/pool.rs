use hashbrown::{raw::RawTable, HashMap};
use std::convert::TryInto;
use std::fmt;
use std::hash::{BuildHasher, Hasher};
use std::str;

#[cfg(feature = "fast-hash")]
use rustc_hash::FxHasher;
#[cfg(feature = "fast-hash")]
use std::hash::BuildHasherDefault;

#[cfg(not(feature = "fast-hash"))]
use ahash::RandomState;

#[cfg(feature = "fast-hash")]
type Build = BuildHasherDefault<FxHasher>;
#[cfg(not(feature = "fast-hash"))]
type Build = RandomState;

/// Hash map implementation used by the string pool and other helpers.
pub type FastHashMap<K, V> = HashMap<K, V, Build>;

pub type MemberId = u32;

// Encodes location inside the arena.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) struct Loc {
    chunk: u32,
    off: u32,
    len: u32,
}

// Entry we store in RawTable. No owned strings.
pub(crate) struct KeyEntry {
    hash: u64,
    loc: Loc,
    id: MemberId,
}

// Arena parameters
const ARENA_CHUNK: usize = 4 * 1024 * 1024; // 4 MiB, tune if needed

pub struct StringPool {
    hasher: Build,
    // Big append-only chunks for string bytes
    pub(crate) arena: Vec<Box<[u8]>>,
    // Current write head into last chunk
    write_chunk: usize, // index into arena
    write_off: usize,   // offset into arena[write_chunk]
    // Key lookup table: compares by bytes in arena
    pub(crate) table: RawTable<KeyEntry>,
    // id -> Loc mapping (None when freed)
    pub(crate) index: Vec<Option<Loc>>,
    // freelist of reusable ids
    pub(crate) free_ids: Vec<MemberId>,
    // Fast length (live members)
    len: usize,
}

impl Default for StringPool {
    fn default() -> Self {
        Self {
            hasher: Build::default(),
            arena: Vec::new(),
            write_chunk: 0,
            write_off: 0,
            table: RawTable::new(),
            index: Vec::new(),
            free_ids: Vec::new(),
            len: 0,
        }
    }
}

impl fmt::Debug for StringPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StringPool")
            .field("arena_chunks", &self.arena.len())
            .field("write_chunk", &self.write_chunk)
            .field("write_off", &self.write_off)
            .field("len", &self.len)
            .field("allocated_ids", &self.index.len())
            .finish()
    }
}

impl StringPool {
    pub fn intern(&mut self, s: &str) -> MemberId {
        let bytes = s.as_bytes();
        let hash = self.hash_bytes(bytes);
        if let Some(entry) = self
            .table
            .get(hash, |entry| self.loc_bytes(entry.loc) == bytes)
        {
            return entry.id;
        }

        let loc = self.write_bytes(bytes);
        let id = if let Some(id) = self.free_ids.pop() {
            self.index[id as usize] = Some(loc);
            id
        } else {
            let idx = self.index.len();
            let id: MemberId = idx.try_into().expect("too many members in string pool");
            self.index.push(Some(loc));
            id
        };

        self.table
            .insert(hash, KeyEntry { hash, loc, id }, |entry| entry.hash);
        self.len += 1;
        id
    }

    pub fn lookup(&self, s: &str) -> Option<MemberId> {
        let bytes = s.as_bytes();
        let hash = self.hash_bytes(bytes);
        self.table
            .get(hash, |entry| self.loc_bytes(entry.loc) == bytes)
            .map(|entry| entry.id)
    }

    pub fn get(&self, id: MemberId) -> &str {
        let loc = self
            .index
            .get(id as usize)
            .and_then(|loc| loc.as_ref())
            .copied()
            .expect("invalid member id");
        self.loc_str(loc)
    }

    pub fn remove(&mut self, s: &str) -> Option<MemberId> {
        let bytes = s.as_bytes();
        let hash = self.hash_bytes(bytes);
        let (id, loc) = match self
            .table
            .get(hash, |entry| self.loc_bytes(entry.loc) == bytes)
        {
            Some(entry) => (entry.id, entry.loc),
            None => return None,
        };
        let removed = self
            .table
            .remove_entry(hash, |entry| entry.id == id && entry.loc == loc);
        debug_assert!(removed.is_some(), "entry must exist when removing");
        self.index[id as usize] = None;
        self.free_ids.push(id);
        self.len -= 1;
        Some(id)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn allocated_ids(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, MemberId)> + '_ {
        self.index.iter().enumerate().filter_map(move |(idx, loc)| {
            let loc = loc.as_ref()?;
            let id: MemberId = idx.try_into().expect("too many members in string pool");
            Some((self.loc_str(*loc), id))
        })
    }

    fn hash_bytes(&self, bytes: &[u8]) -> u64 {
        let mut state = self.hasher.build_hasher();
        state.write(bytes);
        state.finish()
    }

    fn write_bytes(&mut self, bytes: &[u8]) -> Loc {
        self.ensure_capacity(bytes.len());
        let chunk_idx = self.write_chunk;
        let start = self.write_off;
        let end = start
            .checked_add(bytes.len())
            .expect("string pool offset overflow");
        if !bytes.is_empty() {
            self.arena[chunk_idx][start..end].copy_from_slice(bytes);
        }
        let loc = Loc {
            chunk: chunk_idx
                .try_into()
                .expect("too many chunks in string pool"),
            off: start
                .try_into()
                .expect("chunk offset exceeded supported range"),
            len: bytes
                .len()
                .try_into()
                .expect("string exceeds supported length"),
        };
        self.write_off = end;
        loc
    }

    fn ensure_capacity(&mut self, needed: usize) {
        if self.arena.is_empty() {
            self.add_chunk(needed);
            return;
        }
        let chunk_len = self.arena[self.write_chunk].len();
        let end = self
            .write_off
            .checked_add(needed)
            .expect("string pool offset overflow");
        if end > chunk_len {
            self.add_chunk(needed);
        }
    }

    fn add_chunk(&mut self, needed: usize) {
        let size = ARENA_CHUNK.max(needed);
        let chunk = vec![0u8; size].into_boxed_slice();
        self.arena.push(chunk);
        self.write_chunk = self.arena.len() - 1;
        self.write_off = 0;
    }

    fn loc_bytes(&self, loc: Loc) -> &[u8] {
        let chunk_idx = loc.chunk as usize;
        let off = loc.off as usize;
        let len = loc.len as usize;
        let end = off.checked_add(len).expect("string pool location overflow");
        let chunk = &self.arena[chunk_idx];
        &chunk[off..end]
    }

    fn loc_str(&self, loc: Loc) -> &str {
        // SAFETY: bytes stored in the arena originate from valid UTF-8 strings.
        unsafe { str::from_utf8_unchecked(self.loc_bytes(loc)) }
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
