use hashbrown::HashMap;
use rustc_hash::FxHasher;
use std::hash::BuildHasherDefault;

pub type FastHashMap<K, V> = HashMap<K, V, BuildHasherDefault<FxHasher>>;
pub type MemberId = u32;

#[derive(Default, Debug)]
pub struct StringPool {
    pub(crate) map: FastHashMap<&'static str, MemberId>,
    pub(crate) strings: Vec<Box<str>>,
}

impl StringPool {
    pub fn intern(&mut self, s: &str) -> MemberId {
        if let Some(&id) = self.map.get(s) {
            id
        } else {
            let boxed: Box<str> = s.to_owned().into_boxed_str();
            let ptr: &'static str = unsafe {
                // SAFETY: `boxed` is moved into `self.strings` and never freed
                // individually, so this pointer remains valid for the life of
                // the pool and can be treated as `'static`.
                &*(boxed.as_ref() as *const str)
            };
            let id = self.strings.len() as MemberId;
            self.strings.push(boxed);
            self.map.insert(ptr, id);
            id
        }
    }

    pub fn lookup(&self, s: &str) -> Option<MemberId> {
        self.map.get(s).copied()
    }

    pub fn get(&self, id: MemberId) -> &str {
        &self.strings[id as usize]
    }

    pub fn get_static(&self, id: MemberId) -> &'static str {
        let s: &str = &self.strings[id as usize];
        unsafe {
            // SAFETY: entries in `self.strings` live for the duration of the
            // pool, so casting the borrowed string to `'static` is sound.
            &*(s as *const str)
        }
    }
}
