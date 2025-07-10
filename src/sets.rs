use crate::FastHashMap;
use std::cell::RefCell;

use crate::ScoreSet;

thread_local! {
    static SETS: RefCell<FastHashMap<String, ScoreSet>> = RefCell::new(FastHashMap::default());
}

pub fn with_write<F, R>(key: &str, f: F) -> R
where
    F: FnOnce(&mut ScoreSet) -> R,
{
    SETS.with(|cell| {
        let mut map = cell.borrow_mut();
        let result;
        {
            let set = map.entry(key.to_owned()).or_default();
            result = f(set);
            if set.is_empty() {
                map.remove(key);
            }
        }
        result
    })
}

pub fn with_read<F, R>(key: &str, f: F) -> R
where
    F: FnOnce(&ScoreSet) -> R,
{
    SETS.with(|cell| {
        let map = cell.borrow();
        f(map.get(key).unwrap_or(&ScoreSet::default()))
    })
}
