use crate::score_set::ScoreSet;
use std::cell::RefCell;
use std::collections::HashMap;

thread_local! {
    static KEYSPACE: RefCell<HashMap<String, ScoreSet>> = RefCell::new(HashMap::new());
}

pub fn with_write<F, R>(key: &str, f: F) -> R
where
    F: FnOnce(&mut ScoreSet) -> R,
{
    KEYSPACE.with(|cell| {
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
    KEYSPACE.with(|cell| {
        let map = cell.borrow();
        f(map.get(key).unwrap_or(&ScoreSet::default()))
    })
}

/// Remove all stored sets, typically in response to FLUSHDB/FLUSHALL events.
pub fn clear_all() {
    KEYSPACE.with(|cell| cell.borrow_mut().clear());
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_clears_map() {
        with_write("k", |s| {
            s.insert(1.0, "a");
        });
        clear_all();
        let len = with_read("k", |s| s.members.len());
        assert_eq!(len, 0);
    }
}
