use crate::score_set::ScoreSet;
use once_cell::sync::Lazy;
use redis_module::Context;
use std::collections::HashMap;
use std::sync::Mutex;

static KEYSPACE: Lazy<Mutex<HashMap<String, ScoreSet>>> = Lazy::new(|| Mutex::new(HashMap::new()));

pub fn with_write<F, R>(ctx: Option<&Context>, key: &str, f: F) -> R
where
    F: FnOnce(&mut ScoreSet) -> R,
{
    let mut map = KEYSPACE.lock().unwrap();
    let existed = map.contains_key(key);
    let set = map.entry(key.to_owned()).or_default();
    let result = f(set);
    if !existed && !set.is_empty() {
        if let Some(c) = ctx {
            let k = c.create_string(key);
            let redis_key = c.open_key_writable(&k);
            let _ = redis_key.set_value(
                &crate::command::GZSET_TYPE,
                crate::memory::ScoreSetRef {
                    key: key.to_owned(),
                },
            );
        }
    }
    if set.is_empty() {
        map.remove(key);
        if let Some(c) = ctx {
            let k = c.create_string(key);
            let redis_key = c.open_key_writable(&k);
            let _ = redis_key.delete();
        }
    }
    result
}

pub fn with_read<F, R>(key: &str, f: F) -> R
where
    F: FnOnce(&ScoreSet) -> R,
{
    let map = KEYSPACE.lock().unwrap();
    f(map.get(key).unwrap_or(&ScoreSet::default()))
}

/// Remove all stored sets, typically in response to FLUSHDB/FLUSHALL events.
pub fn clear_all() {
    KEYSPACE.lock().unwrap().clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_clears_map() {
        with_write(None, "k", |s| {
            s.insert(1.0, "a");
        });
        assert_eq!(with_read("k", |s| s.members.len()), 1);
        clear_all();
        assert_eq!(with_read("k", |s| s.members.len()), 0);
    }
}
