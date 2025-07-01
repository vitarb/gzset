#![deny(clippy::uninlined_format_args)]
use std::os::raw::{c_char, c_int, c_void};

use redis_module::{self as rm, raw, Context, RedisError, RedisResult, RedisString, RedisValue};
use std::ffi::CString;

macro_rules! redis_command {
    (
        $ctx:expr,
        $command_name:expr,
        $command_handler:ident,
        $command_flags:expr,
        $firstkey:expr,
        $lastkey:expr,
        $keystep:expr
    ) => {{
        let name = CString::new($command_name).unwrap();
        let flags = CString::new($command_flags).unwrap();

        extern "C" fn __do_command(
            ctx: *mut raw::RedisModuleCtx,
            argv: *mut *mut raw::RedisModuleString,
            argc: c_int,
        ) -> c_int {
            let context = rm::Context::new(ctx);
            let args = rm::decode_args(ctx, argv, argc);
            let response = $command_handler(&context, args);
            context.reply(response.map(|v| v.into())) as c_int
        }

        let status = unsafe {
            raw::RedisModule_CreateCommand.unwrap()(
                $ctx,
                name.as_ptr(),
                Some(__do_command),
                flags.as_ptr(),
                $firstkey,
                $lastkey,
                $keystep,
            )
        };
        if status == raw::Status::Err as c_int {
            Err(rm::RedisError::Str("command registration failed"))
        } else {
            Ok(())
        }
    }};
}

const REDISMODULE_API_VERSION: c_int = raw::REDISMODULE_APIVER_1 as c_int;

/// Convenient result type used throughout the crate.
pub type Result<T = RedisValue> = RedisResult<T>;

static GZSET_TYPE: rm::native_types::RedisType = rm::native_types::RedisType::new(
    "gzsetmod1",
    0,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: Some(gzset_rdb_load),
        rdb_save: Some(gzset_rdb_save),
        aof_rewrite: None,
        free: None,

        // Currently unused by Redis
        mem_usage: None,
        digest: None,

        // Aux data callbacks
        aux_load: None,
        aux_save: None,
        aux_save2: None,
        aux_save_triggers: 0,

        free_effort: None,
        unlink: None,
        copy: None,
        defrag: None,

        copy2: None,
        free_effort2: None,
        mem_usage2: None,
        unlink2: None,
    },
);

unsafe extern "C" fn gzset_rdb_load(_io: *mut raw::RedisModuleIO, _encver: c_int) -> *mut c_void {
    std::ptr::null_mut()
}

unsafe extern "C" fn gzset_rdb_save(_io: *mut raw::RedisModuleIO, _value: *mut c_void) {}

use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Mutex;

#[derive(Default)]
pub struct ScoreSet {
    by_score: BTreeMap<OrderedFloat<f64>, BTreeSet<String>>,
    members: HashMap<String, OrderedFloat<f64>>,
}

impl ScoreSet {
    pub fn insert(&mut self, score: f64, member: String) -> bool {
        let key = OrderedFloat(score);
        match self.members.insert(member.clone(), key) {
            Some(old) if old == key => return false,
            Some(old) => {
                if let Some(set) = self.by_score.get_mut(&old) {
                    set.remove(&member);
                    if set.is_empty() {
                        self.by_score.remove(&old);
                    }
                }
            }
            None => {}
        }
        self.by_score.entry(key).or_default().insert(member);
        true
    }

    pub fn remove(&mut self, member: &str) -> bool {
        if let Some(score) = self.members.remove(member) {
            if let Some(set) = self.by_score.get_mut(&score) {
                set.remove(member);
                if set.is_empty() {
                    self.by_score.remove(&score);
                }
            }
            true
        } else {
            false
        }
    }

    pub fn score(&self, member: &str) -> Option<f64> {
        self.members.get(member).map(|s| s.0)
    }

    pub fn rank(&self, member: &str) -> Option<usize> {
        let target = *self.members.get(member)?;
        let mut idx = 0usize;
        for (score, set) in &self.by_score {
            if *score == target {
                for m in set {
                    if m == member {
                        return Some(idx);
                    }
                    idx += 1;
                }
            } else {
                idx += set.len();
            }
        }
        None
    }

    pub fn range_iter(&self, start: isize, stop: isize) -> Vec<(f64, String)> {
        let len = self.members.len() as isize;
        if len == 0 {
            return Vec::new();
        }
        let mut start = if start < 0 { len + start } else { start };
        let mut stop = if stop < 0 { len + stop } else { stop };
        if start < 0 {
            start = 0;
        }
        if stop < 0 {
            return Vec::new();
        }
        if stop >= len {
            stop = len - 1;
        }
        if start > stop {
            return Vec::new();
        }
        let mut idx = 0isize;
        let mut out = Vec::new();
        for (score, set) in &self.by_score {
            for member in set {
                if idx >= start && idx <= stop {
                    out.push((score.0, member.clone()));
                }
                idx += 1;
                if idx > stop {
                    return out;
                }
            }
        }
        out
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }
}

/// Map of key name to sorted set data.
type SetsMap = BTreeMap<String, ScoreSet>;

static SETS: once_cell::sync::Lazy<Mutex<SetsMap>> =
    once_cell::sync::Lazy::new(|| Mutex::new(BTreeMap::new()));

fn gzadd(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 4 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].to_string_lossy();
    let score: f64 = args[2].parse_float()?;
    let member = args[3].to_string_lossy();

    let mut sets = SETS.lock().unwrap();
    let set = sets.entry(key).or_default();
    let added = set.insert(score, member);
    Ok((added as i64).into())
}

fn gzrank(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 3 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].to_string_lossy();
    let member = args[2].to_string_lossy();
    let sets = SETS.lock().unwrap();
    if let Some(set) = sets.get(&key) {
        if let Some(rank) = set.rank(&member) {
            return Ok((rank as i64).into());
        }
    }
    Ok(RedisValue::Null)
}

fn gzrange(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 4 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].to_string_lossy();
    let parse_index = |arg: &RedisString| -> Result<isize> {
        arg.to_string_lossy()
            .parse::<isize>()
            .map_err(|_| RedisError::Str("ERR index is not an integer or out of range"))
    };
    let start = parse_index(&args[2])?;
    let stop = parse_index(&args[3])?;
    let sets = SETS.lock().unwrap();
    if let Some(set) = sets.get(&key) {
        let vals: Vec<RedisValue> = set
            .range_iter(start, stop)
            .into_iter()
            .map(|(_, m)| m.into())
            .collect();
        return Ok(RedisValue::Array(vals));
    }
    Ok(RedisValue::Array(Vec::new()))
}

fn gzrem(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 3 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].to_string_lossy();
    let member = args[2].to_string_lossy();
    let mut sets = SETS.lock().unwrap();
    if let Some(set) = sets.get_mut(&key) {
        let removed = set.remove(&member);
        if set.is_empty() {
            sets.remove(&key);
        }
        return Ok((removed as i64).into());
    }
    Ok(0i64.into())
}

fn gzscore(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 3 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].to_string_lossy();
    let member = args[2].to_string_lossy();
    let sets = SETS.lock().unwrap();
    if let Some(set) = sets.get(&key) {
        if let Some(score) = set.score(&member) {
            return Ok(score.into());
        }
    }
    Ok(RedisValue::Null)
}

fn gzcard(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 2 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].to_string_lossy();
    let sets = SETS.lock().unwrap();
    if let Some(set) = sets.get(&key) {
        return Ok((set.members.len() as i64).into());
    }
    Ok(0i64.into())
}

/// Module initialization function called by Valkey/Redis on module load.
///
/// # Safety
///
/// The caller must provide valid pointers as expected by the Valkey module API.
#[no_mangle]
pub unsafe extern "C" fn gzset_on_load(
    ctx: *mut raw::RedisModuleCtx,
    _argv: *mut *mut raw::RedisModuleString,
    _argc: c_int,
) -> c_int {
    let module_name = b"gzset\0";
    unsafe {
        if raw::Export_RedisModule_Init(
            ctx,
            module_name.as_ptr().cast::<c_char>(),
            1,
            REDISMODULE_API_VERSION,
        ) == raw::Status::Err as c_int
        {
            return raw::Status::Err as c_int;
        }
    }

    let result: rm::RedisResult<()> = (|| {
        if GZSET_TYPE.create_data_type(ctx).is_err() {
            return Err(rm::RedisError::Str("datatype"));
        }

        redis_command!(ctx, "GZADD", gzadd, "write fast", 1, 1, 1)?;
        redis_command!(ctx, "GZRANK", gzrank, "readonly fast", 1, 1, 1)?;
        redis_command!(ctx, "GZRANGE", gzrange, "readonly fast", 1, 1, 1)?;
        redis_command!(ctx, "GZREM", gzrem, "write fast", 1, 1, 1)?;
        redis_command!(ctx, "GZSCORE", gzscore, "readonly fast", 1, 1, 1)?;
        redis_command!(ctx, "GZCARD", gzcard, "readonly fast", 1, 1, 1)?;
        Ok(())
    })();

    if result.is_err() {
        return raw::Status::Err as c_int;
    }

    raw::Status::Ok as c_int
}

/// Entrypoint called by Redis when loading the module.
///
/// # Safety
///
/// The `ctx` pointer and argument list must be valid as provided by Redis.
#[no_mangle]
pub unsafe extern "C" fn RedisModule_OnLoad(
    ctx: *mut raw::RedisModuleCtx,
    argv: *mut *mut raw::RedisModuleString,
    argc: c_int,
) -> c_int {
    gzset_on_load(ctx, argv, argc)
}

/// Entrypoint called by Valkey when loading the module.
///
/// # Safety
///
/// The `ctx` pointer and arguments must be valid as provided by Valkey.
#[no_mangle]
pub unsafe extern "C" fn ValkeyModule_OnLoad(
    ctx: *mut raw::RedisModuleCtx,
    argv: *mut *mut raw::RedisModuleString,
    argc: c_int,
) -> c_int {
    gzset_on_load(ctx, argv, argc)
}

/// Optional unload function called when the module is unloaded.
///
/// # Safety
///
/// Called by Valkey when unloading the module. The provided context must be valid.
#[no_mangle]
pub unsafe extern "C" fn gzset_on_unload(_ctx: *mut c_void) {
    // Clean-up logic would go here.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_insert_and_range() {
        let mut set = ScoreSet::default();
        assert!(set.insert(2.0, "b".to_string()));
        assert!(set.insert(1.0, "a".to_string()));
        assert!(!set.insert(1.0, "a".to_string()));
        let all = set.range_iter(0, -1);
        assert_eq!(all, vec![(1.0, "a".to_string()), (2.0, "b".to_string())]);
    }

    #[test]
    fn negative_indices() {
        let mut set = ScoreSet::default();
        for i in 0..5 {
            set.insert(i as f64, format!("m{i}"));
        }
        let out = set.range_iter(-2, -1);
        assert_eq!(out[0].1, "m3");
        assert_eq!(out[1].1, "m4");
    }
}
