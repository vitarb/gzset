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

use dashmap::DashMap;
use ordered_float::OrderedFloat;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, RwLock};

#[derive(Default)]
pub struct ScoreSet {
    by_score: BTreeMap<OrderedFloat<f64>, BTreeSet<String>>,
    members: HashMap<String, OrderedFloat<f64>>,
}

impl ScoreSet {
    pub fn insert(&mut self, score: f64, member: &str) -> bool {
        let key = OrderedFloat(score);
        match self.members.insert(member.to_owned(), key) {
            Some(old) if old == key => return false,
            Some(old) => {
                if let Some(set) = self.by_score.get_mut(&old) {
                    set.remove(member);
                    if set.is_empty() {
                        self.by_score.remove(&old);
                    }
                }
            }
            None => {}
        }
        self.by_score
            .entry(key)
            .or_default()
            .insert(member.to_owned());
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

    pub fn all_items(&self) -> Vec<(f64, String)> {
        let mut out = Vec::new();
        for (score, set) in &self.by_score {
            for m in set {
                out.push((score.0, m.clone()));
            }
        }
        out
    }
}

/// Map of key name to sorted set data.
type SetsMap = DashMap<String, Arc<RwLock<ScoreSet>>>;

static SETS: once_cell::sync::Lazy<SetsMap> = once_cell::sync::Lazy::new(DashMap::new);

fn gzadd(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 4 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let score: f64 = args[2].parse_float()?;
    let member = args[3].try_as_str()?;

    let entry = SETS
        .entry(key.to_owned())
        .or_insert_with(|| Arc::new(RwLock::new(ScoreSet::default())));
    let mut set = entry.write().unwrap();
    let added = set.insert(score, member);
    Ok((added as i64).into())
}

fn gzrank(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 3 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let member = args[2].try_as_str()?;
    if let Some(set) = SETS.get(key) {
        if let Some(rank) = set.value().read().unwrap().rank(member) {
            return Ok((rank as i64).into());
        }
    }
    Ok(RedisValue::Null)
}

fn gzrange(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 4 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let parse_index = |arg: &RedisString| -> Result<isize> {
        arg.to_string_lossy()
            .parse::<isize>()
            .map_err(|_| RedisError::Str("ERR index is not an integer or out of range"))
    };
    let start = parse_index(&args[2])?;
    let stop = parse_index(&args[3])?;
    if let Some(set) = SETS.get(key) {
        let vals: Vec<RedisValue> = set
            .value()
            .read()
            .unwrap()
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
    let key = args[1].try_as_str()?;
    let member = args[2].try_as_str()?;
    if let Some(entry) = SETS.get_mut(key) {
        let mut set = entry.write().unwrap();
        let removed = set.remove(member);
        let empty = set.is_empty();
        drop(set);
        drop(entry);
        if empty {
            SETS.remove(key);
        }
        return Ok((removed as i64).into());
    }
    Ok(0i64.into())
}

fn gzscore(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 3 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let member = args[2].try_as_str()?;
    if let Some(set) = SETS.get(key) {
        if let Some(score) = set.value().read().unwrap().score(member) {
            return Ok(score.into());
        }
    }
    Ok(RedisValue::Null)
}

fn gzcard(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 2 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    if let Some(set) = SETS.get(key) {
        return Ok((set.value().read().unwrap().members.len() as i64).into());
    }
    Ok(0i64.into())
}

fn gzpop_generic(args: Vec<RedisString>, min: bool) -> Result {
    if args.len() > 3 || args.len() < 2 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let mut count = 1usize;
    if args.len() == 3 {
        let c: i64 = args[2].parse_integer()?;
        if c < 0 {
            return Err(RedisError::Str("ERR count must be positive"));
        }
        if c == 0 {
            return Ok(RedisValue::Array(Vec::new()));
        }
        count = c as usize;
    }
    let mut result = Vec::new();
    if let Some(entry) = SETS.get_mut(key) {
        let mut set = entry.write().unwrap();
        for _ in 0..count {
            let candidate = if min {
                set.by_score
                    .iter()
                    .next()
                    .map(|(s, m)| (*s, m.iter().next().unwrap().clone()))
            } else {
                set.by_score
                    .iter()
                    .next_back()
                    .map(|(s, m)| (*s, m.iter().next_back().unwrap().clone()))
            };
            let (score, member) = match candidate {
                Some(v) => v,
                None => break,
            };
            set.remove(&member);
            result.push(member.into());
            result.push(score.0.to_string().into());
        }
        let empty = set.is_empty();
        drop(set);
        drop(entry);
        if empty {
            SETS.remove(key);
        }
    }
    if result.is_empty() {
        if count == 1 {
            Ok(RedisValue::Null)
        } else {
            Ok(RedisValue::Array(Vec::new()))
        }
    } else {
        Ok(RedisValue::Array(result))
    }
}

fn gzpopmin(_ctx: &Context, args: Vec<RedisString>) -> Result {
    gzpop_generic(args, true)
}

fn gzpopmax(_ctx: &Context, args: Vec<RedisString>) -> Result {
    gzpop_generic(args, false)
}

fn gzrandmember(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() < 2 || args.len() > 4 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let mut idx = 2usize;
    let mut count: Option<i64> = None;
    let mut with_scores = false;
    if idx < args.len() {
        let a = args[idx].to_string_lossy();
        if a.eq_ignore_ascii_case("withscores") {
            return Err(RedisError::WrongArity);
        }
        count = Some(args[idx].parse_integer()?);
        idx += 1;
    }
    if idx < args.len() {
        if args[idx]
            .to_string_lossy()
            .eq_ignore_ascii_case("withscores")
        {
            with_scores = true;
            idx += 1;
        } else {
            return Err(RedisError::WrongArity);
        }
    }
    if idx != args.len() {
        return Err(RedisError::WrongArity);
    }

    let set = match SETS.get(key) {
        Some(s) => s,
        None => {
            return if count.is_some() {
                Ok(RedisValue::Array(Vec::new()))
            } else {
                Ok(RedisValue::Null)
            };
        }
    };
    let items = set.value().read().unwrap().all_items();
    if items.is_empty() {
        return if count.is_some() {
            Ok(RedisValue::Array(Vec::new()))
        } else {
            Ok(RedisValue::Null)
        };
    }
    use rand::{seq::SliceRandom, thread_rng};
    let mut rng = thread_rng();
    match count {
        None => {
            let &(score, ref member) = items.choose(&mut rng).unwrap();
            if with_scores {
                Ok(RedisValue::Array(vec![
                    member.clone().into(),
                    score.to_string().into(),
                ]))
            } else {
                Ok(member.clone().into())
            }
        }
        Some(c) => {
            if c == 0 {
                return Ok(RedisValue::Array(Vec::new()));
            }
            let mut out = Vec::new();
            if c < 0 {
                let cnt = (-c) as usize;
                for _ in 0..cnt {
                    let &(score, ref member) = items.choose(&mut rng).unwrap();
                    out.push(member.clone().into());
                    if with_scores {
                        out.push(score.to_string().into());
                    }
                }
            } else {
                let cnt = c as usize;
                let mut idxs: Vec<_> = items.iter().collect();
                idxs.shuffle(&mut rng);
                for &(score, ref member) in idxs.into_iter().take(cnt.min(items.len())) {
                    out.push(member.clone().into());
                    if with_scores {
                        out.push(score.to_string().into());
                    }
                }
            }
            Ok(RedisValue::Array(out))
        }
    }
}

fn gzmscore(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let members: Vec<_> = args[2..].iter().map(|m| m.try_as_str().unwrap()).collect();
    let mut out = Vec::new();
    if let Some(set) = SETS.get(key) {
        let set = set.value().read().unwrap();
        for m in &members {
            if let Some(score) = set.score(m) {
                out.push(score.to_string().into());
            } else {
                out.push(RedisValue::Null);
            }
        }
    } else {
        out.extend((0..members.len()).map(|_| RedisValue::Null));
    }
    Ok(RedisValue::Array(out))
}

fn gzunion(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let num: i64 = args[1].parse_integer()?;
    if num <= 0 {
        return Err(RedisError::Str("ERR numkeys must be > 0"));
    }
    let num = num as usize;
    if args.len() != num + 2 {
        return Err(RedisError::WrongArity);
    }
    let keys: Vec<_> = args[2..].iter().map(|k| k.try_as_str().unwrap()).collect();
    use std::collections::HashMap;
    let mut agg: HashMap<String, f64> = HashMap::new();
    for k in keys {
        if let Some(set) = SETS.get(k) {
            for (score, member) in set.value().read().unwrap().all_items() {
                *agg.entry(member).or_insert(0.0) += score;
            }
        }
    }
    let mut items: Vec<_> = agg.into_iter().collect();
    items.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then_with(|| a.0.cmp(&b.0)));
    let mut out = Vec::new();
    for (m, s) in items {
        out.push(m.into());
        out.push(s.to_string().into());
    }
    Ok(RedisValue::Array(out))
}

fn gzinter(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let num: i64 = args[1].parse_integer()?;
    if num <= 0 {
        return Err(RedisError::Str("ERR numkeys must be > 0"));
    }
    let num = num as usize;
    if args.len() != num + 2 {
        return Err(RedisError::WrongArity);
    }
    let keys: Vec<_> = args[2..].iter().map(|k| k.try_as_str().unwrap()).collect();
    let mut refs: Vec<Arc<RwLock<ScoreSet>>> = Vec::new();
    for &k in &keys {
        match SETS.get(k) {
            Some(s) => refs.push(Arc::clone(s.value())),
            None => return Ok(RedisValue::Array(Vec::new())),
        }
    }
    refs.sort_by_key(|s| s.read().unwrap().members.len());
    let first = refs[0].read().unwrap();
    use std::collections::HashMap;
    let mut agg: HashMap<String, f64> = HashMap::new();
    for (m, s) in &first.members {
        let mut sum = s.0;
        let mut present = true;
        for other in &refs[1..] {
            let other = other.read().unwrap();
            if let Some(sc) = other.members.get(m) {
                sum += sc.0;
            } else {
                present = false;
                break;
            }
        }
        if present {
            agg.insert(m.clone(), sum);
        }
    }
    let mut items: Vec<_> = agg.into_iter().collect();
    items.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then_with(|| a.0.cmp(&b.0)));
    let mut out = Vec::new();
    for (m, s) in items {
        out.push(m.into());
        out.push(s.to_string().into());
    }
    Ok(RedisValue::Array(out))
}

fn gzdiff(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let num: i64 = args[1].parse_integer()?;
    if num <= 0 {
        return Err(RedisError::Str("ERR numkeys must be > 0"));
    }
    let num = num as usize;
    if args.len() != num + 2 {
        return Err(RedisError::WrongArity);
    }
    let keys: Vec<_> = args[2..].iter().map(|k| k.try_as_str().unwrap()).collect();
    let first_arc = match SETS.get(keys[0]) {
        Some(s) => Arc::clone(s.value()),
        None => return Ok(RedisValue::Array(Vec::new())),
    };
    let first = first_arc.read().unwrap();
    use std::collections::HashMap;
    let mut diff: HashMap<String, f64> = HashMap::new();
    for (m, s) in &first.members {
        let mut found = false;
        for &k in &keys[1..] {
            if let Some(set) = SETS.get(k) {
                if set.value().read().unwrap().members.contains_key(m) {
                    found = true;
                    break;
                }
            }
        }
        if !found {
            diff.insert(m.clone(), s.0);
        }
    }
    let mut items: Vec<_> = diff.into_iter().collect();
    items.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then_with(|| a.0.cmp(&b.0)));
    let mut out = Vec::new();
    for (m, s) in items {
        out.push(m.into());
        out.push(s.to_string().into());
    }
    Ok(RedisValue::Array(out))
}

fn gzintercard(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() < 3 || args.len() > 4 {
        return Err(RedisError::WrongArity);
    }
    let key1 = args[1].try_as_str()?;
    let key2 = args[2].try_as_str()?;
    let limit = if args.len() == 4 {
        Some(args[3].parse_integer()?)
    } else {
        None
    };
    let set1 = match SETS.get(key1) {
        Some(s) => Arc::clone(s.value()),
        None => return Ok(0i64.into()),
    };
    let set2 = match SETS.get(key2) {
        Some(s) => Arc::clone(s.value()),
        None => return Ok(0i64.into()),
    };
    let (small, big) = if set1.read().unwrap().members.len() <= set2.read().unwrap().members.len() {
        (set1, set2)
    } else {
        (set2, set1)
    };
    let small = small.read().unwrap();
    let big = big.read().unwrap();
    let mut count = 0i64;
    for m in small.members.keys() {
        if big.members.contains_key(m) {
            count += 1;
            if let Some(l) = limit {
                if count >= l {
                    return Ok(count.into());
                }
            }
        }
    }
    Ok(count.into())
}

fn gzscan(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 3 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let cursor: u64 = args[2].parse_integer()? as u64;
    let set = match SETS.get(key) {
        Some(s) => s,
        None => {
            return Ok(RedisValue::Array(vec![
                "0".into(),
                RedisValue::Array(Vec::new()),
            ]));
        }
    };
    let items = set.value().read().unwrap().all_items();
    const BATCH: usize = 10;
    let start = cursor as usize;
    let chunk: Vec<_> = items.iter().skip(start).take(BATCH).cloned().collect();
    let next = if start + chunk.len() >= items.len() {
        0
    } else {
        (start + chunk.len()) as u64
    };
    let mut arr = Vec::new();
    for (score, member) in chunk {
        arr.push(member.into());
        arr.push(score.to_string().into());
    }
    Ok(RedisValue::Array(vec![
        next.to_string().into(),
        RedisValue::Array(arr),
    ]))
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
        redis_command!(ctx, "GZRANK", gzrank, "readonly", 1, 1, 1)?;
        redis_command!(ctx, "GZRANGE", gzrange, "readonly", 1, 1, 1)?;
        redis_command!(ctx, "GZREM", gzrem, "write fast", 1, 1, 1)?;
        redis_command!(ctx, "GZSCORE", gzscore, "readonly", 1, 1, 1)?;
        redis_command!(ctx, "GZCARD", gzcard, "readonly", 1, 1, 1)?;
        redis_command!(ctx, "GZPOPMIN", gzpopmin, "write fast", 1, 1, 1)?;
        redis_command!(ctx, "GZPOPMAX", gzpopmax, "write fast", 1, 1, 1)?;
        redis_command!(ctx, "GZRANDMEMBER", gzrandmember, "readonly", 1, 1, 1)?;
        redis_command!(ctx, "GZMSCORE", gzmscore, "readonly", 1, 1, 1)?;
        redis_command!(ctx, "GZUNION", gzunion, "readonly", 2, -1, 1)?;
        redis_command!(ctx, "GZINTER", gzinter, "readonly", 2, -1, 1)?;
        redis_command!(ctx, "GZDIFF", gzdiff, "readonly", 2, -1, 1)?;
        redis_command!(ctx, "GZINTERCARD", gzintercard, "readonly", 1, 2, 1)?;
        redis_command!(ctx, "GZSCAN", gzscan, "readonly", 1, 1, 1)?;
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
        assert!(set.insert(2.0, "b"));
        assert!(set.insert(1.0, "a"));
        assert!(!set.insert(1.0, "a"));
        let all = set.range_iter(0, -1);
        assert_eq!(all, vec![(1.0, "a".to_string()), (2.0, "b".to_string())]);
    }

    #[test]
    fn negative_indices() {
        let mut set = ScoreSet::default();
        for i in 0..5 {
            set.insert(i as f64, &format!("m{i}"));
        }
        let out = set.range_iter(-2, -1);
        assert_eq!(out[0].1, "m3");
        assert_eq!(out[1].1, "m4");
    }
}
