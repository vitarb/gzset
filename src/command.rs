use crate::format::{fmt_f64, with_fmt_buf};
use crate::{score_set::ScoreSet, FastHashMap};
use redis_module::{self as rm, raw, Context, RedisError, RedisResult, RedisString, RedisValue};
use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_long, c_void};

pub type Result<T = RedisValue> = RedisResult<T>;

const REDISMODULE_API_VERSION: c_int = raw::REDISMODULE_APIVER_1 as c_int;

pub static GZSET_TYPE: rm::native_types::RedisType = rm::native_types::RedisType::new(
    "gzsetmod1",
    0,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: Some(gzset_rdb_load),
        rdb_save: Some(gzset_rdb_save),
        aof_rewrite: None,
        free: Some(crate::memory::gzset_free),
        mem_usage: Some(crate::memory::gzset_mem_usage),
        digest: None,
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

fn with_set_write<F, R>(ctx: &Context, key: &str, f: F) -> rm::RedisResult<R>
where
    F: FnOnce(&mut ScoreSet) -> R,
{
    let keyname = ctx.create_string(key);
    let rkey = ctx.open_key_writable(&keyname);
    if rkey.get_value::<ScoreSet>(&GZSET_TYPE)?.is_none() {
        rkey.set_value(&GZSET_TYPE, ScoreSet::default())?;
    }
    let (res, empty) = {
        let set = rkey.get_value::<ScoreSet>(&GZSET_TYPE)?.unwrap();
        let r = f(set);
        (r, set.is_empty())
    };
    if empty {
        drop(rkey);
        let rkey = ctx.open_key_writable(&keyname);
        rkey.delete()?;
    }
    Ok(res)
}

fn with_set_read<F, R>(ctx: &Context, key: &str, f: F) -> rm::RedisResult<R>
where
    F: FnOnce(&ScoreSet) -> R,
{
    let keyname = ctx.create_string(key);
    let rkey = ctx.open_key(&keyname);
    if let Some(set) = rkey.get_value::<ScoreSet>(&GZSET_TYPE)? {
        Ok(f(set))
    } else {
        let tmp = ScoreSet::default();
        Ok(f(&tmp))
    }
}

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
            context.reply(response) as c_int
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

fn gzadd(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 4 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let score: f64 = args[2].parse_float()?;
    let member = args[3].try_as_str()?;

    let added = with_set_write(_ctx, key, |s| s.insert(score, member))?;
    Ok((added as i64).into())
}

fn gzrank(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 3 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let member = args[2].try_as_str()?;
    if let Some(rank) = with_set_read(_ctx, key, |s| s.rank(member))? {
        return Ok((rank as i64).into());
    }
    Ok(RedisValue::Null)
}

fn gzrange(ctx: &Context, args: Vec<RedisString>) -> Result {
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
    with_set_read(ctx, key, |s| {
        let mut it = s.iter_range(start, stop);
        unsafe {
            raw::RedisModule_ReplyWithArray.unwrap()(ctx.get_raw(), it.len() as c_long);
            for (m, _) in &mut it {
                raw::RedisModule_ReplyWithStringBuffer.unwrap()(
                    ctx.get_raw(),
                    m.as_ptr().cast(),
                    m.len(),
                );
            }
        }
    })?;
    Ok(RedisValue::NoReply)
}

fn gzrem(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 3 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let member = args[2].try_as_str()?;
    let removed = with_set_write(_ctx, key, |s| s.remove(member))?;
    Ok((removed as i64).into())
}

fn gzscore(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 3 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let member = args[2].try_as_str()?;
    if let Some(score) = with_set_read(_ctx, key, |s| s.score(member))? {
        return Ok(score.into());
    }
    Ok(RedisValue::Null)
}

fn gzcard(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 2 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let len = with_set_read(_ctx, key, |s| s.members.len() as i64)?;
    Ok(len.into())
}

fn gzpop_generic(ctx: &Context, args: Vec<RedisString>, min: bool) -> Result {
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
    let result = with_set_write(ctx, key, |set| {
        let mut out = Vec::new();
        for _ in 0..count {
            let (score_key, id) = {
                let mut entry = if min {
                    match set.by_score.first_entry() {
                        Some(e) => e,
                        None => break,
                    }
                } else {
                    match set.by_score.last_entry() {
                        Some(e) => e,
                        None => break,
                    }
                };
                let score_key = *entry.key();
                let (id, remove_score) = {
                    let bucket = entry.get_mut();
                    let id = if min {
                        bucket.remove(0)
                    } else {
                        bucket.pop().unwrap()
                    };
                    let empty = bucket.is_empty();
                    if !empty && bucket.spilled() && bucket.len() <= 4 {
                        bucket.shrink_to_fit();
                    }
                    (id, empty)
                };
                if remove_score {
                    entry.remove_entry();
                }
                (score_key, id)
            };
            set.members.remove(id);
            let member = set.pool.get(id);
            out.push(member.to_owned().into());
            with_fmt_buf(|b| out.push(fmt_f64(b, score_key.0).to_owned().into()));
        }
        out
    })?;
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

fn gzpopmin(ctx: &Context, args: Vec<RedisString>) -> Result {
    gzpop_generic(ctx, args, true)
}

fn gzpopmax(ctx: &Context, args: Vec<RedisString>) -> Result {
    gzpop_generic(ctx, args, false)
}

fn gzrandmember(ctx: &Context, args: Vec<RedisString>) -> Result {
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

    let items = with_set_read(ctx, key, |s| s.all_items())?;
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
                    with_fmt_buf(|b| fmt_f64(b, score).to_owned()).into(),
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
                        with_fmt_buf(|b| out.push(fmt_f64(b, score).to_owned().into()));
                    }
                }
            } else {
                let cnt = c as usize;
                let mut idxs: Vec<_> = items.iter().collect();
                idxs.shuffle(&mut rng);
                for &(score, ref member) in idxs.into_iter().take(cnt.min(items.len())) {
                    out.push(member.clone().into());
                    if with_scores {
                        with_fmt_buf(|b| out.push(fmt_f64(b, score).to_owned().into()));
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
    with_set_read(_ctx, key, |set| {
        for m in &members {
            if let Some(score) = set.score(m) {
                with_fmt_buf(|b| out.push(fmt_f64(b, score).to_owned().into()));
            } else {
                out.push(RedisValue::Null);
            }
        }
    })?;
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
    let mut agg: FastHashMap<String, f64> = FastHashMap::default();
    for k in keys {
        with_set_read(_ctx, k, |set| {
            for (score, member) in set.all_items() {
                *agg.entry(member).or_insert(0.0) += score;
            }
        })?;
    }
    let mut items: Vec<_> = agg.into_iter().collect();
    items.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then_with(|| a.0.cmp(&b.0)));
    let mut out = Vec::new();
    for (m, s) in items {
        out.push(m.into());
        with_fmt_buf(|b| out.push(fmt_f64(b, s).to_owned().into()));
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
    let mut keys_vec: Vec<_> = keys.clone();
    keys_vec.sort_by_key(|k| with_set_read(_ctx, k, |s| s.members.len()).unwrap());
    let first_members: Vec<(String, f64)> =
        with_set_read(_ctx, keys_vec[0], |s| s.members_with_scores())?;
    let mut agg: FastHashMap<String, f64> = FastHashMap::default();
    for (m, sc) in &first_members {
        let mut sum = *sc;
        let mut present = true;
        for k in &keys_vec[1..] {
            match with_set_read(_ctx, k, |set| set.score(m))? {
                Some(other_sc) => sum += other_sc,
                None => {
                    present = false;
                    break;
                }
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
        with_fmt_buf(|b| out.push(fmt_f64(b, s).to_owned().into()));
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
    let first_items: Vec<(String, f64)> =
        with_set_read(_ctx, keys[0], |s| s.members_with_scores())?;
    let mut diff: FastHashMap<String, f64> = FastHashMap::default();
    for (m, sc) in first_items {
        let mut found = false;
        for &k in &keys[1..] {
            if with_set_read(_ctx, k, |set| set.contains(&m))? {
                found = true;
                break;
            }
        }
        if !found {
            diff.insert(m, sc);
        }
    }
    let mut items: Vec<_> = diff.into_iter().collect();
    items.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then_with(|| a.0.cmp(&b.0)));
    let mut out = Vec::new();
    for (m, s) in items {
        out.push(m.into());
        with_fmt_buf(|b| out.push(fmt_f64(b, s).to_owned().into()));
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
    let len1 = with_set_read(_ctx, key1, |s| s.members.len())?;
    let len2 = with_set_read(_ctx, key2, |s| s.members.len())?;
    if len1 == 0 || len2 == 0 {
        return Ok(0i64.into());
    }
    let (small_key, big_key) = if len1 <= len2 {
        (key1, key2)
    } else {
        (key2, key1)
    };
    let small_members: Vec<String> = with_set_read(_ctx, small_key, |s| s.member_names())?;
    let mut count = 0i64;
    for m in small_members {
        let present = with_set_read(_ctx, big_key, |set| set.contains(&m))?;
        if present {
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
    let items = with_set_read(_ctx, key, |s| s.all_items())?;
    if items.is_empty() {
        return Ok(RedisValue::Array(vec![
            "0".into(),
            RedisValue::Array(Vec::new()),
        ]));
    }
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
        with_fmt_buf(|b| arr.push(fmt_f64(b, score).to_owned().into()));
    }
    Ok(RedisValue::Array(vec![
        next.to_string().into(),
        RedisValue::Array(arr),
    ]))
}

/// Register all module commands with the server.
///
/// # Safety
///
/// The `ctx` pointer must be a valid module context provided by Valkey/Redis.
pub unsafe fn register_commands(ctx: *mut raw::RedisModuleCtx) -> rm::Status {
    let result: rm::RedisResult<()> = (|| {
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
        rm::Status::Err
    } else {
        rm::Status::Ok
    }
}

pub unsafe extern "C" fn gzset_on_load(
    ctx: *mut raw::RedisModuleCtx,
    _argv: *mut *mut raw::RedisModuleString,
    _argc: c_int,
) -> c_int {
    let module_name = b"gzset\0";
    if raw::Export_RedisModule_Init(
        ctx,
        module_name.as_ptr().cast::<c_char>(),
        1,
        REDISMODULE_API_VERSION,
    ) == raw::Status::Err as c_int
    {
        return raw::Status::Err as c_int;
    }
    if GZSET_TYPE.create_data_type(ctx).is_err() {
        return raw::Status::Err as c_int;
    }
    if register_commands(ctx) == rm::Status::Err {
        return raw::Status::Err as c_int;
    }
    raw::Status::Ok as c_int
}

#[no_mangle]
pub unsafe extern "C" fn RedisModule_OnLoad(
    ctx: *mut raw::RedisModuleCtx,
    argv: *mut *mut raw::RedisModuleString,
    argc: c_int,
) -> c_int {
    gzset_on_load(ctx, argv, argc)
}

#[no_mangle]
pub unsafe extern "C" fn ValkeyModule_OnLoad(
    ctx: *mut raw::RedisModuleCtx,
    argv: *mut *mut raw::RedisModuleString,
    argc: c_int,
) -> c_int {
    gzset_on_load(ctx, argv, argc)
}

#[no_mangle]
pub unsafe extern "C" fn gzset_on_unload(_ctx: *mut c_void) {}
