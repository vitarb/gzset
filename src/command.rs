use crate::format::{fmt_f64, with_fmt_buf};
use crate::keyspace as sets;
use crate::score_set::FastHashMap;
use redis_module::{self as rm, raw, Context, RedisError, RedisResult, RedisString, RedisValue};
use std::ffi::CString;
use std::os::raw::{c_char, c_int, c_long, c_void};

pub type Result<T = RedisValue> = RedisResult<T>;

const REDISMODULE_API_VERSION: c_int = raw::REDISMODULE_APIVER_1 as c_int;

static GZSET_TYPE: rm::native_types::RedisType = rm::native_types::RedisType::new(
    "gzsetmod1",
    0,
    raw::RedisModuleTypeMethods {
        version: raw::REDISMODULE_TYPE_METHOD_VERSION as u64,
        rdb_load: Some(gzset_rdb_load),
        rdb_save: Some(gzset_rdb_save),
        aof_rewrite: None,
        free: None,
        mem_usage: None,
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

    let added = sets::with_write(key, |s| s.insert(score, member));
    Ok((added as i64).into())
}

fn gzrank(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 3 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let member = args[2].try_as_str()?;
    if let Some(rank) = sets::with_read(key, |s| s.rank(member)) {
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
    sets::with_read(key, |s| {
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
    });
    Ok(RedisValue::NoReply)
}

fn gzrem(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 3 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let member = args[2].try_as_str()?;
    let removed = sets::with_write(key, |s| s.remove(member));
    Ok((removed as i64).into())
}

fn gzscore(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 3 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let member = args[2].try_as_str()?;
    if let Some(score) = sets::with_read(key, |s| s.score(member)) {
        return Ok(score.into());
    }
    Ok(RedisValue::Null)
}

fn gzcard(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() != 2 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let len = sets::with_read(key, |s| s.members.len() as i64);
    Ok(len.into())
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
    let result = sets::with_write(key, |set| {
        let mut out = Vec::new();
        for _ in 0..count {
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
            let (member, remove_score) = {
                let set_ref = entry.get_mut();
                let m = if min {
                    let m = set_ref.iter().next().unwrap().clone();
                    set_ref.take(&m).unwrap()
                } else {
                    let m = set_ref.iter().next_back().unwrap().clone();
                    set_ref.take(&m).unwrap()
                };
                let empty = set_ref.is_empty();
                (m, empty)
            };
            if remove_score {
                entry.remove_entry();
            }
            set.members.remove(&member);
            out.push(member.into());
            with_fmt_buf(|b| out.push(fmt_f64(b, score_key.0).to_owned().into()));
        }
        out
    });
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

    let items = sets::with_read(key, |s| s.all_items());
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
    sets::with_read(key, |set| {
        for m in &members {
            if let Some(score) = set.score(m) {
                with_fmt_buf(|b| out.push(fmt_f64(b, score).to_owned().into()));
            } else {
                out.push(RedisValue::Null);
            }
        }
    });
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
        sets::with_read(k, |set| {
            for (score, member) in set.all_items() {
                *agg.entry(member).or_insert(0.0) += score;
            }
        });
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
    keys_vec.sort_by_key(|k| sets::with_read(k, |s| s.members.len()));
    let first_members: Vec<(String, f64)> = sets::with_read(keys_vec[0], |s| {
        s.members.iter().map(|(m, sc)| (m.clone(), sc.0)).collect()
    });
    let mut agg: FastHashMap<String, f64> = FastHashMap::default();
    for (m, sc) in &first_members {
        let mut sum = *sc;
        let mut present = true;
        for k in &keys_vec[1..] {
            match sets::with_read(k, |set| set.members.get(m).copied()) {
                Some(other_sc) => sum += other_sc.0,
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
    let first_items: Vec<(String, f64)> = sets::with_read(keys[0], |s| {
        s.members.iter().map(|(m, sc)| (m.clone(), sc.0)).collect()
    });
    let mut diff: FastHashMap<String, f64> = FastHashMap::default();
    for (m, sc) in first_items {
        let mut found = false;
        for &k in &keys[1..] {
            if sets::with_read(k, |set| set.members.contains_key(&m)) {
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
    let len1 = sets::with_read(key1, |s| s.members.len());
    let len2 = sets::with_read(key2, |s| s.members.len());
    if len1 == 0 || len2 == 0 {
        return Ok(0i64.into());
    }
    let (small_key, big_key) = if len1 <= len2 {
        (key1, key2)
    } else {
        (key2, key1)
    };
    let small_members: Vec<String> =
        sets::with_read(small_key, |s| s.members.keys().cloned().collect());
    let mut count = 0i64;
    for m in small_members {
        let present = sets::with_read(big_key, |set| set.members.contains_key(&m));
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
    let items = sets::with_read(key, |s| s.all_items());
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

#[no_mangle]
pub unsafe extern "C" fn gzset__on_flush(
    _ctx: *mut raw::RedisModuleCtx,
    _event: raw::RedisModuleEvent,
    _sub: u64,
    _data: *mut c_void,
) {
    sets::clear_all();
}

unsafe extern "C" fn gzset_cmd_filter(fctx: *mut raw::RedisModuleCommandFilterCtx) {
    let arg0 = raw::RedisModule_CommandFilterArgGet.unwrap()(fctx, 0);
    if !arg0.is_null() {
        if let Ok(name) = rm::RedisString::from_ptr(arg0) {
            if name.eq_ignore_ascii_case("flushdb") || name.eq_ignore_ascii_case("flushall") {
                sets::clear_all();
            }
        }
    }
}

const REDISMODULE_EVENT_FLUSHDB_VERSION: u64 = 1;

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
    if raw::RedisModule_RegisterCommandFilter.unwrap()(ctx, Some(gzset_cmd_filter), 0).is_null() {
        return raw::Status::Err as c_int;
    }
    const FLUSH_EVENT: raw::RedisModuleEvent = raw::RedisModuleEvent {
        id: raw::REDISMODULE_EVENT_FLUSHDB,
        dataver: REDISMODULE_EVENT_FLUSHDB_VERSION,
    };
    if raw::RedisModule_SubscribeToServerEvent.unwrap()(ctx, FLUSH_EVENT, Some(gzset__on_flush))
        == raw::Status::Err as c_int
    {
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
