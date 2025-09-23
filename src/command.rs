use crate::format::{fmt_f64, with_fmt_buf};
use crate::{score_set::ScoreSet, FastHashMap};
use ordered_float::OrderedFloat;
use redis_module::raw::{
    RedisModule_ReplySetArrayLength, RedisModule_ReplyWithArray, RedisModule_ReplyWithDouble,
    RedisModule_ReplyWithNull, RedisModule_ReplyWithStringBuffer, REDISMODULE_POSTPONED_ARRAY_LEN,
};
use redis_module::{self as rm, raw, Context, RedisError, RedisResult, RedisString, RedisValue};
use std::convert::TryFrom;
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
    if !score.is_finite() {
        return Err(RedisError::Str("ERR score is not a finite number"));
    }
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
        let x: i64 = arg.parse_integer()?;
        isize::try_from(x).map_err(|_| RedisError::Str("ERR index is out of range"))
    };
    let start = parse_index(&args[2])?;
    let stop = parse_index(&args[3])?;
    with_set_read(ctx, key, |s| {
        let len = s.len();
        if len > 0 && start == 0 && (stop == -1 || (stop >= 0 && stop as usize == len - 1)) {
            unsafe {
                raw::RedisModule_ReplyWithArray.unwrap()(ctx.get_raw(), len as c_long);
                for (m, _) in s.iter_all() {
                    raw::RedisModule_ReplyWithStringBuffer.unwrap()(
                        ctx.get_raw(),
                        m.as_ptr().cast(),
                        m.len(),
                    );
                }
            }
        } else {
            let mut it = s.iter_range_fwd(start, stop);
            unsafe {
                raw::RedisModule_ReplyWithArray.unwrap()(ctx.get_raw(), it.size_hint().0 as c_long);
                for (m, _) in &mut it {
                    raw::RedisModule_ReplyWithStringBuffer.unwrap()(
                        ctx.get_raw(),
                        m.as_ptr().cast(),
                        m.len(),
                    );
                }
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
    let len = with_set_read(_ctx, key, |s| s.len() as i64)?;
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
    let emitted = with_set_write(ctx, key, |set| {
        if set.is_empty() {
            return None;
        }
        let raw = ctx.get_raw();
        unsafe {
            RedisModule_ReplyWithArray.unwrap()(raw, REDISMODULE_POSTPONED_ARRAY_LEN as c_long)
        };
        let mut pairs = 0usize;
        set.pop_n_visit(min, count, |name, score| {
            unsafe {
                RedisModule_ReplyWithStringBuffer.unwrap()(raw, name.as_ptr().cast(), name.len());
                RedisModule_ReplyWithDouble.unwrap()(raw, score);
            }
            pairs += 1;
        });
        unsafe { RedisModule_ReplySetArrayLength.unwrap()(raw, (pairs * 2) as c_long) };
        Some(pairs)
    })?;
    match emitted {
        Some(_) => Ok(RedisValue::NoReply),
        None => {
            if count == 1 {
                Ok(RedisValue::Null)
            } else {
                Ok(RedisValue::Array(Vec::new()))
            }
        }
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

    let result = with_set_read(ctx, key, |s| -> rm::RedisResult<RedisValue> {
        if s.is_empty() {
            return Ok(if count.is_some() {
                RedisValue::Array(Vec::new())
            } else {
                RedisValue::Null
            });
        }
        use rand::{seq::index::sample, thread_rng, Rng};
        use rustc_hash::FxHashSet;
        let len = s.len();
        let mut rng = thread_rng();
        match count {
            None => {
                let idx = rng.gen_range(0..len);
                let (m, sc) = s.select_by_rank(idx);
                if with_scores {
                    Ok(RedisValue::Array(vec![
                        m.to_owned().into(),
                        with_fmt_buf(|b| fmt_f64(b, sc).to_owned()).into(),
                    ]))
                } else {
                    Ok(m.to_owned().into())
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
                        let idx = rng.gen_range(0..len);
                        let (m, sc) = s.select_by_rank(idx);
                        out.push(m.to_owned().into());
                        if with_scores {
                            with_fmt_buf(|b| out.push(fmt_f64(b, sc).to_owned().into()));
                        }
                    }
                } else {
                    let cnt = c as usize;
                    if cnt >= len {
                        for (m, sc) in s.iter_all() {
                            out.push(m.to_owned().into());
                            if with_scores {
                                with_fmt_buf(|b| out.push(fmt_f64(b, sc).to_owned().into()));
                            }
                        }
                    } else if cnt <= 64 || cnt * 3 <= len {
                        let mut seen: FxHashSet<usize> = FxHashSet::default();
                        while out.len() < cnt {
                            let idx = rng.gen_range(0..len);
                            if seen.insert(idx) {
                                let (m, sc) = s.select_by_rank(idx);
                                out.push(m.to_owned().into());
                                if with_scores {
                                    with_fmt_buf(|b| out.push(fmt_f64(b, sc).to_owned().into()));
                                }
                            }
                        }
                    } else {
                        let mut idxs = sample(&mut rng, len, cnt).into_vec();
                        idxs.sort_unstable();
                        let iter = s.iter_all().enumerate();
                        let mut idx_iter = idxs.into_iter();
                        let mut next_idx = idx_iter.next();
                        for (i, (m, sc)) in iter {
                            if Some(i) == next_idx {
                                out.push(m.to_owned().into());
                                if with_scores {
                                    with_fmt_buf(|b| out.push(fmt_f64(b, sc).to_owned().into()));
                                }
                                next_idx = idx_iter.next();
                                if next_idx.is_none() {
                                    break;
                                }
                            }
                        }
                    }
                }
                Ok(RedisValue::Array(out))
            }
        }
    })??;
    Ok(result)
}

fn gzmscore(ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let members: Vec<_> = args[2..].iter().map(|m| m.try_as_str().unwrap()).collect();
    let raw = ctx.get_raw();
    unsafe { RedisModule_ReplyWithArray.unwrap()(raw, members.len() as c_long) };
    with_set_read(ctx, key, |set| {
        for member in &members {
            unsafe {
                if let Some(score) = set.score(member) {
                    RedisModule_ReplyWithDouble.unwrap()(raw, score);
                } else {
                    RedisModule_ReplyWithNull.unwrap()(raw);
                }
            }
        }
    })?;
    Ok(RedisValue::NoReply)
}

fn gzunion(ctx: &Context, args: Vec<RedisString>) -> Result {
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
        with_set_read(ctx, k, |set| {
            agg.reserve(set.len());
            for (member, score) in set.iter_all() {
                if let Some(v) = agg.get_mut(member) {
                    *v += score;
                } else {
                    agg.insert(member.to_owned(), score);
                }
            }
        })?;
    }
    let mut items: Vec<_> = agg.into_iter().collect();
    items.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then_with(|| a.0.cmp(&b.0)));
    let raw = ctx.get_raw();
    unsafe { RedisModule_ReplyWithArray.unwrap()(raw, (items.len() * 2) as c_long) };
    for (member, score) in items {
        unsafe {
            RedisModule_ReplyWithStringBuffer.unwrap()(raw, member.as_ptr().cast(), member.len());
            RedisModule_ReplyWithDouble.unwrap()(raw, score);
        }
    }
    Ok(RedisValue::NoReply)
}

fn gzinter(ctx: &Context, args: Vec<RedisString>) -> Result {
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
    keys_vec.sort_by_key(|k| with_set_read(ctx, k, |s| s.len()).unwrap());
    let mut agg: FastHashMap<String, f64> = FastHashMap::default();
    with_set_read(ctx, keys_vec[0], |s| -> rm::RedisResult<()> {
        agg.reserve(s.len());
        for (m, sc) in s.iter_all() {
            let mut sum = sc;
            let mut present = true;
            for k in &keys_vec[1..] {
                match with_set_read(ctx, k, |set| set.score(m))? {
                    Some(other_sc) => sum += other_sc,
                    None => {
                        present = false;
                        break;
                    }
                }
            }
            if present {
                agg.insert(m.to_owned(), sum);
            }
        }
        Ok(())
    })??;
    let mut items: Vec<_> = agg.into_iter().collect();
    items.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then_with(|| a.0.cmp(&b.0)));
    let raw = ctx.get_raw();
    unsafe { RedisModule_ReplyWithArray.unwrap()(raw, (items.len() * 2) as c_long) };
    for (member, score) in items {
        unsafe {
            RedisModule_ReplyWithStringBuffer.unwrap()(raw, member.as_ptr().cast(), member.len());
            RedisModule_ReplyWithDouble.unwrap()(raw, score);
        }
    }
    Ok(RedisValue::NoReply)
}

fn gzdiff(ctx: &Context, args: Vec<RedisString>) -> Result {
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
    let mut diff: FastHashMap<String, f64> = FastHashMap::default();
    with_set_read(ctx, keys[0], |s| -> rm::RedisResult<()> {
        diff.reserve(s.len());
        for (m, sc) in s.iter_all() {
            let mut found = false;
            for &k in &keys[1..] {
                if with_set_read(ctx, k, |set| set.contains(m))? {
                    found = true;
                    break;
                }
            }
            if !found {
                diff.insert(m.to_owned(), sc);
            }
        }
        Ok(())
    })??;
    let mut items: Vec<_> = diff.into_iter().collect();
    items.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap().then_with(|| a.0.cmp(&b.0)));
    let raw = ctx.get_raw();
    unsafe { RedisModule_ReplyWithArray.unwrap()(raw, (items.len() * 2) as c_long) };
    for (member, score) in items {
        unsafe {
            RedisModule_ReplyWithStringBuffer.unwrap()(raw, member.as_ptr().cast(), member.len());
            RedisModule_ReplyWithDouble.unwrap()(raw, score);
        }
    }
    Ok(RedisValue::NoReply)
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
    let len1 = with_set_read(_ctx, key1, |s| s.len())?;
    let len2 = with_set_read(_ctx, key2, |s| s.len())?;
    if len1 == 0 || len2 == 0 {
        return Ok(0i64.into());
    }
    let (small_key, big_key) = if len1 <= len2 {
        (key1, key2)
    } else {
        (key2, key1)
    };
    let count = with_set_read(_ctx, small_key, |s| -> rm::RedisResult<i64> {
        let mut count = 0i64;
        for (m, _) in s.iter_all() {
            let present = with_set_read(_ctx, big_key, |set| set.contains(m))?;
            if present {
                count += 1;
                if let Some(l) = limit {
                    if count >= l {
                        break;
                    }
                }
            }
        }
        Ok(count)
    })??;
    Ok(count.into())
}

fn gzscan(_ctx: &Context, args: Vec<RedisString>) -> Result {
    if args.len() < 3 {
        return Err(RedisError::WrongArity);
    }
    let key = args[1].try_as_str()?;
    let cursor = args[2].try_as_str()?;

    const DEFAULT_COUNT: usize = 10;
    const MAX_COUNT: usize = 1024;

    let mut count = DEFAULT_COUNT;
    let mut idx = 3;
    let mut seen_count = false;
    while idx < args.len() {
        let opt = args[idx].try_as_str()?;
        if opt.eq_ignore_ascii_case("COUNT") {
            if seen_count {
                return Err(RedisError::Str("ERR syntax error"));
            }
            idx += 1;
            if idx >= args.len() {
                return Err(RedisError::Str("ERR syntax error"));
            }
            let raw = args[idx].parse_integer()?;
            if raw <= 0 || raw as usize > MAX_COUNT {
                return Err(RedisError::Str("ERR COUNT must be between 1 and 1024"));
            }
            count = raw as usize;
            seen_count = true;
            idx += 1;
        } else {
            return Err(RedisError::Str("ERR syntax error"));
        }
    }

    fn encode_cursor(score: f64, member: &str) -> String {
        with_fmt_buf(|b| {
            let score_s = fmt_f64(b, score);
            let mut out = String::with_capacity(score_s.len() + 1 + member.len() * 3);
            out.push_str(score_s);
            out.push('|');
            for ch in member.chars() {
                match ch {
                    '|' => out.push_str("%7C"),
                    '%' => out.push_str("%25"),
                    _ => out.push(ch),
                }
            }
            out
        })
    }

    fn decode_cursor(cur: &str) -> Option<(f64, String)> {
        let (score_s, member_s) = cur.split_once('|')?;
        let score = score_s.parse::<f64>().ok()?;
        if !score.is_finite() {
            return None;
        }
        if !with_fmt_buf(|b| fmt_f64(b, score) == score_s) {
            return None;
        }

        fn decode_hex(b: u8) -> Option<u8> {
            match b {
                b'0'..=b'9' => Some(b - b'0'),
                b'a'..=b'f' => Some(b - b'a' + 10),
                b'A'..=b'F' => Some(b - b'A' + 10),
                _ => None,
            }
        }

        let bytes = member_s.as_bytes();
        let mut member_bytes = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' {
                if i + 2 >= bytes.len() {
                    return None;
                }
                let hi = decode_hex(bytes[i + 1])?;
                let lo = decode_hex(bytes[i + 2])?;
                member_bytes.push((hi << 4) | lo);
                i += 3;
            } else {
                member_bytes.push(bytes[i]);
                i += 1;
            }
        }
        let member = String::from_utf8(member_bytes).ok()?;
        Some((score, member))
    }

    let parsed = if cursor == "0" {
        None
    } else {
        Some(decode_cursor(cursor).ok_or(RedisError::Str("ERR invalid cursor"))?)
    };

    let (arr, next) = with_set_read(
        _ctx,
        key,
        move |s| -> rm::RedisResult<(Vec<RedisValue>, String)> {
            if s.is_empty() {
                return Ok((Vec::new(), "0".to_string()));
            }

            let mut iter = match parsed {
                None => s
                    .iter_from(OrderedFloat(f64::NEG_INFINITY), "", true)
                    .peekable(),
                Some((score, ref member)) => {
                    s.iter_from(OrderedFloat(score), member, true).peekable()
                }
            };

            let mut arr = Vec::new();
            let mut last = None;
            for _ in 0..count {
                if let Some((m, sc)) = iter.next() {
                    arr.push(m.to_owned().into());
                    with_fmt_buf(|b| arr.push(fmt_f64(b, sc).to_owned().into()));
                    last = Some((sc, m.to_owned()));
                } else {
                    break;
                }
            }
            let next = match last {
                Some((sc, m)) if iter.peek().is_some() => encode_cursor(sc, &m),
                _ => "0".to_string(),
            };
            Ok((arr, next))
        },
    )??;

    Ok(RedisValue::Array(vec![next.into(), RedisValue::Array(arr)]))
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
