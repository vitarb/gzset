#![deny(clippy::uninlined_format_args)]
#![deny(clippy::to_string_in_format_args)]
#![allow(clippy::unnecessary_mut_passed)]
use std::os::raw::{c_char, c_int, c_long, c_void};

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
use rustc_hash::FxHashMap;
use ryu::Buffer;
use smallvec::SmallVec;
use std::collections::BTreeMap;

pub type FastHashMap<K, V> = FxHashMap<K, V>;

#[inline]
pub fn fmt_f64(buf: &mut Buffer, score: f64) -> &str {
    let formatted = buf.format_finite(score);
    formatted.strip_suffix(".0").unwrap_or(formatted)
}

thread_local! {
    static FMT_BUF: std::cell::RefCell<Buffer> = std::cell::RefCell::new(Buffer::new());
}

#[inline]
pub fn with_fmt_buf<F, R>(f: F) -> R
where
    F: FnOnce(&mut Buffer) -> R,
{
    FMT_BUF.with(|b| f(&mut b.borrow_mut()))
}

mod sets;

#[derive(Default)]
pub struct ScoreSet {
    by_score: BTreeMap<OrderedFloat<f64>, SmallVec<[String; 4]>>,
    members: FastHashMap<String, OrderedFloat<f64>>,
}

#[allow(clippy::type_complexity)]
struct ScoreIter<'a> {
    outer: std::collections::btree_map::Iter<'a, OrderedFloat<f64>, SmallVec<[String; 4]>>,
    current: Option<(&'a SmallVec<[String; 4]>, OrderedFloat<f64>, usize)>,
    index: usize,
    start: usize,
    stop: usize,
}

impl<'a> ScoreIter<'a> {
    fn new(
        map: &'a BTreeMap<OrderedFloat<f64>, SmallVec<[String; 4]>>,
        start: usize,
        stop: usize,
    ) -> Self {
        Self {
            outer: map.iter(),
            current: None,
            index: 0,
            start,
            stop,
        }
    }

    fn empty(map: &'a BTreeMap<OrderedFloat<f64>, SmallVec<[String; 4]>>) -> Self {
        Self {
            outer: map.iter(),
            current: None,
            index: 0,
            start: 1,
            stop: 0,
        }
    }

    #[inline]
    fn total_len(&self) -> usize {
        if self.start > self.stop {
            0
        } else {
            self.stop - self.start + 1
        }
    }
}

impl<'a> Iterator for ScoreIter<'a> {
    type Item = (&'a str, f64);

    fn next(&mut self) -> Option<Self::Item> {
        while self.index <= self.stop {
            if let Some((vec, score, ref mut pos)) = &mut self.current {
                if *pos < vec.len() {
                    let global = self.index;
                    let out_member = &vec[*pos];
                    *pos += 1;
                    self.index += 1;
                    if global < self.start {
                        continue;
                    }
                    return Some((out_member.as_str(), score.0));
                }
                self.current = None;
                continue;
            }
            match self.outer.next() {
                Some((score, vec)) => {
                    self.current = Some((vec, *score, 0));
                }
                None => break,
            }
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len();
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for ScoreIter<'_> {
    #[inline]
    fn len(&self) -> usize {
        let total = self.total_len();
        let done = self.index.saturating_sub(self.start);
        total.saturating_sub(done)
    }
}

impl ScoreSet {
    pub fn insert(&mut self, score: f64, member: &str) -> bool {
        let key = OrderedFloat(score);
        match self.members.insert(member.to_owned(), key) {
            Some(old) if old == key => return false,
            Some(old) => {
                if let Some(vec) = self.by_score.get_mut(&old) {
                    if let Ok(pos) = vec.binary_search_by(|m| m.as_str().cmp(member)) {
                        vec.remove(pos);
                    }
                    if vec.is_empty() {
                        self.by_score.remove(&old);
                    }
                }
            }
            None => {}
        }
        let vec = self.by_score.entry(key).or_default();
        match vec.binary_search_by(|m| m.as_str().cmp(member)) {
            Ok(_) => {}
            Err(pos) => vec.insert(pos, member.to_owned()),
        }
        true
    }

    pub fn remove(&mut self, member: &str) -> bool {
        if let Some(score) = self.members.remove(member) {
            if let Some(vec) = self.by_score.get_mut(&score) {
                if let Ok(pos) = vec.binary_search_by(|m| m.as_str().cmp(member)) {
                    vec.remove(pos);
                }
                if vec.is_empty() {
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
                for m in set.iter() {
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
        self.iter_range(start, stop)
            .map(|(m, s)| (s, m.to_owned()))
            .collect()
    }

    pub(crate) fn iter_range(&self, start: isize, stop: isize) -> ScoreIter<'_> {
        let len = self.members.len() as isize;
        if len == 0 {
            return ScoreIter::empty(&self.by_score);
        }
        let mut start = if start < 0 { len + start } else { start };
        let mut stop = if stop < 0 { len + stop } else { stop };
        if start < 0 {
            start = 0;
        }
        if stop < 0 {
            return ScoreIter::empty(&self.by_score);
        }
        if stop >= len {
            stop = len - 1;
        }
        if start > stop {
            return ScoreIter::empty(&self.by_score);
        }
        ScoreIter::new(&self.by_score, start as usize, stop as usize)
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn all_items(&self) -> Vec<(f64, String)> {
        let mut out = Vec::new();
        for (score, set) in &self.by_score {
            for m in set.iter() {
                out.push((score.0, m.clone()));
            }
        }
        out
    }

    #[cfg(any(test, feature = "bench"))]
    pub fn pop_all(&mut self, min: bool) -> Vec<String> {
        let mut out = Vec::new();
        while !self.by_score.is_empty() {
            let mut entry = if min {
                self.by_score.first_entry().unwrap()
            } else {
                self.by_score.last_entry().unwrap()
            };
            let s = entry.get_mut();
            let m = if min {
                let m = s.swap_remove(0);
                if !s.is_empty() {
                    s.sort_unstable();
                }
                m
            } else {
                s.pop().unwrap()
            };
            let empty = s.is_empty();
            let _ = s;
            if empty {
                entry.remove_entry();
            }
            self.members.remove(&m);
            out.push(m);
        }
        out
    }
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
        let it = s.iter_range(start, stop);
        unsafe {
            raw::RedisModule_ReplyWithArray.unwrap()(ctx.get_raw(), it.len() as c_long);
            for (m, _) in it {
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
                    let m = set_ref.swap_remove(0);
                    if !set_ref.is_empty() {
                        set_ref.sort_unstable();
                    }
                    m
                } else {
                    set_ref.pop().unwrap()
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

    #[test]
    fn pop_order_with_duplicates() {
        let mut set = ScoreSet::default();
        for m in ["b", "a", "c"] {
            set.insert(1.0, m);
        }
        let mins = set.pop_all(true);
        assert_eq!(mins, ["a", "b", "c"]);

        for m in ["b", "a", "c"] {
            set.insert(1.0, m);
        }
        let maxs = set.pop_all(false);
        assert_eq!(maxs, ["c", "b", "a"]);
    }
}
