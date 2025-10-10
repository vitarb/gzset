//! Bench-only helpers gated behind `bench-internals` (such as `RankFind`) are not
//! part of the stable public API surface.
#![deny(clippy::uninlined_format_args, clippy::to_string_in_format_args)]

#[cfg(all(not(test), feature = "redis-module"))]
#[global_allocator]
static GLOBAL: redis_module::alloc::RedisAlloc = redis_module::alloc::RedisAlloc;

pub use crate::{
    command::register_commands,
    format::{fmt_f64, with_fmt_buf},
    pool::{FastHashMap, MemberId, StringPool},
    score_set::{RangeIterFwd, ScoreIter, ScoreSet},
};

#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub use crate::score_set::RankFind;

mod buckets;
mod command;
mod format;
mod memory;
mod pool;
mod score_set;
