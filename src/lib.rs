#![deny(clippy::uninlined_format_args, clippy::to_string_in_format_args)]

#[cfg(not(test))]
#[global_allocator]
static GLOBAL: redis_module::alloc::RedisAlloc = redis_module::alloc::RedisAlloc;

pub use crate::{
    command::register_commands,
    score_set::{FastHashMap, ScoreIter, ScoreSet},
};

mod command;
mod format;
mod memory;
mod score_set;
