#![deny(clippy::uninlined_format_args, clippy::to_string_in_format_args)]

pub use crate::{
    command::register_commands,
    score_set::{ScoreSet, ScoreIter, FastHashMap},
};

mod command;
mod score_set;
mod format;
pub mod keyspace;
pub use keyspace as sets;
