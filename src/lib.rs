use std::os::raw::{c_char, c_int, c_void};

use redis_module::{self as rm, raw, Context, RedisError, RedisResult, RedisString, RedisValue};
use std::ffi::CString;

const REDISMODULE_API_VERSION: c_int = raw::REDISMODULE_APIVER_1 as c_int;

/// Convenient result type used throughout the crate.
pub type Result<T = RedisValue> = RedisResult<T>;

/// Simple helper used by unimplemented commands.
fn not_implemented<T>() -> Result<T> {
    Err(RedisError::String("not implemented".to_string()))
}

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

fn gzadd(_ctx: &Context, _args: Vec<RedisString>) -> Result {
    not_implemented()
}

fn gzrank(_ctx: &Context, _args: Vec<RedisString>) -> Result {
    not_implemented()
}

fn gzrange(_ctx: &Context, _args: Vec<RedisString>) -> Result {
    not_implemented()
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

    if GZSET_TYPE.create_data_type(ctx).is_err() {
        return raw::Status::Err as c_int;
    }

    rm::redis_command!(ctx, "GZADD", gzadd, "write", 1, 1, 1);
    rm::redis_command!(ctx, "GZRANK", gzrank, "readonly", 1, 1, 1);
    rm::redis_command!(ctx, "GZRANGE", gzrange, "readonly", 1, 1, 1);

    raw::Status::Ok as c_int
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
