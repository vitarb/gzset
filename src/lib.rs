use std::os::raw::{c_int, c_void, c_char};

/// Module initialization function called by Valkey/Redis on module load.
///
/// This is a minimal stub that currently just returns `0` to signal
/// successful initialization. All module setup logic will live here in
/// the future.
#[no_mangle]
pub extern "C" fn gzset_on_load(
    _ctx: *mut c_void,
    _argv: *mut *mut c_char,
    _argc: c_int,
) -> c_int {
    0
}

/// Optional unload function called when the module is unloaded.
#[no_mangle]
pub extern "C" fn gzset_on_unload(_ctx: *mut c_void) {
    // Clean-up logic would go here.
}

#[cfg(test)]
mod tests {
    #[test]
    fn load_returns_success() {
        let rc = unsafe { super::gzset_on_load(std::ptr::null_mut(), std::ptr::null_mut(), 0) };
        assert_eq!(rc, 0);
    }
}
