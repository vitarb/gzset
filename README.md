# gzset
GPU accelerated learned sorted set module for Valkey/Redis

This repository now contains the initial Rust crate scaffolding for
developing a Valkey/Redis module. The library builds as a `cdylib`
and exposes stubbed `gzset_on_load` and `gzset_on_unload` functions
that will be called by Valkey/Redis when the module is loaded or
unloaded.

## Testing

Run `cargo test` for unit tests.

For functional tests with a Valkey instance, run `cargo integ` (or `cargo test -- --ignored`).
The `cargo integ` alias automatically builds the `libgzset.so` shared library first via
the `build_module` helper test so that the module is available when the integration
tests start.
