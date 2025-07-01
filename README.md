# gzset
GPU accelerated learned sorted set module for Valkey/Redis

This repository now contains the initial Rust crate scaffolding for
developing a Valkey/Redis module. The library builds as a `cdylib`
and exposes stubbed `gzset_on_load` and `gzset_on_unload` functions
that will be called by Valkey/Redis when the module is loaded or
unloaded.

## Testing

Run `cargo test` to execute the full test suite. The tests start a Valkey instance and
automatically build the `libgzset.so` shared library via the `build_module` helper.

## Prerequisites

`valkey-server` must be available in `PATH` for `cargo valkey` and the test suite.
On Ubuntu run `sudo apt-get install -y valkey-server valkey-tools`. On macOS use
`brew install valkey`.

## Quick start

The `cargo valkey` alias is defined in `.cargo/config.toml`.
Start a local server with the module pre-loaded:

```bash
cargo valkey -- --loglevel warning
```

## Contributing

See [AGENTS.md](AGENTS.md) for contributor guidelines used by automated agents.
