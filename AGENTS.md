# Contributor checklist for automated agents

Rules for automated agents / contributors

## Prerequisites
- Install Rust toolchain with `rustup`.
- Ensure `rustfmt` and `clippy` components are installed.
- Have Valkey binaries (`valkey-server` and `valkey-cli`) in `PATH` for integration tests.

## Standard build
Agents must verify that a plain build succeeds:

```bash
cargo build --all-targets
```

Running with `--release` is optional but must also succeed if used.

## Unit tests
Run the unit test suite:

```bash
cargo test
```

## Integration tests
Integration tests are executed via the cargo alias:

```bash
cargo integ
```

This builds `libgzset.so` automatically. Agents must ensure Valkey binaries are in `PATH` so the tests can start a server.

## Formatting
Code must be formatted with:

```bash
cargo fmt -- --check
```

## Lint / Clippy
Code must pass Clippy without warnings:

```bash
cargo clippy --all-targets -- -D warnings
```

## No unchecked files
- Ensure `git status --porcelain` shows no changes.
- Generated files such as `Cargo.lock` must be committed when modified.

## Commit message hints
- Use imperative mood in the subject line.
- Limit the subject to 72 characters.
- Reference issues or PRs when relevant.
