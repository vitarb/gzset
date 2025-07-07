# Changelog

## Unreleased
- Replaced global mutex with sharded `DashMap` and per-key `RwLock`.
- Read-only commands are registered with `readonly` flag instead of `readonly fast`.
