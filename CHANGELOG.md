# Changelog

## Unreleased
- Removed DashMap concurrency layer. The module now keeps state in a
  thread-local `RefCell<HashMap>` and assumes single-threaded execution.
- Read-only commands are registered with `readonly` flag instead of `readonly fast`.
