# gzset – B‑tree Sorted‑Set Module for Valkey/Redis  
![CI](https://github.com/<your‑org>/gzset/actions/workflows/ci.yml/badge.svg)
![Bench](https://github.com/<your-org>/gzset/actions/workflows/bench.yml/badge.svg)

**gzset** is an experimental Valkey/Redis module that re‑implements
`ZSET` semantics on top of an **in‑memory B‑tree** rather than the
compressed‑skiplist used by upstream Redis.  
The original goal—exploring a GPU‑accelerated “learned” index—is still on
the roadmap, but we have **parked the GPU work** while we finish a solid,
CPU‑only reference implementation.

When built with the `redis-module` feature, the module uses Valkey’s allocator so memory stats and maxmemory policies work as expected.
Each `GZSET` key owns its B-tree data directly, so `MEMORY USAGE` reflects the exact footprint (aside from allocator fragmentation).

---

## Why a B‑tree?

* **Predictable memory layout** – dense pages give better cache‑line
  locality than a skiplist, especially for large sets.
* **Ordered iterations are fast** – in‑order traversal is a single walk
  instead of many pointer hops.
* **Natural persistence path** – if/when we add on‑disk backing, B‑tree
  pages translate directly to disk/ext‑memory blocks.

---

## Project status

| Area                | State | Notes |
|---------------------|-------|-------|
| Core commands       | ✅   | `GZADD / GZREM / GZRANGE / GZRANK / GZPOPMIN / GZPOPMAX …` |
| Valkey‑side unit tests | ✅   | Runs in CI on every push |
| RDB/AOF persistence | ❌   | Explicitly non‑persistent; keys are volatile |
| GPU‑learned index   | ⏸   | Prototype branch retained, not in `main` |
| Cluster support     | ❌   | Single‑node only for now |
| MEMORY USAGE key | reports exact usage | ✅ |

---

## Quick start

```bash
# 1. Build the module (debug)
cargo build -p gzset

# 2. Launch a throw‑away Valkey instance with the module pre‑loaded
# (builds the module with the `redis-module` feature so memory stats are accurate)
cargo valkey -- --loglevel warning
#                       └─────────────── extra args passed straight to valkey-server

# 3. In another shell, play with it
valkey-cli > GZADD myset 42.0 alice
(integer) 1
valkey-cli > GZRANGE myset 0 -1
1) "alice"
````

`cargo valkey` defaults to port **6379** when free; otherwise it prints the
chosen port. Use `--port <n>` to pin or `--force-kill` to evict an old
server already listening on 6379.

---

### Profiling with perf (Linux)

```bash
# 1) Generate a flamegraph while you drive load (press Ctrl-C when done, or pass --duration to stop automatically)
cargo valkey-flame

# 2) In a separate shell, drive a workload (example):
valkey-cli -p 6379 <<'EOS'
GZADD s 0 a
# … add data, then pop …
EOS

# Output: target/flame/<timestamp>/flame.svg
# Open it with your browser or: xdg-open target/flame/<timestamp>/flame.svg


Requires: perf and inferno-flamegraph (cargo install inferno).
```

macOS is supported too via Apple's `sample` profiler; see below.

### Profiling with sample (macOS)

```bash
# 1) Generate a flamegraph while you drive load (press Ctrl-C when done, or pass --duration to stop automatically)
cargo valkey-flame

# 2) In a separate shell, drive a workload (example):
valkey-cli -p 6379 <<'EOS'
GZADD s 0 a
# … add data, then pop …
EOS

# Output: target/flame/<timestamp>/flame.svg
# Open it with:
open target/flame/<timestamp>/flame.svg


Requires: Xcode Command Line Tools (xcode-select --install) and inferno (cargo install inferno).
The task enables frame pointers and includes debug symbols for clear call stacks.
```

---

## Building & testing

| Command                      | Purpose                                           |
| ---------------------------- | ------------------------------------------------- |
| `cargo build --all-targets`  | Compile library + tests                           |
| `cargo test`                 | Run Rust unit/integration tests (spins up Valkey) |
| `cargo clippy --all-targets` | Lint (warnings are *errors* in CI)                |
| `cargo fmt -- --check`       | Format check                                      |

First-time runs may take a while as Cargo compiles the `xtask` helper.
Run `cargo build --all-targets` before `cargo test` to prime the cache and prevent launch timeouts.

The GitHub Actions workflow replicates the above on *ubuntu‑latest*.

### Prerequisites

* **Rust ≥ 1.74** (install via [https://rustup.rs](https://rustup.rs))
* **Valkey ≥ 7.2** binaries (`valkey-server`, `valkey-cli`) in `PATH`
  *Ubuntu*: `sudo apt-get install -y valkey-server valkey-tools`
  *macOS*: `brew install valkey`

---

## Command reference

| Command                                 | Semantics (parity with Redis)                 |
| --------------------------------------- | --------------------------------------------- |
| `GZADD key score member`                | Add/update a member                           |
| `GZRANGE key start stop [WITHSCORES]`   | Inclusive range by rank                       |
| `GZRANK key member`                     | 0‑based rank or nil                           |
| `GZREM key member`                      | Remove member                                 |
| `GZSCORE key member`                    | Return score or nil                           |
| `GZCARD key`                            | Element count                                 |
| `GZPOPMIN / GZPOPMAX key [count]`       | Pop N lowest/highest                          |
| `GZRANDMEMBER key [count] [WITHSCORES]` | Random sampling                               |
| `GZSCAN key cursor`                     | Stateless incremental scan                    |

`GZSCAN` uses a score/member-based cursor. The initial cursor is `0`; subsequent
calls pass back the previous `<score>|<member>` pair. A `|` in member names is
escaped as `%7C`.

Differences from core Redis:

* Persistence (`SAVE`, AOF) is **disabled**; data is volatile between restarts.

---

## Architecture overview

```
           ┌────────────────┐
           │ Valkey server  │
           └──────┬─────────┘
                  │ Module ABI
      ┌───────────▼───────────┐
      │      gzset.so         │  (Rust, cdylib)
      │                       │
        │  • GZADD… commands    │
        │  • B‑tree per key     │
     └───────────┬───────────┘
                 │
      ┌───────────▼───────────┐
      │  ScoreSet (Rust)      │  in‑memory structure
      │  by_score: BTreeMap   │───► OrderedFloat<f64> → BTreeSet<&'static str>
      │  members: HashMap     │───► MemberId → score (O(1) lookup)
      │  pool: StringPool     │───► MemberId ↔ &'static str
      └───────────────────────┘
```

Each set lives as a Valkey key holding a `ScoreSet` value. Commands open the
key directly and operate on its data; no global map or flush handler is needed.
Valkey handles key eviction and expiry automatically.

> **Note:** member strings are interned in a per‑set pool. When a member is
> removed, its `Box<str>` is dropped and the ID is reused; the vector that
> stores IDs may retain capacity (amortized), but string memory itself is reclaimed.
Future work will:

1. Add RDB/AOF serialization hooks.
2. Swap the B‑tree for a *learned index* backed by GPU inference.

---

## Roadmap

| Milestone                         | ETA        | Details                             |
| --------------------------------- | ---------- | ----------------------------------- |
| Stable in‑memory B‑tree           | **Q3 ’25** | Complete persistence                 |
| Concurrent shard map              | Q3 ’25     | Eliminate global lock               |
| Learned index prototype (CPU)     | Q4 ’25     | Replace B‑tree with PGM/ALEX        |
| CUDA/ROCm offload (opt‑in)        | 2026       | GPU kernels for search / bulk load  |
| Cluster & RESP3 streaming support | 2026       | Scale‑out & client‑side cursors     |

---

## Contributing

Automated agents and humans follow the same rules – see
[**AGENTS.md**](AGENTS.md) for the exact checklist (formatting, Clippy,
commit style, etc.).
Issues and PRs are welcome even during the GPU hiatus; getting the B‑tree
version rock‑solid is the fastest path to the fun stuff.

---

## License

MIT – see [LICENSE](LICENSE) for full text.

