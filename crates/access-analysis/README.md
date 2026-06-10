# spacetimedb-access-analysis

Static per-reducer table access-set analysis for SpacetimeDB WASM modules.

Given a module's `.wasm` bytes and its validated `ModuleDef`, computes for each
reducer the set of tables it reads and the set it writes — or marks the reducer
**wildcard** (conflicts with everything) when a sound, finite access set cannot
be proven. The output feeds disjointness-based reducer scheduling (conflict
matrix, shadow-mode validation, and eventually the concurrent batch scheduler).

## Public API

```rust
pub struct AccessSet {
    pub reads: BTreeSet<Identifier>,   // table names (ids aren't assigned until publish)
    pub writes: BTreeSet<Identifier>,
    pub wildcard: bool,                // conflicts with everything
}

pub fn analyze(wasm: &[u8], module_def: &ModuleDef) -> Result<Vec<AccessSet>, AnalysisError>;
// Vec indexed by ModuleDef reducer order == ReducerId.

pub struct ConflictMatrix;             // symmetric N×N bitset over reducer ids
impl ConflictMatrix {
    pub fn build(sets: &[AccessSet]) -> Self;
    pub fn conflicts(&self, i: usize, j: usize) -> bool;  // O(1)
}
```

Conflict rule (`matrix.rs`): `conflict(i, j)` iff either side is wildcard, or
one side's writes intersect the other's reads ∪ writes. **read ∩ read is NOT a
conflict** — merging reads and writes would serialize all readers of a shared
table. The diagonal uses the same formula, so two queued invocations of the
same *writing* reducer conflict while pure readers do not.

## Soundness rule (absolute)

Over-approximate, never under. Every table a reducer actually touches is in its
predicted set, or the reducer is wildcard. An under-approximation is a
data-corruption bug for any consumer that schedules on disjointness. Any
uncertainty — unresolved id argument, unknown reachable `spacetime_*` import,
`call_indirect` with a possible table-relevant target, non-constant name
string, missing reducer body, unmatched codegen pattern, non-Rust module —
wildcards the reducer. Wildcard is always a sound (if useless) answer.

## How it works (module map)

| File | Role |
|---|---|
| `imports.rs` | Classify host imports: 14 table ops (read/write, table_id- or index_id-keyed), the 2 name resolvers, known id-free safe ops; anything else reachable ⇒ wildcard |
| `ids.rs` | Id provenance: read the constant name string passed to `table_id_from_name` / `index_id_from_name` from the data segment; bind it to the producing accessor fn / OnceLock static address (survives release inlining) |
| `reducers.rs` | Find each reducer's body function via the wasm name section, cross-checked against `__preinit__20_register_describer_<name>` exports |
| `reachability.rs` | DFS over direct call edges; `call_indirect` targets constrained by element-segment ∩ type-signature (fmt/panic vtables don't wildcard everything) |
| `analyze.rs` | Driver: per reducer, walk reachable table-op call sites, resolve each id argument to a table (index → parent table via `ModuleDef`), classify read/write |
| `matrix.rs` | `ConflictMatrix` bitset construction from the access sets |

## Testing

Prerequisites: `wasm32-unknown-unknown` target installed; integration tests
compile real modules via `spacetimedb-testing`'s `CompiledModule`. The
wasm-opt-shape test additionally wants `wasm-opt` on PATH (it is skipped with a
note if absent). Typical PATH additions: `~/.cargo/bin`, `~/.local/bin`.

```sh
# Unit tests only (import classification, const-string recovery, matrix) — fast
cargo test -p spacetimedb-access-analysis --lib

# Full suite incl. integration tests (compiles fixture modules; several minutes cold)
cargo test -p spacetimedb-access-analysis
```

Integration tests (`tests/integration.rs`) and what each proves:

| Test | Module | Proves |
|---|---|---|
| `perf_test_debug` / `perf_test_release` | `modules/perf-test` | Exact sets on a real module: `load_location_table` writes `{location}`, the 4 `test_index_scan_*` read `{location}`, no wildcards |
| `perf_test_release_wasm_opt` | `modules/perf-test` + `wasm-opt -all -g -O2` | The name-section/provenance fingerprints survive the production optimization pipeline |
| `fixture_debug` / `fixture_release` | `modules/access-analysis-fixture` | Read/write split and disjointness on a 2-table module (the batchability primitive) |
| `shadow_access_fixture_debug` / `_release` | `modules/sdk-test-shadow-access` | Shadow-harness expectations: per-regime sets, `indirect_touch` forced wildcard via fn-pointer array |

Release assertions use sound-superset guards rather than exact equality where
release inlining is known to lose attribution (see Limitations). The acceptance
bar everywhere is **zero under-approximation**: an observed access outside the
predicted non-wildcard set is a bug in this crate, never tolerable noise.

Runtime (dynamic) validation lives outside this crate: the shadow-mode harness
(`crates/core/src/host/shadow_access.rs`, env `STDB_SHADOW_ACCESS=1`) diffs
these predictions against observed table access on a live server, and the SDK
test `shadow_access_capture` (`sdks/rust/tests/test.rs`) asserts
`under_approx == 0` end-to-end.

### Eyeball tool

```sh
cargo run -p spacetimedb-access-analysis --example dump -- <module-name> [--release]
# e.g.
cargo run -p spacetimedb-access-analysis --example dump -- sdk-test-shadow-access
```

Compiles `modules/<module-name>` and prints per-reducer reads/writes/wildcard.

## Limitations

- **Release inlining can wildcard reducers that Debug resolves exactly.**
  Attribution is per-function, not per-call-site; aggressive inlining
  (`wasm-opt`) can merge accessor chains and lose the id provenance for some
  reducers (observed: one of two identical-shape writers wildcarded,
  deterministically). Sound, but costs parallelism; per-call-site attribution
  is the known improvement if measured false-conflict rates warrant it.
- **Non-Rust modules (C#, C++, TS) ⇒ all wildcard.** The Rust bindings' codegen
  fingerprint (OnceLock + constant name string) is what makes ids recoverable.
  Language-agnostic recovery is dynamic capture at the host functions, not
  static analysis.
- **Table granularity only.** Row/key-level disjointness is not statically
  recoverable; high-contention single tables serialize by design.
- Host-internal table writes performed by the SpacetimeDB host itself inside a
  reducer transaction (e.g. `st_client` maintenance on disconnect) are outside
  the module wasm and invisible here; schedulers must handle lifecycle reducers
  separately (see `design/DESIGN_DECISIONS.md` §17).
