# Concurrent Reducer Execution — Implementation Guide

> Companion to `DESIGN_DECISIONS.md` (the "why" + rejected alternatives). This
> file is the build plan: what to implement, where it lands in the codebase, and
> in what order. When a decision changes, update both files.
>
> **For the implementing session:** read `DESIGN_DECISIONS.md` first. Do not
> re-open questions marked "rejected" there. Items below marked **[VERIFY]**
> require reading the actual source before coding — assumptions there are not yet
> confirmed.

**Target:** `clockworklabs/SpacetimeDB`, master (post-PR #5095, ~June 2026).

**One-line architecture:** prove disjointness statically at publish; batch only
contiguous non-conflicting queue prefixes; fork only reducers measured heavy
enough to pay for a thread; run their bodies in parallel over a shared immutable
snapshot; serialize the commits in FIFO order.

---

## Progress log

- **Phase 6 — learned sets + early-cut trap — ✅ BUILT (2026-06-12).** Executed
  `design/PHASE6_PLAN.md` U1–U8 via the dev-time tiered pipeline (sonnet
  U1/U2/U4/U6/U7, opus U3/U5), per-unit test gates, independent opus review of
  the full diff (verdict: **adequate**, zero CRITICAL/MAJOR; two cosmetic MINORs
  fixed — stale `dead_code` allow, Debug-only wildcard comment). What landed:
  - **U1** datastore key-level view check: `BatchTxState::view_refresh_nonempty`
    (read-guard port of `views_for_refresh`, both branches) extracted into the
    free fn `view_refresh_required`; `CommittedState::view_overlap_kind`
    (`None`/`KeyOnly`/`FullScan`). The batch-commit debug_assert is now key-level
    (`view_refresh_required`), not table-level. Parity must-trap test asserts
    batch ⇔ mut across write patterns (under-detection = corruption).
  - **U2** `ResolvedWrites` → `ResolvedAccess` + resolved `read_tables` (unknown
    read name ⇒ wildcard, same sound fallback as writes).
  - **U3** capture plumbing: `CaptureSpec{capture_access,check_views}`,
    `BatchBodyOutcome.{observed,view_refresh_needed}`, capture on inline + batch
    lanes, charge-committed-only energy (batch bodies skip `record_reducer`; the
    drain bills only committed outcomes via `record_reducer_energy`).
  - **U4** tier state machine (`AccessTier{Static,Unknown,Learned,Parked}` +
    strikes), pure `record_observation`/`record_escape`, tier-aware `admit`
    (Static×Static → matrix bit; any Learned side → IntSet intersection;
    resolved-wildcard Static treated as universal — soundness guard + test).
  - **U5** early-cut trap in `run_batch`: `earlier_writes` running union; member
    post-body trap (view → escape → hazard) before the next admission; head join
    trap (always commits; whole-batch view-abort; suffix cut at first conflict);
    requeue-at-pending-front = serial FIFO slot.
  - **U6** fixture `heavy_learn` (Debug call_indirect wildcard) + `set_flag`/`Flag`;
    end-to-end tests: learning→promotion→width-2 fork; trap (observed live:
    `head_traps=1, demotions=1`) with DB serial-correctness; strike-cap. View
    e2e DEFERRED (no ergonomic subscription helper; corruption-critical case is
    covered by the U1 datastore parity must-trap + U5 view-trap unit tests).
  - **U7** learned bench `bench_circles_learned`: promotes the Release-wildcard
    `run_game_ia_loop` (promotion at run 8), then A/B the verdict-named pair
    `run_game_circles ‖ run_game_ia_loop` — **cap=1: 20/20 width-2 forks,
    mean 36.5 ms; cap=0: forks=0, mean 38.6 ms** (~5.7%; bodies cost-matched).
    The learned path forks where the static path produced none — the verdict's
    demonstration.
  - Suites green: datastore 94 (`--features test`), access-analysis 17, core
    --lib 242, batching test 5 + off 1; clippy clean core+datastore. Source
    facts: DESIGN verification log "Phase 6 BUILT".
- **Cross-language benchmarks (2026-06-13, measurement-only).** Rust three-way
  (`bench_three_way`): static pair base→v1≡v2 = **1.47×** (Phase 6 zero-overhead
  on the static path); lost pair v1 forks=0 → v2 20/20 width-2 (learning recovers
  the Release-wildcard pair). **C# base vs v2** (C# has no static path — analyzer
  wildcards all C# wasm — so all batching is learning): the synthetic mixed-burst
  fixture ported to C# (`modules/reducer-batching-fixture-cs`) runs **1.57×**
  (28.6→18.2 ms, width-5 in 20/21 batches; all 7 reducers promote at run 8).
  **TypeScript: N/A** — V8 host never reaches `BatchingExecutor` (base ≡ v2).
  Key correction: §19.7 originally lumped C# with TS as "deferred"; C# is on the
  same wasmtime/`BatchingExecutor` as Rust and batches NOW via learning — only
  TS/V8 is deferred. New benches: `bench_three_way`, `csharp_batching_bench.rs`,
  `csharp_mixed_bench.rs`. Source facts: DESIGN log "Cross-language benchmarks".
- **Phase 6 — design grilled + plan APPROVED (2026-06-12), not yet built.**
  Eight forks DECIDED with the user (DESIGN §19): per-boot SchedulerState
  storage; immutable matrix + `AccessTier` overlay; union-stable promotion
  (≥8 runs, 4 quiet) + 2-strike park; **early-cut trap** (member check
  post-body before next admission, head check at join — victims structurally
  impossible; requeue-at-pending-front = serial FIFO slot); charge committed
  run only; tiered view speculation (members, key-only overlap); V8 deferred.
  Unit plan U1–U8 in `design/PHASE6_PLAN.md`. Source facts in DESIGN log
  "Phase 6 design grilled".
- **TASK A — Phase-3 verification — ✅ DONE (2026-06-10).** All three items
  resolved (SharedReadGuard<CommittedState> is Send; no background mutation under
  read guards; ViewReadSets = mirror-not-reuse). Two source-vs-doc corrections
  recorded (TxSlot is a field not a thread-local; `jobs.rs` is Tokio — don't
  reuse). Details: DESIGN verification log + §10/§15; Phase-3 `[VERIFY]`s below
  marked DONE.
- **TASK B — Phase-0 analyzer — ✅ DONE (2026-06-10).** Built as new crate
  `crates/access-analysis` (`spacetimedb-access-analysis`). Independent opus review:
  adequate, no under-approximation path. Tests: 5 unit + 5 integration green —
  perf-test Debug + Release + **wasm-opt-optimized** shape all exact;
  `modules/access-analysis-fixture` (2-table) proves read/write split + disjointness.
  Hardening: name-recovery const window de-capped (was 16) to remove the only
  eviction-based wrongness risk. Known sound over-approximation under release
  inlining (per-function, not per-call-site, attribution) — **deferred to TASK C
  measurement** (it's a shadow-mode output), not a bug. Only root `Cargo.toml`/
  `.lock` touched outside the new crate + fixture; no runtime/publish wiring.
- **TASK C — Phase 1+2 shadow harness — ✅ BUILT (2026-06-10).** Conflict matrix
  (`ConflictMatrix`, `crates/access-analysis/src/matrix.rs`, DESIGN §2 formula,
  diagonal = same formula so a writing reducer self-conflicts); dynamic capture
  (`MutTxId.observed: Option<Box<ObservedAccess>>` + Reducer arms in the existing
  `record_*` hooks + new write hooks in `instance_env.rs` at insert/update/
  delete_by_index×2/delete_all_by_eq/clear); shadow diff + strict-prefix batch sim
  + JSON report in `crates/core/src/host/shadow_access.rs`. Env-gated, default
  OFF: `STDB_SHADOW_ACCESS=1`, `STDB_SHADOW_ACCESS_REPORT=<dir>` (one
  `<database_identity>.json` per DB), `STDB_SHADOW_ACCESS_REPORT_INTERVAL_SECS`
  (default 60). Publish wiring: `make_actor` threads `program_bytes` →
  `WasmModuleHostActor::new` runs `analyze()` only when env on; failure → warn +
  no shadow, never blocks publish. Sim is UNGATED (no threshold) by decision —
  records raw widths + per-member runtimes; threshold/k applied offline.
  Independent opus review: **adequate** (findings fixed: cfg-gated release
  wildcard assert, seq-under-lock, LICENSE).
  - **Integration:** `modules/sdk-test-shadow-access` (write_a/b, heavy_a/b,
    write_c_1/2 conflict pair, read_c, read_d_1/2, indirect_touch wildcard via
    static fn-ptr array) + `sdks/rust/tests/shadow-access-client` (fire-and-forget
    phased bursts + sentinel flush) + `shadow_access_capture` in
    `sdks/rust/tests/test.rs` — **green** (31.6s). Static expectations added to
    `crates/access-analysis/tests/integration.rs` (Debug exact; Release
    sound-or-wildcard).
  - **First-run data:** `under_approx = 0` (the gate holds); width histogram
    `{1:126, 2:123, 32:1, 67:1}` — alternating disjoint *writers* cap at width 2
    (a writer self-conflicts with the next queued instance of itself), pure
    *readers* batch arbitrarily wide (67 observed). Findings in DESIGN §17.
- **TASK C — GO/NO-GO — ✅ DECIDED: GO (user, 2026-06-10).** Release-server
  measurement run: under_approx = 0 again; handoff proxy 53µs (≈10–50× the §8
  assumption — regime narrower than the bet, threshold ≈160–265µs at k=3–5);
  heavy fixture reducer (naive 500 row-ops) 295µs mean — clears k=3. GO rationale,
  skipped pre-checks, and the **post-implementation kill criterion** (don't raise
  the pool cap if real workloads never show profitable width≥2 batches against
  k × true measured fork cost) are recorded in DESIGN §17. **Next: TASK D,
  Phase 3 (BatchTxState).**
- **TASK D Phase 3 — BatchTxState — ✅ DONE (2026-06-10).** Built per DESIGN §15;
  independent opus review adequate (findings fixed). Datastore:
  `batch_tx.rs` (`BatchTxState`/`FinishedBatchTx`, StateView impl, row ops via the
  SAME free fns as `MutTxId` — `update` extracted verbatim to match; `clear_table`
  verified read-safe), `reducer_tx.rs` (`ReducerTx` trait +
  `ReducerTxVariant{Mut,Batch}`), `Locking::begin_batch_tx`/`commit_batch_tx`
  (merge verbatim; debug_assert tripwire on delete-pointer validity). Core:
  `TxSlot` holds the enum, host fns via trait, relational_db reducer wrappers
  generic; DDL/SQL/views stay `MutTxId`-concrete; module_host_actor/v8/wasmtime
  needed no edits; nothing constructs `Batch` until Phase 5 (zero behavior
  change). Tests: assert_send, Mut-vs-Batch equivalence, autoinc refill across
  chunk boundary, two-thread overlay + FIFO drain. **NEW FINDING for Phase 5:
  sequence refill writes `st_sequence` via tx_state — conflict-matrix blind spot,
  fold into the §17-finding-1 host-write rule decision (details: DESIGN
  verification log "TASK D Phase 3").** Verify:
  `cargo test -p spacetimedb-datastore --features test` (the feature flag is
  required), `cargo test -p spacetimedb-core --lib`.

- **TASK D Phase 4 — autoinc interplay — ✅ DONE (2026-06-11, verification-only,
  no code).** (a) MutTxId-vs-Batch interplay VERIFIED SAFE and stronger than the
  concern: lock order is documented (`datastore.rs:58-66`, memory →
  committed_state → sequence_state) and both acquirers comply (`begin_mut_tx`
  write guard :902 then seq mutex :903; `BatchTxState` read guard for life +
  per-draw seq lock). No deadlock cycle. The "batch draw blocks behind a serial
  reducer" worry is VACUOUS — MutTxId (write guard) and live overlays (read
  guards) are mutually exclusive via the committed-state RwLock itself, so no
  overlay exists while a MutTx holds the seq mutex. Real contention surface =
  overlay-vs-overlay per-draw on the one global SequencesState mutex (all
  sequences share it); short critical section, ≤2 contenders at cap=1.
  (b) Fast-path atomic fetch-add: **DEFERRED** until Phase-5 contention data —
  no correctness need; refill must stay under the lock anyway. Details: DESIGN
  verification log "TASK D Phase 4".

- **TASK D Phase 5 — scheduler + worker — ✅ BUILT (2026-06-11).** Implemented per
  the approved unit plan `design/PHASE5_PLAN.md` (U1–U8) after grilling the four
  §18 forks with the user. Shapes: `crates/core/src/host/reducer_scheduler.rs`
  (`BatchingExecutor` — typed `WasmJob::{Async,Sync,Reducer}` loop forked from
  `util/jobs.rs`, threshold-gated strict-prefix batching, two-stage calibration,
  `STDB_REDUCER_POOL_CAP`/`STDB_REDUCER_FORK_K`), `wasm_common/reducer_worker.rs`
  (self-constructing pinned worker, trap self-heal, channel-drop shutdown),
  `wasm_common/reducer_access.rs` (unconditional analyzer wiring + matrix +
  lazy name→TableId resolution), body/commit split in `module_host_actor.rs`
  (`call_reducer_body_batch`/`BatchBodyOutcome`), batch commit-downgrade in
  datastore + `commit_and_broadcast_batch_event`, shadow harness DELETED per the
  temp-table. Fixture `modules/reducer-batching-fixture` + integration tests
  (`crates/testing/tests/reducer_batching{,_off}_test.rs`). Independent opus
  review: adequate (3 MINOR fixed); comment audit done. Suites: core lib 216,
  datastore 89 (`--features test`), analyzer 16, integration 3.
  **Release measurement:** calibrated threshold (k=4) = 246.6µs ⇒ true fork cost
  ≈ 62µs (confirms the 53µs shadow proxy); width-2 fork fired round 0, FIFO +
  exact counts held. Kill criterion remains a REAL-workload gate (self-authored
  fixture caveat per §17) — pool stays lazy/dormant by construction; cap=0 is
  the hard off switch.

### TASK C edits: temporary vs permanent (required note)

| Edit | Status |
|---|---|
| `crates/access-analysis/src/matrix.rs` (`ConflictMatrix`) | **Permanent** — Phase 5 scheduler input |
| `MutTxId.observed` capture + `record_*` reducer arms (datastore) | **Permanent** — Phase 6 learned-sets / runtime trap reuse |
| `instance_env.rs` write-record hooks | **Permanent** — same |
| `program_bytes` → `WasmModuleHostActor::new` + `analyze()` publish wiring | **Permanent shape** — becomes unconditional (not env-gated) at Phase 5 |
| `crates/core/src/host/shadow_access.rs` (mirror, `PrefixBatchSim`, report writer, handoff proxy, env flags) | **TEMPORARY** — measurement only; superseded by the real scheduler at TASK D |
| `CallReducerParams.shadow_seq` + the two wasm enqueue registration sites | **TEMPORARY** — replaced by real queue introspection when the scheduler owns the queue |
| `enable_access_capture`/`take_observed`/diff block in `call_reducer_with_tx` | **TEMPORARY** — Phase 6 keeps a *variant* (runtime trap), not this diff |
| `sdk-test-shadow-access` module + `shadow-access-client` + `shadow_access_capture` test | **TEMPORARY with the harness** — keep while shadow exists (CI guard for under-approx==0); retire together |

- **Toolchain:** earlier broken local `clang` fixed (2026-06-10); plain `cargo`
  works, no linker override needed. `wasm-tools` 1.251 + `wasm-opt` v123 installed.

---

## Build order (phased — each phase is independently shippable/testable)

The phases are ordered so the **risky, cheap-to-build validation comes before
the expensive engine surgery**. Do not start Phase 3 until Phase 2 says the
profitable regime actually exists in real workloads.

### Phase 0 — Static access-set analysis (standalone, no runtime changes) — DECIDED build spec
Rationale + empirical findings: DESIGN §16. Pure function over `.wasm` + `ModuleDef`.
No scheduling, no datastore changes, **no publish-path wiring yet** (that's TASK C+).

**Where:** new standalone crate **`crates/access-analysis`** (`spacetimedb-access-analysis`).
Deps: `walrus` (latest), `spacetimedb-schema` (`ModuleDef`), `spacetimedb-lib`
(`Identifier`), `thiserror`. dev-deps: `spacetimedb-testing` (`CompiledModule`).
Add to workspace members + `walrus` to workspace deps. Keeps `walrus` out of `core`.

**Public API:**
```rust
pub struct AccessSet { pub reads: BTreeSet<Identifier>, pub writes: BTreeSet<Identifier>, pub wildcard: bool }
pub fn analyze(wasm: &[u8], module_def: &ModuleDef) -> Result<Vec<AccessSet>, AnalysisError>;
// Vec indexed by ModuleDef reducer order == reducer_id. Tables identified by NAME
// (ids aren't assigned until publish). wildcard == conflicts-with-everything.
```

**Module map:** `imports.rs` (classify host imports), `ids.rs` (name/provenance
recovery), `reducers.rs` (reducer-entry id), `reachability.rs` (DFS + call_indirect),
`analyze.rs` (driver), `examples/dump.rs` (eyeball tool).

**Algorithm:**
1. Parse with `walrus`.
2. **Classify host imports** → `func_id → HostOp{ Read|Write, TableId|IndexId-keyed }`
   for the **14 table ops** (full list in DESIGN §16; arg0 carries the id except
   `datastore_update_bsatn` = table_id@arg0 + index_id@arg1); locate
   `table_id_from_name`/`index_id_from_name`. **Unknown-op rule:** any reachable
   `spacetime_*` import not in the explicit allowlist (14 ops + 2 resolvers + known
   id-free safe ops: `row_iter_*`, `console_*`, `bytes_*`, `volatile_*`, etc.) ⇒
   wildcard the reducer.
3. **Name provenance** (`ids.rs`): for each `table_id_from_name(const_ptr,len)` /
   `index_id_from_name(...)`, read the data segment at `const_ptr` → UTF-8 name;
   bind it to the producing accessor fn / OnceLock static address (track both
   signals — robust to release inlining). Build `provenance_source → (name, Table|Index)`.
   Map index name → parent table via `ModuleDef` (`TableDef.indexes` keys). Assert
   in tests that the resolved index string matches an `IndexDef` name/source_name.
4. **Reducer entries** (`reducers.rs`): for each reducer in `ModuleDef` order, find
   its body wasm func via the **name section** (demangled `<module>::<reducer>`),
   cross-checked against the `__preinit__20_register_describer_<name>` **exports**
   (alphabetical export order = reducer_id; also the soundness-independent fallback
   if names are absent). Body not found ⇒ wildcard.
5. **Table-relevant set:** all funcs that can transitively (direct calls) reach a
   table op / resolver / unknown `spacetime_*` op.
6. **Per reducer:** DFS over **direct `call`** edges from the body. On `call_indirect`,
   constrain targets via element-segment ∩ type-signature; if any possible target
   ∈ table-relevant ⇒ wildcard, else ignore (so fmt/panic/log vtables don't wildcard
   everything).
7. **Provenance pass:** at each reachable table-op call site, resolve the id-arg
   (operand-stack / local tracking of values whose source is a known accessor or
   OnceLock address) → `TableId(name)` adds to reads/writes per op class;
   `IndexId(name)` maps to parent table then adds. Unresolved id ⇒ wildcard.
8. **Wildcard triggers** (⇒ reducer.wildcard = true): unresolved id, unknown reachable
   host op, dangerous call_indirect, non-const name string, body not found.

**Soundness rule (absolute):** over-approximate, never under. Every actual access
∈ predicted set, or the reducer is wildcard. When in doubt → wildcard.

**Validation / acceptance (bar = ZERO under-approximation):**
- Obtain `ModuleDef` in tests via `CompiledModule::compile(name, Debug|Release)` →
  `.program_bytes()` + `.extract_schema_blocking()` (`crates/testing/src/modules.rs`).
- **`modules/perf-test`, Debug AND Release(+wasm-opt):** `load_location_table` →
  writes `{location}`, reads `{}`, not wildcard; the 4 `test_index_scan_*` →
  reads `{location}`, writes `{}`, not wildcard.
- **New 2-table fixture** (`crates/access-analysis/tests/fixtures/`): tables A,B;
  reducers `writes_a`, `reads_b`, `reads_a_writes_b`, `touches_both` → assert the
  read/write split and that `writes_a` ∩ `reads_b` are disjoint (batchable).
- Unit tests: import classification + const-string recovery on a minimal hand-built
  wasm (wat → bytes).
- `examples/dump.rs`: `cargo run --example dump -- <module>` prints per-reducer sets.

**Build/verify:** `cargo test -p spacetimedb-access-analysis` (clang fixed; no
override). wasm32 target installed; tests build modules via `CompiledModule`.

**Non-Rust modules:** out of scope to handle → wildcard (sound). Language-agnostic
recovery = dynamic capture (Phase 1/6).

### Phase 1 — Conflict matrix + shadow-mode harness (still no behavior change)
- **Conflict matrix:** symmetric N×N bitset. `conflict(i,j)` =
  `wildcard_i || wildcard_j || writes_i ∩ (reads_j ∪ writes_j) || writes_j ∩ reads_i`.
  Build once after analysis; store beside `ModuleDef`/`ModuleInfo`.
- **Dynamic capture (shadow):** in `InstanceEnv`, record per-invocation observed
  read/write table sets (each `datastore_*` host fn inserts its resolved table id
  into a per-call set; map index_id→table_id same as static). Reset per
  invocation, lives next to the tx slot.
- **Shadow diff:** after each reducer, compare observed vs static-predicted.
  Log/count two classes separately:
  - **under-approximation** (observed ⊄ predicted): CORRECTNESS-CRITICAL. Must be
    zero before any scheduler trusts the matrix. Alert loudly.
  - **over-approximation** (predicted ⊋ observed): false-conflict rate; informs
    how much parallelism is being left on the table.
- **Also record (Phase 2 input):** per-reducer runtime, and reconstruct what
  batch widths *would have* formed.

### Phase 2 — Go/No-Go measurement (decision gate, no new engine code)
Run Phases 0–1 in shadow on representative real workloads. Decide whether to
proceed based on:
- under-approximation count == 0 (else fix the analyzer; do not proceed).
- batch-width distribution: are width≥2 batches actually common?
- runtime-vs-handoff: do reducers in those batches clear `k × handoff_cost`?

If the profitable regime is rare, STOP — the feature is a narrow optimization not
worth the engine surgery. (See DESIGN §8.) If it's present, proceed to Phase 3.

### Phase 3 — BatchTxState decomposition (the hard engine work) — ✅ DONE (see progress log)
**Source-confirmed (June 2026): the read foundation is sound and SIMPLER than
first designed.** `committed_state` is already `Arc<RwLock<CommittedState>>`; all
read paths are `&self` with no lazy mutation (DESIGN §14). `MutTxId` serializes
reducers only because it holds the RwLock WRITE guard for its whole life; its
`!Send` is a synthetic `PhantomData<Rc<()>>`. `TxId` already holds a READ guard
with no `!Send` marker. So:
- **No `CommittedSnapshot` type to invent** — the `Arc<RwLock<CommittedState>>`
  read guard IS the shared immutable view. N overlays hold concurrent read guards.
- **No warm-before-freeze** — no read mutates shared state.
- **`BatchTxState`** = `MutTxId`'s `tx_state` delta + a `SharedReadGuard<CommittedState>`
  (like `TxId`), MINUS the write guard, MINUS the synthetic `!Send`. Deletes are
  `Vec<RowPointer>` in `tx_state` (existing model). It's `Send` by construction
  because it holds only a read guard (cannot mutate committed state).
- **Drain** acquires the WRITE guard on the home thread, one reducer at a time,
  and calls the existing `CommittedState::merge(&mut self, tx_state, read_sets, ctx)`
  (`committed_state.rs:474`) in FIFO order. Reuse verbatim.
- **Sequences**: `Arc<Mutex<SequencesState>>` already exists (`datastore.rs:70`);
  concurrent overlays draw under it (Phase 4 atomicity rides this).
- **`TxSlot` → per-worker** as before; each worker resolves reads via its overlay's
  read guard + `tx_state`, layered exactly as `MutTxId` layers `tx_state` over
  committed reads (the `StateView` trait abstracts this). Do NOT route through
  procedure-side `CallContext` (#5095 separation).
- **[VERIFY, small] DONE (TASK A):** `SharedReadGuard<CommittedState>` IS `Send`.
  `cargo check` passes `assert_send::<SharedReadGuard<CommittedState>>()`; alias is
  `ArcRwLockReadGuard<RawRwLock, _>`, parking_lot built with `send_guard`
  (`Cargo.toml:249`), `CommittedState: Send + Sync`. (DESIGN verification log [A1].)
- **[VERIFY] DONE (TASK A):** no background compaction/GC mutates committed pages
  while read guards are held — confirmed none exists; all page-free/blob-refcount
  paths are `&mut` write-side; the only async worker compresses commitlog segments,
  not pages. So a RowPointer captured in an overlay stays valid to drain (read
  guard for batch life + disjointness). One latency note: the snapshot worker takes
  the committed-state *write* lock, so snapshots and reducer batches are mutually
  exclusive (correct, but contend). (DESIGN verification log [A2].)

### Phase 4 — Autoinc atomicity — ✅ DONE (2026-06-11, verification-only; see progress log)
- **Scope shrank after Phase 3:** `BatchTxState` already draws under a per-call
  lock of the `Arc<Mutex<SequencesState>>` (the whole draw+refill is one short
  critical section), so concurrent overlays are already atomic. Remaining Phase-4
  work: (a) verify the MutTxId-vs-Batch interplay (a `MutTxId` holds the
  sequences mutex for its whole TX, so a batch can't draw while a serial reducer
  runs — correct, possibly slow); (b) decide whether a fast-path atomic fetch-add
  on the in-memory counter is worth it (defer until Phase-5 contention data
  exists). Nothing else changes — no sub-ranges, no deferral. (See DESIGN §7.)
- The st_sequence refill write rides the overlay's tx_state (NEW FINDING — see
  DESIGN verification log "TASK D Phase 3"; fold into the Phase-5 host-write rule).
- Add a doc note: autoinc IDs interleave across concurrent reducers (already true
  across separate txns; gaps are documented).

### Phase 5 — Scheduler + worker pool (the three changes)
Implement on top of #5095's synchronous reducer lane. See DESIGN §9 for full
rationale and **DESIGN §18 for the four grilled-and-decided Phase-5 forks
(2026-06-11): view-aware admission rule, host-owned typed-job drain loop (+
shadow harness deletion), self-constructing pinned worker, two-stage threshold
calibration.** The pseudocode below predates §18; where they disagree, §18 wins
(notably: admission predicate gains the tx=None/lifecycle/view-overlap checks;
`AutoReplacingModuleInstance` does not exist — hot-swap is whole-ModuleHost
replacement; fork-cost measurement is the §18.4 two-stage calibration).
**Post-build (2026-06-11): the "fork the heaviest" rule below is SUPERSEDED —
the HEAD forks (user decision; §13 supersession note + §18 amendments). The
as-built flow: lone-head guard → begin+view-check head → dispatch → per-member
admit/begin/check/execute pipeline on home → join → FIFO drain.**
Control flow:

```
on reducer dequeued (head of FIFO):
    if runtime_stats[head].high_water < THRESHOLD
       OR runtime_stats[head].ema < THRESHOLD:        # both must exceed
        run_inline(head)                               # #5095 sync lane, untouched
        record_runtime(head)
        return

    batch = [head]
    for next in queue:                                 # build disjoint prefix
        if any(conflict(next, m) for m in batch): break
        batch.push(pop(next))

    forks = [r for r in batch if fork_worthy(r)]
    # v1: at most 1 fork dispatched to the worker (cap=1). Home runs the rest,
    # INCLUDING any further fork-worthy reducers, inline — home is a real
    # execution lane. So fork the single heaviest, run everything else on home.
    if forks:
        heaviest = max(forks, key=lambda r: runtime_stats[r].high_water)
        ensure_worker_spawned()            # no-op if already warm
        dispatch(heaviest, to=worker)      # moves BatchTxState to worker
        for r in batch if r is not heaviest:
            run_inline(r)                  # useful work during the parallel window
        join(worker)                       # barrier: heaviest's BatchTxState back
    else:
        for r in batch: run_inline(r)
    for r in batch in ORIGINAL FIFO ORDER:             # serial drain
        commit(r)   # merge delta→committed, commitlog, offset, broadcast
        record_runtime(r)
        maybe_spawn_worker_in_background(r) # if r just tripped threshold & no worker yet
```

- **`runtime_stats[r]`** — parallel array, NOT inside `AccessSet`. Holds
  `{ high_water, ema }`. Init 0. Update both after every invocation (inline or
  forked).
- **Threshold:** `THRESHOLD = k * measured_fork_cost`, k ≈ 3–5. Measure
  `fork_cost` once at startup as a **no-op reducer round-trip on a pool worker**
  (dispatch → trivial reducer → return) — NOT a bare thread ping, because a pool
  worker runs in its own wasm instance/linear memory and reads committed state
  cold (handoff + cold-instance cost). Store globally. (See DESIGN §11.)
- **Worker pool = per-module, lazily-sized set of warm (OS thread + wasm
  instance) pairs**, scoped INSIDE the per-database `WasmtimeModuleHost` (NOT a
  shared host-wide pool — see DESIGN §12 for why; tenant isolation + no existing
  shared pool to extend). A wasm instance is module-specific, so workers are
  inherently (thread, instance, module)-bound. Running a reducer body requires a
  wasm instance whose transaction slot points at that reducer's `BatchTxState`.
  **[CORRECTION, TASK A]** the slot is the `InstanceEnv.tx: TxSlot` *field*
  (`TxSlot { inner: Arc<Mutex<Option<MutTxId>>> }`), **not** a thread-local as
  earlier drafts said. Instance pinning still holds. Note `InstanceEnv` is `!Send`
  today (it holds a `!Send` `MutTxId`); a worker must hold an `InstanceEnv` whose
  slot carries a `BatchTxState` (Send), not a `MutTxId`. (DESIGN §10 correction.)
  - **Lazy: zero workers until a module actually forks.** Grow to a small cap on
    demand; reap idle workers. All-cheap modules (the common case) keep a pool of
    size 0 and cost nothing. Thread count scales with modules-doing-heavy-work,
    not modules-published.
  - **v1 cap: max 1 worker (configurable, default 1)** — see DESIGN §13.
    Home thread + 1 worker = **effective width-2, BOTH lanes execute** (home is an
    execution resource, not just a dispatcher). Two heavy disjoint reducers → fork
    one to the worker, run the other inline on home. Only ceiling: 3+
    simultaneously-runnable heavy disjoint reducers get 2-wide, not N-wide.
  - **Lazy spawn timing:** spawn the worker **after the commit** of the reducer
    that first trips the threshold (in the background) — NOT during its hot path.
    That reducer runs inline (only learned slow as it finished); the warm worker
    pays off on the *next* fork-warranting batch. Instantiation cost stays off the
    critical path.
  - Topology per module: **1 home worker (the #5095 sync lane, untouched) + 0..1
    lazily-spawned pool worker (v1).**
  - Pool workers ONLY run reducer bodies into their `BatchTxState`; they NEVER
    commit. Home worker is the drain thread; `DrainGuard`/write lock lives only
    there.
  - Crosses to worker (all `Send`): `Arc<CommittedSnapshot>`, reducer id + owned
    args, empty `BatchTxState`. Back: populated `BatchTxState`. NEVER crosses: the
    wasm instance (pinned to its thread), the `DrainGuard`.
  - **[VERIFY] resolved (TASK A): do NOT reuse `crates/core/src/util/jobs.rs`** —
    it is `JobCores`, a pool of single-threaded **Tokio** executors (async), the
    runtime infra reducer workers must stay off (§5/§10). Use plain OS threads +
    per-worker channel + counting join barrier.
  - Cap + idle-reap TTL: small fixed cap for v1; tune against warm-instance memory
    cost **[VERIFY]**.
- **Module hot-swap [NEW lifecycle surface]:** warm pool instances go stale on
  republish. Replace pool instances atomically with the main lane's on module
  reload (same hook as `AutoReplacingModuleInstance`). Must not leave a worker
  holding a stale instance across a republish.
- **Commit order invariant:** the drain commits in original FIFO order
  regardless of which thread executed which body. This is what preserves
  observable ordering; disjointness makes the execution interleaving invisible.

### Phase 6 — Learned sets + early-cut trap (✅ BUILT 2026-06-12; see progress log)
Verdict-mandated before the upstream PR (kill-criterion VERDICT log entry).
Lifts the two big admission exclusions: wildcard reducers (release-inlining
losses — the named pair `run_game_circles × run_game_ia_loop`; non-Rust
modules ride later host adoption) and view-overlap writers (key-level
speculation). All forks DECIDED — DESIGN §19; build plan with unit-level
acceptance gates in **`design/PHASE6_PLAN.md`** (U1 datastore view check →
U2 resolved reads → U3 capture plumbing → U4 tier machine → U5 early-cut
trap → U6 fixture/integration → U7 learned ia_loop×circles bench → U8
review/audit/docs). Promotion: union-stable ≥8 committed runs + 4 quiet;
demotion: strike cap 2 → parked. Trap: early-cut — member checked post-body
(harmless escape commits + cuts; conflicting escape requeues at pending front
+ cuts), head checked at join (first conflicting member cuts the suffix);
nothing commits before validation. See DESIGN §6 (tier model) + §19.

---

## Invariants the implementation must never violate

1. **No under-approximation is ever trusted.** Static analysis must be sound
   (over-approximate to wildcard). Shadow mode must show zero under-approximations
   before the scheduler acts on the matrix.
2. **Committed-state mutation stays single-threaded** (only in the serial drain).
   Only reads are concurrent. Never make the write path thread-safe — avoid the
   need.
3. **Commit drain is always FIFO order**, independent of execution thread.
4. **Per-reducer atomicity and per-reducer broadcast are preserved** — the batch
   is a scheduling unit, not a transactional unit. Commits stay independent.
5. **The all-cheap workload pays ~one comparison** over #5095's sync lane —
   batch construction is gated on the threshold, not just batch execution.
6. **Reducer and procedure runtimes stay separate** (per #5095). Reducer worker
   threads are plain OS threads, not procedure-async tasks.

## Cross-references
- Rationale, rejected alternatives, source-verified facts → `DESIGN_DECISIONS.md`.
- Items marked **[VERIFY]** must be confirmed against source before relying on them.
