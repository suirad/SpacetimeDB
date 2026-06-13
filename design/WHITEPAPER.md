# Concurrent Reducer Execution in SpacetimeDB
## Provably-Safe, Profit-Gated Parallelism via Static Access Analysis and Strict-Prefix Batching

> **Status of this document.** Technical whitepaper covering the design as of
> 2026-06-13. **Phases 0–6 are built, reviewed-adequate, and measured** (all
> test suites green); the verdict-mandated gate — learned access sets + the
> early-cut trap — is satisfied, and the next step is the upstream PR.
> Cross-language results (Rust, C#) are included; TypeScript/V8 batching is
> deferred (its host does not yet run the batching executor). Companion
> documents: [`DESIGN_DECISIONS.md`](DESIGN_DECISIONS.md) (full decision log,
> rejected alternatives, and the per-phase build records) and
> [`IMPLEMENTATION_GUIDE.md`](IMPLEMENTATION_GUIDE.md) (phased build plan and
> invariants).

---

## Index

- [1. The Core Thesis](#1-the-core-thesis)
- [2. Problem and Motivation](#2-problem-and-motivation)
  - [2.1 The serial reducer lane](#21-the-serial-reducer-lane)
  - [2.2 Why this hasn't been done before](#22-why-this-hasnt-been-done-before)
  - [2.3 Design constraints (non-negotiable)](#23-design-constraints-non-negotiable)
- [3. Architecture Overview](#3-architecture-overview)
- [4. Why This Approach Wins](#4-why-this-approach-wins)
  - [4.1 Measured performance](#41-measured-performance)
  - [4.2 Cross-language results: static path vs learned path](#42-cross-language-results-static-path-vs-learned-path)
  - [4.3 Zero cost when idle](#43-zero-cost-when-idle-by-construction-not-by-tuning)
  - [4.4 Safety is layered and was validated empirically](#44-safety-is-layered-and-was-validated-empirically)
  - [4.5 Engineering risk was spent in the right order](#45-engineering-risk-was-spent-in-the-right-order)
- [5. Component Design](#5-component-design)
  - [5.1 Static access analysis](#51-static-access-analysis)
  - [5.2 Conflict matrix](#52-conflict-matrix)
  - [5.3 `BatchTxState`: decomposing the `!Send` transaction](#53-batchtxstate-decomposing-the-send-transaction)
  - [5.4 The scheduler (`BatchingExecutor`)](#54-the-scheduler-batchingexecutor)
  - [5.5 The worker (`ReducerWorker`)](#55-the-worker-reducerworker)
  - [5.6 Sequences and autoinc](#56-sequences-and-autoinc-a-non-problem-verified)
  - [5.7 Phase 6: learned sets and the early-cut trap](#57-phase-6-learned-sets-and-the-early-cut-trap-built-and-measured)
- [6. Validation Methodology and Results](#6-validation-methodology-and-results)
- [7. Invariants](#7-invariants)
- [8. Configuration and Observability](#8-configuration-and-observability)
- [9. Limitations and Future Work](#9-limitations-and-future-work)
- [10. Summary](#10-summary)

---

## Abstract

SpacetimeDB executes all reducers — its atomic, deterministic,
WebAssembly-hosted transactional functions — on a single serial OS thread per
database. This work adds
concurrent reducer execution **only when it is provably safe and measurably
profitable**, without regressing the synchronous single-thread lane that the
engine deliberately optimized (PR [#5095](https://github.com/clockworklabs/SpacetimeDB/pull/5095)), and without weakening
serializability or per-reducer atomicity.

The approach replaces general concurrency control with three cheaper
mechanisms composed end-to-end:

1. **Static access analysis** of the module's WASM binary at publish time
   proves, per reducer, which tables it can read and write — soundly
   over-approximated, never under-approximated.
2. **Admission control** batches only contiguous, pairwise-disjoint prefixes
   of the existing FIFO queue, and only when a runtime profitability threshold
   says the parallelism will pay for its own thread handoff.
3. **A serial commit drain** applies every transaction's effects in original
   FIFO order on a single thread, so committed-state mutation never becomes
   concurrent.

On a real game-shaped benchmark module not written for this feature, the
system delivers **1.41–1.86× round-time speedups** at an effective parallel
width of 2, while the all-cheap workload — the common case — pays a measured
**zero penalty** (one branch comparison per reducer). The feature ships
dormant by default: zero worker threads exist until a module demonstrably
earns one, and a single environment variable disables it entirely.

---

## 1. The Core Thesis

> **Isolation comes from admission control plus a serial commit drain — not
> from general concurrency control.**

If two reducers provably touch disjoint table sets, executing their bodies in
parallel against the same immutable snapshot and then committing them in queue
order is *observationally identical* to running them serially: commit order is
the only externally visible order, and disjointness makes the execution
interleaving invisible. The engine therefore never needs concurrent writers,
row locks, or validation-and-retry for the core path. The hard problem is
avoided by construction, not solved.

Profitability is handled the same way safety is — by gating, not by hoping.
Thread handoff on real hardware measured ~62µs (an order of magnitude above
folk estimates), so parallelizing a 10µs reducer is strictly net-negative.
The scheduler forks only reducers whose **measured** runtime clears
`k × measured_fork_cost` (k = 4 by default), with the fork cost calibrated at
runtime on the actual worker. Hardware with slow handoff automatically raises
its own bar.

---

## 2. Problem and Motivation

### 2.1 The serial reducer lane

Every state mutation in a SpacetimeDB database flows through reducers, and all
reducers for a database execute on one dedicated OS thread, one at a time.
PR [#5095](https://github.com/clockworklabs/SpacetimeDB/pull/5095) (May 2026) made this lane *synchronous* — stripping async/Tokio
overhead — on the thesis that most reducers are tiny (1–50µs) and the serial
path should be as fast as possible.

That thesis is correct for the median reducer, but it leaves two costs on the
table:

- **Wasted cores on heavy reducers.** Reducers with real per-call compute —
  table scans, pathfinding, physics ticks, matchmaking, bulk imports,
  scheduled jobs — run hundreds of microseconds to tens of milliseconds. Two
  such reducers touching unrelated tables still execute strictly one after
  the other.
- **Head-of-line blocking.** Under strict FIFO, a burst of cheap reducers
  queued behind one expensive reducer waits for the entire expensive call,
  even when every one of them is provably independent of it.

### 2.2 Why this hasn't been done before

The obvious blockers are real:

- The mutable transaction handle (`MutTxId`) holds the committed-state
  **write lock for its entire lifetime** — that lock is *why* reducers
  serialize — and is explicitly marked `!Send` (PR [#4039](https://github.com/clockworklabs/SpacetimeDB/pull/4039)).
- Reducer bodies run inside a wasm instance whose transaction slot must point
  at the live transaction; instances are pinned to their thread.
- General-purpose concurrency control (locking, OCC with validation, MVCC)
  is a large engineering surface with pervasive runtime cost, and it taxes
  exactly the all-cheap workload [#5095](https://github.com/clockworklabs/SpacetimeDB/pull/5095) optimized.

The contribution of this design is showing that none of these blockers
requires general concurrency control. Each dissolves under a narrower
mechanism, and the composition preserves every existing semantic guarantee.

### 2.3 Design constraints (non-negotiable)

1. **Soundness over parallelism.** A missed conflict is a data-corruption
   bug. Every layer over-approximates: when in doubt, a reducer is treated as
   conflicting with everything.
2. **The all-cheap workload pays ~one comparison** over the existing lane.
   No batch construction, no matrix lookups, no allocation on the fast path.
3. **Per-reducer atomicity and per-reducer broadcast are preserved.** A batch
   is a *scheduling* unit, never a transactional unit; commits stay
   independent.
4. **Observable ordering is preserved.** Commits always land in original FIFO
   order, regardless of which thread executed which body.
5. **Reducer and procedure runtimes stay separate** (per [#5095](https://github.com/clockworklabs/SpacetimeDB/pull/5095)). Procedures
   are SpacetimeDB's *other* module-function type — side-effect-capable
   (HTTP, explicit transactions), non-deterministic, and run on their own
   async runtime; they are a disjoint concept from reducers and are untouched
   by this work. Worker threads are plain OS threads, not tasks on the
   procedure async runtime.

---

## 3. Architecture Overview

The system is a pipeline from publish time to commit time:

```
 publish time                     runtime (per reducer call)
┌─────────────────┐   ┌──────────────────────────────────────────────────┐
│ WASM binary     │   │ FIFO queue (typed jobs)                          │
│   │             │   │   │                                              │
│   ▼             │   │   ▼                                              │
│ static access   │   │ threshold gate ── below ──► inline on sync lane  │
│ analysis        │   │   │ (head_runtime > k × fork_cost?)   (today's   │
│   │             │   │   ▼ above                              path)     │
│   ▼             │   │ admission: walk queue prefix,                    │
│ per-reducer     │──►│ admit while disjoint (conflict matrix)           │
│ AccessSet       │   │   │                                              │
│ {reads,writes,  │   │   ▼                                              │
│  wildcard}      │   │ head ──────────► worker thread (parallel body    │
│   │             │   │ members ──────► home thread     over shared      │
│   ▼             │   │   │              (sequential)    read snapshot)  │
│ N×N conflict    │   │   ▼                                              │
│ matrix (bitset) │   │ join ► serial commit drain, original FIFO order  │
└─────────────────┘   └──────────────────────────────────────────────────┘
```

**Execution of a batch proceeds in three bands:**

1. **Shared read snapshot.** Committed state already lives behind
   `Arc<RwLock<CommittedState>>`; every batch member takes a concurrent
   *read* guard. No snapshot copy, no freeze step — verified against source
   that no read path lazily mutates shared state.
2. **Parallel overlays.** Each member runs with its own private staging delta
   (`BatchTxState`) layered over the shared snapshot — the same overlay model
   the serial transaction already uses. Disjoint table sets mean no overlay
   can observe another's data.
3. **Serial commit drain.** The home thread acquires the write lock one
   member at a time, in original FIFO order, and merges each delta using the
   *existing, unmodified* `CommittedState::merge` — commitlog append, offset
   bump, and subscriber broadcast per member, exactly as today.

**Concurrency topology (v1):** one home thread (the unmodified [#5095](https://github.com/clockworklabs/SpacetimeDB/pull/5095) lane —
it executes bodies, runs the drain, and is the only place the write lock
lives) plus **at most one** lazily-spawned worker per module. Effective
width 2, with *both* lanes doing real work: the batch head is dispatched to
the worker while the remaining admitted members execute sequentially on home.
The cap is configurable; raising it is an evidence-gated future step, not a
redesign.

---

## 4. Why This Approach Wins

### 4.1 Measured performance

All numbers are A/B on identical binaries, batching on (`POOL_CAP=1`) vs off
(`POOL_CAP=0`), release builds.

| Workload | Speedup | Notes |
|---|---|---|
| Synthetic heavy disjoint pair (20k row-ops each) | **1.53×** | 38.8 vs 59.4 ms/round; width-2 fork in 40/40 rounds |
| Synthetic mixed burst (2 heavy + 10 cheap, 7 tables) | **1.32×** | width ≥4–5 batches in 40/40 rounds |
| Real module, heavy hetero pair (`run_game_circles ∥ update_position_with_velocity`) | **1.41×** | of a 1.49× ceiling set by the longer member |
| Real module, pure-reader self-pair (`run_game_circles ∥ run_game_circles`) | **1.86×** | of the 2.0× theoretical width-2 ceiling |
| Real module, 5-call mixed burst | **1.39×** | width-3 batches in 38/40 rounds; truncation at a genuine write-write conflict, as designed |
| All-cheap fast path | **no measurable change** | 68–80µs/call vs 54–84µs/call; ranges overlap, client jitter dominates |

The "real module" is the in-repo `benchmarks` module (the circles/ia-loop
game simulation), compiled in shipping shape (Release + wasm-opt) and **not
written with this feature in mind**. Batch members in these runs exceeded the
profitability threshold by ~135× — these are not marginal forks.

### 4.2 Cross-language results: static path vs learned path

A base / v1 / v2 comparison isolates what each layer contributes — base =
pool off (today's serial lane); **v1** = static-analysis batching only;
**v2** = v1 plus Phase 6 learned sets. (Single-run point estimates on one
machine; the qualitative signals — does it fork, how wide — are the robust
part.)

| Module language | Pair | base | v1 (static) | v2 (learned) |
|---|---|---|---|---|
| **Rust** | `run_game_circles ∥ update_position_with_velocity` (statically analyzable) | 105.6 ms | 71.8 ms (**1.47×**, 39/39 width-2) | 71.8 ms (**≡ v1**) |
| **Rust** | `run_game_circles ∥ run_game_ia_loop` (ia_loop Release-wildcarded by inlining) | 39.2 ms | 38.2 ms (**forks 0** — can't batch) | 36.1 ms (**20/20 width-2**, ia_loop promoted at run 8) |
| **C#** | mixed burst, 7 tables, 2 heavy + 10 cheap (no static path — analyzer is Rust-keyed) | 28.6 ms | ≡ base (forks 0) | **18.2 ms (1.57×)**, width-5 in 20/21, all 7 reducers promoted |

Three things this table establishes:

- **Phase 6 is free on the static path.** Rust's analyzable pair is byte-for-byte
  the same under v1 and v2 — learning adds zero overhead where static analysis
  already succeeds.
- **Learning recovers what static analysis loses.** The `run_game_circles ×
  run_game_ia_loop` pair — the case that motivated Phase 6, where release
  inlining destroys the analyzer's ID attribution — goes from *cannot batch*
  (v1 forks = 0) to *batches every round* (v2, 20/20 width-2). The wall-clock
  win here is small (~1.09×) only because the two bodies happen to be
  cost-matched; the load-bearing result is forks 0 → 20.
- **Learning makes batching language-agnostic.** C# compiles to wasm and runs
  on the *same* host and scheduler as Rust, but the Rust-symbol-keyed analyzer
  wildcards all of it — so a C# module gets nothing from v1. Phase 6 capture
  fires on the language-agnostic bindings ABI, so C# reducers promote and batch
  exactly like Rust's release-wildcarded ones: **1.57× with no analyzer support
  at all.** (TypeScript/V8 is the exception — its host doesn't run the batching
  executor, so it is base ≡ v2 until that host work lands.)

### 4.3 Zero cost when idle: by construction, not by tuning

- A module that never runs a heavy reducer keeps a worker pool of size
  **zero**. No threads, no instances, no memory.
- The fast path adds exactly one comparison (is the head's runtime stat above
  threshold?) plus an O(1) wildcard/tier check before today's inline path.
- An unmeasured reducer always runs inline first (stats initialize to zero),
  so the system never forks a reducer it has no timing data for.
- `STDB_REDUCER_POOL_CAP=0` is a hard kill switch restoring exactly today's
  behavior.

This dormant posture is the deployment story: the feature can merge and ship
inert, self-arming only on databases whose traffic demonstrably earns it, with
`batch_stats()` exposing widths, fork counts, and the calibrated threshold for
observation.

### 4.4 Safety is layered and was validated empirically

The corruption-critical property — the static analyzer never *misses* a table
a reducer actually touches — was not assumed. A shadow-mode harness ran static
prediction and dynamic observation side by side on live workloads, acting on
neither, and diffed them per invocation. The acceptance bar was **zero
under-approximations**, and it held across debug and release runs before any
scheduler ever trusted the matrix. (Over-approximation — predicting more than
observed — only costs missed parallelism and is continuously tolerable.)

### 4.5 Engineering risk was spent in the right order

The project was explicitly staged so the cheap, risky validation preceded the
expensive engine surgery, with a real go/no-go gate between them and a
post-launch kill criterion (if real workloads never form profitable batches,
the pool cap stays at its dormant default). Several intermediate designs were
adversarially discarded along the way — sub-range autoinc allocation,
batch-fused commits, a shared cross-module thread pool, deferred ID
assignment — and the decision log records why, so the design does not drift
back into them.

---

## 5. Component Design

This section descends from the publish-time analysis to the runtime engine.

### 5.1 Static access analysis

A standalone crate ([`crates/access-analysis`](../crates/access-analysis))
performs this pass; it has no runtime dependencies and is unit-tested against
compiled `.wasm`.

**Goal.** For each reducer in a published module, compute
`AccessSet { reads: BTreeSet<TableName>, writes: BTreeSet<TableName>, wildcard: bool }`
from the WASM binary alone, where `wildcard = true` means "assume it conflicts
with everything."

**Why it's tractable.** All table access flows through a finite, well-defined
host ABI — 14 table-touching imports (e.g. `datastore_insert_bsatn(table_id,…)`
is a write; `datastore_index_scan_range_bsatn(index_id,…)` is a read), plus
two name resolvers. Index-keyed operations are mapped to their parent table
via the schema. Generated module code resolves table IDs at runtime through a
recognizable fingerprint — a `OnceLock` accessor initialized by
`table_id_from_name("X")` with a constant data-segment string — which survives
release-mode inlining, so the analyzer can recover the table *name* behind
each ID argument by reading the data segment and tracking value provenance.

**The algorithm** (walrus-based, per reducer): locate the reducer's body
function (name section, cross-checked against describer exports), DFS over
direct call edges, constrain `call_indirect` targets by element-segment ∩
type-signature (so formatting/panic vtables don't poison everything), and at
each reachable table operation resolve the ID argument's provenance to a table
name classified read or write.

**The soundness rule is absolute:** any unresolved ID, unknown reachable
`spacetime_*` import, unconstrained indirect call through table-relevant code,
or unlocatable body ⇒ the reducer is wildcard. Doubt always degrades to "no
parallelism," never to a wrong set. The analyzer is Rust-symbol-keyed, so
non-Rust modules (C#, C++, TypeScript) are wholly wildcard at the static
layer — sound, and recovered at runtime by Phase 6 learning ([§5.7](#57-phase-6-learned-sets-and-the-early-cut-trap-built-and-measured)), which is
language-agnostic. (C# and C++ compile to wasm and run on the same host and
scheduler as Rust, so their learned sets batch *now*; TypeScript/V8 waits on
host work — see [§5.7](#57-phase-6-learned-sets-and-the-early-cut-trap-built-and-measured).)

**Measured precision** on the in-repo benchmark suite: 0/64 reducers
wildcarded in Debug builds; 2/64 in Release (both losses caused by aggressive
inlining destroying ID attribution — the named motivating case for Phase 6),
with an 87.6% admissible-pair density in shipping shape.

**Publish integration:** analysis runs unconditionally at module load; failure
logs a warning and falls back to all-wildcard — it can never block a publish.

### 5.2 Conflict matrix

From the access sets, a symmetric N×N bitset is built once per publish:

```
conflict(i, j) ⟺ wildcard_i ∨ wildcard_j
               ∨ writes_i ∩ (reads_j ∪ writes_j) ≠ ∅
               ∨ writes_j ∩ reads_i ≠ ∅
```

Read∩read is deliberately **not** a conflict — shared readers batch freely
(this matters enormously in practice; see the 1.86× pure-reader result). The
diagonal uses the same formula, so a writing reducer conflicts with a second
queued instance of itself — a structural width limiter for write-heavy
workloads that the measurements confirm. Runtime admission is one branch-free
bitset test. Table names resolve lazily to `TableId`s at module-host start;
an unresolvable name degrades that reducer to wildcard.

### 5.3 `BatchTxState`: decomposing the `!Send` transaction

The pivotal source discovery: reducers serialize today because `MutTxId`
holds the committed-state RwLock **write guard** for its whole lifetime, and
its `!Send` is a *synthetic* marker (`PhantomData<Rc<()>>`) enforcing
discipline, not real thread affinity. Meanwhile the read-transaction type
`TxId` already holds a **read guard** and is effectively `Send`. The
workspace builds `parking_lot` with `send_guard`, so the read guard is
`Send` outright.

`BatchTxState` is therefore not a new transaction engine — it is a
recomposition of existing parts:

> **`BatchTxState` ≈ `TxId`'s read guard + `MutTxId`'s `tx_state` delta,
> minus the write guard, minus the synthetic `!Send`.**

- It holds a shared read guard on committed state (N overlays read
  concurrently — already a first-class pattern; the snapshot worker does the
  same), its own `TxState` staging delta, and the existing shared sequence
  allocator handle. It is `Send` by construction (compile-asserted) because
  holding only a read guard makes committed-state mutation impossible.
- Deletes are recorded **by value** as `RowPointer`s — which is the existing
  model, not a workaround: the delete API already takes
  `IntoIterator<Item = RowPointer>`, and pointer stability across the batch
  window is guaranteed by the immutable snapshot plus disjointness.
- Read-your-own-writes works unchanged: the overlay layers `tx_state` over
  committed reads exactly as the serial transaction does.
- `finish()` drops the read guard *before* the drain (necessary — the drain's
  write-lock acquisition would deadlock against sibling read guards) and
  yields a `FinishedBatchTx` carrying the delta to commit. Dropping a
  `FinishedBatchTx` without committing is a clean abort that never advances
  the transaction offset — a property Phase 6's trap relies on.
- The serial drain commits each member with the **existing**
  `CommittedState::merge`, used verbatim, via a commit-and-downgrade path
  mirroring the serial commit (so subscription broadcast evaluation sees the
  identical read-transaction shape).
- DDL is unreachable on a batch transaction *by construction* — the type
  simply has no schema-change methods.

**Host-function generalization.** Reducer-reachable host functions previously
took `&mut MutTxId`. A `ReducerTx` trait now abstracts the
reducer-reachable surface, implemented by both `MutTxId` (pure delegation)
and `BatchTxState`, dispatched through a two-variant enum in the transaction
slot. DDL, SQL, subscriptions, and views remain `MutTxId`-concrete. Hot-lane
cost: one predicted match per host call. Equivalence is tested directly
(Mut-vs-Batch runs compared on final row sets and per-table transaction
data).

### 5.4 The scheduler (`BatchingExecutor`)

The per-module executor loop was forked from the generic single-threaded
executor for wasm hosts only, with the job type gaining a **typed reducer
variant** alongside opaque closures. Only the two client call lanes produce
typed jobs; init, lifecycle, scheduled, view, and subscription work stay as
closures — which makes them *structural barriers*: a batch prefix can never
extend across non-reducer work, preserving total FIFO order across job kinds.

Per dequeued reducer:

1. **Fast path.** If the pool isn't ready, or the reducer's runtime stats
   don't clear the threshold, run inline exactly as today and record the
   runtime. This is the one-comparison invariant.
2. **Fork eligibility.** Requires *all* of: statically non-wildcard
   (O(1) flag check, no datastore work), pool `Ready`, non-lifecycle, and the
   dual runtime gate — **both** the reducer's high-water mark **and** its
   EMA (α = 1/8) must exceed the threshold. High-water alone is permanently
   poisoned by one outlier; EMA alone forgets "can be expensive." Requiring
   both means "routinely and provably expensive."
3. **Lone-head guard.** Scan the pending queue for at least one admissible
   companion; if none exists, run the head inline — forking a lone reducer
   just makes the home thread idle-wait, strictly worse than inline.
4. **Head-fork.** Begin the head's overlay, check it against live
   materialized-view read sets, and dispatch it to the worker. The head is
   the right fork choice because it is the only member with *proven*
   threshold-clearing weight (its stats gated batch construction; later
   members' stats may be cold zeros).
5. **Member pipeline on home.** Walk the pending prefix front-to-back; for
   each candidate: admit (non-lifecycle ∧ non-wildcard ∧ matrix-disjoint from
   every admitted member), begin its overlay, view-check it under its own
   read guard, and execute its body on the home thread — useful work
   during the worker's parallel window. The first inadmissible candidate
   stops the prefix (it stays queued; FIFO preserved). A view overlap merely
   stops the prefix rather than aborting the batch.
6. **Join and drain.** Block on the worker's reply (if the worker died, the
   head re-runs as a fresh home overlay so it still drains in position), then
   commit every member in original FIFO order — per-member commitlog append,
   offset, and broadcast — and record runtimes.

**Threshold calibration is two-stage** to resolve a chicken-and-egg: the true
fork cost can only be measured on a warm worker, but the first spawn decision
needs a threshold. At host init, a cheap in-process thread round-trip proxy
seeds a provisional threshold used *only* to decide when to spawn the worker.
The spawn itself is lazy and deliberately off the hot path — triggered in the
background *after* the commit of the first reducer that trips the proxy
threshold. Once the worker reports ready, the scheduler runs calibration
round-trips through the full real path (dispatch → read-guard acquire →
overlay setup → trivial committed read → finish → reply) and sets
`THRESHOLD = k × median`. Forking enables only after calibration, so if the
true cost is much higher than the proxy, the bar rises before any real fork
fires. Measured on a development machine: fork cost ≈ 62µs, threshold ≈
250µs at the default k = 4.

**View-aware admission.** Materialized views are refreshed inside the writing
reducer's transaction — machinery the batch path deliberately lacks. The
admission predicate therefore requires a member's predicted writes to be
disjoint from the set of tables any *live* view subscription reads
(per-reducer and per-batch, so modules that define views but have no
overlapping live subscriptions batch freely). The same rule structurally
excludes the known host-internal write paths (lifecycle reducers' system-table
writes, pre-created-transaction calls), which were inventoried from source.

### 5.5 The worker (`ReducerWorker`)

A worker is not a bare thread — running a reducer body requires a live wasm
instance whose transaction slot points at the right transaction, and
instances are pinned to their thread. The worker is therefore an
**(OS thread + warm wasm instance)** pair that constructs itself: spawned
lazily, it clones the cheap pre-instantiation handle, builds its own
instance and environment (with a fresh transaction slot — sharing the main
lane's would race) *on its own thread*, signals ready, and enters a blocking
job loop.

- What crosses the channel is `Send` by construction: reducer parameters and
  owned argument bytes plus a `BatchTxState` out; a `FinishedBatchTx` and
  result back. The wasm instance and the write lock never cross.
- After each job the worker self-checks for traps and rebuilds its own
  instance — the same self-heal discipline as the main lane. Verified by an
  integration test that panics a forked reducer and confirms the next fork
  works.
- Shutdown is channel-drop: host exit drops the sender, the loop ends, the
  thread exits. Republish replaces the entire module host, so a warm pool
  dies with its host and the new host starts dormant — module hot-swap
  requires no special pool surgery.
- The pool is **per-module**, not host-wide-shared. A shared pool would be
  net-new cross-tenant infrastructure with a real isolation question on
  multi-tenant hosts (two customers' wasm on one pooled thread); lazy
  per-module pools achieve the desired scaling — threads scale with modules
  *doing heavy work*, not modules *published* — without opening it.

### 5.6 Sequences and autoinc: a non-problem, verified

Autoinc IDs come from per-table, chunked (4096), non-transactional,
gap-tolerant sequence allocators behind one shared mutex. Three facts make
concurrent overlays safe with **no new machinery**: disjoint-table reducers
never share a sequence (sequences are per-table); the draw is a short
per-call critical section under the existing mutex; and read-back semantics
hold because a drawn value was never transactional to begin with — it is
real the instant it is drawn. Lock-order analysis confirmed no deadlock cycle,
and the feared "overlay blocks behind a serial reducer's sequence lock" case
is vacuous: the committed-state RwLock already makes serial transactions and
live overlays mutually exclusive. The one observable consequence — IDs
interleave across concurrent reducers — is already true across separate
transactions today and is documented behavior. Rejected alternatives
(sub-range allocation, wildcarding autoinc tables, deferring assignment to
commit) are recorded in the decision log; each was either redundant with the
existing allocator or broke the documented read-back contract.

### 5.7 Phase 6: learned sets and the early-cut trap (built and measured)

Static analysis has two systematic blind spots, both quantified during
validation: release-mode inlining destroys ID attribution for some heavy
reducers (the motivating exhibit: `run_game_circles × run_game_ia_loop` — a
genuinely disjoint heavy pair, admissible under Debug analysis, wildcarded in
Release), and non-Rust modules are wholly wildcard at the static layer. Phase 6
lifts both with **runtime learning guarded by a trap**, plus a key-level
refinement of the view rule. It is built and was the verdict-mandated gate for
the upstream PR; the cross-language results in [§4.2](#42-cross-language-results-static-path-vs-learned-path) are its payoff.

**Tier model.** Each reducer carries a per-boot tier:

```
Static     — analyzer set exists; admission via the matrix (today's path, untouched)
Unknown    — statically wildcard; runs inline with capture on, learning
Learned    — promoted; admissible via direct set intersection, trap-guarded
Parked     — two trap strikes; plain wildcard for the rest of the boot
```

Dynamic capture reuses dormant hooks already in the datastore: every table
operation inserts its resolved `TableId` into a per-invocation observed set
(`ObservedAccess`) — keyed on IDs, so it is immune to both release inlining and
name-normalization hazards, and **language-agnostic by construction** (the
capture lives below the wasm boundary). Because C# and C++ already run on the
same wasmtime host and `BatchingExecutor` as Rust, their reducers — wildcard at
the static layer — promote and batch through exactly this path; the C# 1.57×
result in [§4.2](#42-cross-language-results-static-path-vs-learned-path) is learning alone, with no analyzer support. (TypeScript/V8 is
the lone exception: its host runs a different executor that never reaches the
batch lane, so it stays at base until that host adopts a fork scheduler.)
Learning state lives in the scheduler loop's own thread-local state — zero
locks, and republish invalidates it for free because republish replaces the
whole module host.

**Promotion** requires union-stability, not just a run count: ≥ 8 committed
runs *and* the last 4 added no new table to the running union. Soundness does
not depend on this gate — the trap catches any escape — so the gate exists
purely to keep trap frequency low and to filter data-dependent access
patterns that would churn. **Demotion** on any escape re-seeds the
accumulator with the union plus the escape; a second strike parks the reducer
for the boot.

**The early-cut trap.** The structural fact that makes the trap cheap: batch
members execute *sequentially* on the home thread, so each member's observed
set can be validated immediately after its body — before the next member is
even admitted — and the head's observed set arrives at join, before anything
drains. Nothing commits before its validation, and downstream victims are
structurally impossible. A serial-order argument (recorded in the decision
log) shows an escape by member *k* can never invalidate the head or members
before *k* — so whole-batch aborts are strictly dominated:

- **Harmless escape** (observed sets disjoint from all earlier members'
  effective writes): the member's delta is provably serial-correct — it
  commits; the batch just stops admitting. The reducer is demoted.
- **Conflicting escape:** the member's overlay is dropped uncommitted (a
  clean abort) and its payload — reply channel intact — requeues at the
  *front* of the pending queue, which is exactly its serial FIFO slot; it
  re-executes inline, sees all earlier writes, and answers its caller from
  the committed run. Aborted autoinc draws leak as gaps, which sequences
  already document.
- **Head escape:** the head always commits (position 0 — nothing earlier
  exists to invalidate it); members are walked in order against the head's
  observed writes, and the first conflict cuts that member and the whole
  suffix to requeue, preserving the FIFO-commit invariant for the clean
  prefix.

**View lift (members only).** The admission view check is table-level; the
engine's actual refresh predicate is key-level. Phase 6 admits members whose
written tables overlap view read sets *only through index-key entries*
speculatively, with a post-body key-level check (a read-guard port of the
engine's own predicate, parity-tested in both branches because
under-detection is the corruption case) — a miss aborts and requeues through
the same early-cut path, with no strike, since view misses reflect
subscription data, not learned-set wrongness. Full-scan overlaps remain
excluded pre-execution (refresh is certain; speculation always loses), and
the head stays fully conservative. Integrity rests entirely on the post-body
check; the pre-filter is pure economics.

**Cost discipline holds:** the fast path's wildcard check becomes a tier
discriminant check (same O(1) class); all-static batches skip trap validation
entirely; capture is enabled only on tiers that today cannot batch at all;
and a trap-aborted execution is not billed to the user — the speculation was
the host's bet, so the host eats it. Energy accounting was the one
build-time unknown here, and it resolved cleanly: batch bodies simply skip the
energy record, and the serial drain bills only committed outcomes — no
un-billing of aborted runs was ever required, because not-recording is the
whole mechanism.

**As built.** The tier overlay (`AccessTier { Static, Unknown, Learned,
Parked }` plus a strike counter) and the early-cut trap live in the scheduler
loop; the key-level view check is the datastore free function
`view_refresh_required` (a read-guard port of the engine's own
`views_for_refresh`, both branches), with `view_overlap_kind` classifying a
write set as `None`/`KeyOnly`/`FullScan` for the admission pre-filter. One
soundness refinement surfaced during implementation and is worth recording: a
`Static` reducer whose analyzer set is fine but whose table *names* failed to
resolve has unknown true access, so it is treated as universal-conflict rather
than intersected against its (empty) resolved set — guarded on both sides of
the conflict test. The independent review verdict was **adequate, zero
critical or major findings**, with every flagged corruption surface traced and
closed. End-to-end evidence the trap actually fires: an integration test
watched a learned reducer escape its learned set after a flag flip and observed
`head_traps = 1, demotions = 1` with the requeued run's committed effect
exactly equal to serial execution. The corruption-critical view path is gated
by the datastore parity must-trap test (which fails loudly on the
under-detection direction) plus trap unit tests; the full
subscribe→speculate→refresh round-trip is the one piece left unproven in-tree,
deferred only for lack of an ergonomic subscription test helper.

---

## 6. Validation Methodology and Results

The project gated itself repeatedly, in increasing order of cost.

**Gate 1 — shadow mode (before any engine surgery).** Static analysis and
dynamic capture ran together on live workloads, acting on nothing, diffing
predicted vs observed per invocation and simulating what batches would have
formed. Results: **zero under-approximations** across debug and release runs
(the corruption-critical gate); width simulation showed pure readers batching
arbitrarily wide (width 67 observed) while alternating writers cap at 2 (a
writer self-conflicts with its own next invocation); and the measured thread
handoff proxy (~53µs) was 10–50× the folk estimate, which honestly *narrowed*
the profitable regime and reshaped the threshold design before any code
depended on the optimistic number.

**Gate 2 — go/no-go (human decision, 2026-06-10).** GO, on the basis that:
soundness was demonstrated, not assumed; head-of-line relief is robust even
with expensive handoff (only ≥ k×handoff reducers ever fork; the benefit
lands on the cheap traffic behind them); and the design self-protects on
unfavorable hardware (self-calibrating threshold; worst case is the feature
rarely firing, at one comparison of cost).

**Gate 3 — post-build kill criterion (verdict 2026-06-12).** After the
engine landed, the question became whether profitable batches form on
*realistic* code. A static audit of the in-repo benchmark modules found real
batchable structure in shipping shape (87.6% admissible-pair density), and a
dynamic A/B harness on the real `benchmarks` module produced the 1.39–1.86×
results of [§4.1](#41-measured-performance), with batch members clearing the calibrated threshold by two
orders of magnitude. The criterion was accepted at the in-repo tier with an
honesty caveat recorded: in-repo workloads prove the machinery and the cost
conditions, not deployment-side arrival concurrency — whether real client
traffic presents concurrent admissible calls remains a production observation
via `batch_stats()`, and the dormant-by-default posture means a negative
answer costs nothing. The same verdict mandated that Phase 6 ship *before* the
upstream PR, so the first public version lifts the two big admission
exclusions rather than carrying them as known gaps.

**Gate 4 — Phase 6 trap evidence (post-build, 2026-06-12/13).** The learned
path's correctness was demonstrated, not assumed: an end-to-end test forced a
promoted reducer to escape its learned set and confirmed the trap fired
(`head_traps = 1`, `demotions = 1`) with database state identical to serial
execution; the verdict's named lost pair `run_game_circles × run_game_ia_loop`
went from zero forks (static) to 20/20 width-2 (learned); and the C# port
demonstrated the language-agnostic claim end-to-end at 1.57× with no analyzer
support. A datastore-level parity must-trap test guards the corruption-critical
view direction, and the independent review found zero critical or major issues.

The validation across all phases also caught three real analyzer bugs before
they could matter (a name-recovery false positive that silently wildcarded
whole modules, and two identifier-normalization mismatches that wildcarded any
digit-bearing name) — each now has a regression fixture, and each reinforced
the operative lesson that *machine-readable fork counts*, not graceful test
skips, are the regression signal for a feature whose failure mode is silently
doing nothing.

---

## 7. Invariants

The implementation maintains, and is tested against, the following:

1. **No under-approximation is ever trusted.** Static analysis
   over-approximates to wildcard; in Phase 6, nothing commits before its trap
   validation.
2. **Committed-state mutation stays single-threaded** — only in the serial
   drain. Reads are the only concurrent access.
3. **The commit drain is always FIFO order**, independent of execution
   thread; a requeued payload re-enters at the front of pending — its exact
   serial slot.
4. **Per-reducer atomicity and broadcast are preserved** — the batch is a
   scheduling unit, never a transactional unit.
5. **The all-cheap workload pays ~one comparison** over the unmodified lane.
6. **Reducer and procedure runtimes stay separate**; workers are plain OS
   threads.
7. **Capture is never enabled for `Static` or `Parked` reducers** (Phase 6).

## 8. Configuration and Observability

| Surface | Default | Meaning |
|---|---|---|
| `STDB_REDUCER_POOL_CAP` | `1` | Max workers per module; `0` = hard off switch (today's behavior exactly) |
| `STDB_REDUCER_FORK_K` | `4` | Profitability multiplier: fork only if runtime > k × calibrated fork cost |
| `ModuleHost::batch_stats()` | — | Snapshot: batches, forks, width histogram, calibrated fork cost, pool state; Phase 6 adds promotion/demotion/trap counters |

## 9. Limitations and Future Work

- **Width is capped at 2** (home + one worker) in v1. Three or more
  simultaneously-runnable heavy disjoint reducers serialize the overflow.
  The cap is a config value; raising the default wants evidence that the
  3+-wide regime exists in production.
- **Conflict granularity is the table.** High-contention single tables
  serialize; row-level analysis is not statically recoverable and is
  explicitly out of scope.
- **A writing reducer self-conflicts**, capping same-reducer write bursts at
  width 1 per batch. Pure readers are unaffected (and benefit most).
- **Scheduled reducers don't batch** — they arrive with pre-created
  transactions on a separate path; lifting them is transaction-plumbing work
  orthogonal to the trap machinery.
- **C# and C++ modules batch only via learned sets, never static analysis**
  (the analyzer is Rust-symbol-keyed). They get nothing from v1 and must warm
  up ≥ 8 runs per reducer before promotion, but then batch like Rust —
  measured 1.57× on C#. **TypeScript/V8 does not batch at all** yet: its host
  runs a separate executor that never reaches the batch lane, so enabling it
  is net-new host work, out of scope here.
- **Runtime-adaptive k**, input-size-bucketed timing stats, idle-worker
  reaping, and a shared host-wide pool (which carries a deliberate
  tenant-isolation decision) are all recorded as deferred, with their
  reopening conditions, in the decision log.
- **Residual open question:** deployment-side arrival concurrency — observed,
  not assumed, via `batch_stats()` after merge.

## 10. Summary

This design parallelizes SpacetimeDB reducers by *avoiding* every classically
hard problem in the space: no concurrent writers (serial drain), no
speculation in the core path (sound static admission), no scheduling tax on
the fast path (threshold-gated batch construction), no idle cost (lazy
per-module workers), and no semantic drift (FIFO commits, per-reducer
atomicity, unchanged broadcast). Each layer degrades safely — doubt becomes
wildcard, wildcard becomes serial (or, with Phase 6, *learns* its way back to
parallel under a trap), serial is exactly today's engine — and each claim
above is backed by a measurement or a source-verified fact recorded in the
companion decision log. The result is 1.4–1.9× on real heavy Rust workloads
and 1.57× on C# with no static support at all, at zero measured cost to
everyone else, shipped dormant behind a one-variable kill switch. With all six
phases built and reviewed, the remaining work is the upstream PR and
production observation of arrival concurrency — not further engine surgery.
