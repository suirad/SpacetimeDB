# Concurrent Reducer Execution — Design Decision Log

> Companion to `IMPLEMENTATION_GUIDE.md`. This file records **why** each choice
> was made and which alternatives were rejected, so the implementation does not
> re-litigate settled questions. When a decision changes, update the relevant
> entry here *and* the corresponding section of the implementation guide.

**Status:** design in progress. Worker-pool handoff mechanics and threshold
calibration are not yet finalized (see "Open Questions").

**Target codebase:** `clockworklabs/SpacetimeDB`, master as of ~June 2026
(post-PR #5095).

---

## 0. Problem statement

SpacetimeDB executes reducers on a single serial lane. As of PR #5095 (merged
2026-05-28), the reducer lane is a **synchronous wasm runtime on a single
dedicated OS thread**, deliberately stripped of the async/Tokio overhead that
procedures still use. The goal of this project is to let *some* reducers run
concurrently **when it is provably safe and profitable**, without regressing the
fast synchronous lane that #5095 just optimized, and without weakening
serializability or per-reducer atomicity.

The core thesis: **isolation comes from admission control plus a serial commit
drain, not from general concurrency control.** We statically prove which
reducers cannot interfere, run only those in parallel, and serialize their
commits.

---

## 1. Determining table access (static analysis)

**Decision.** At module publish time, statically analyze the WASM binary to
compute, per reducer, an `AccessSet { reads, writes, wildcard }` over table IDs.
Hook this where `__describe_module__` / `RawModuleDef` extraction already
happens.

**Why.** Table access goes through a well-defined host ABI
(`datastore_insert_bsatn(table_id, ...)`, `datastore_index_scan_*(index_id,
...)`, etc. — confirmed in `crates/bindings-sys/src/lib.rs`). That gives a
finite set of call sites to analyze. The schema (table/index defs) is already
extracted at publish, so the analysis has everything it needs and adds no
runtime cost.

**Key wrinkle — IndexId vs TableId.** Scans, updates, and index-based deletes
are keyed on `IndexId`, **not** `TableId`
(`datastore_index_scan_point_bsatn(index_id, ...)`,
`datastore_update_bsatn(table_id, index_id, ...)`,
`datastore_delete_by_index_scan_point_bsatn(index_id, ...)`). Only
`datastore_insert_bsatn` and `datastore_delete_all_by_eq_bsatn` take a bare
`table_id`. So the analyzer must resolve `IndexId -> TableId` via the schema
(`RawModuleDef` lists each index's parent table).

**Soundness rule.** Over-approximate, never under-approximate. Any unresolved
table/index ID, any `call_indirect` whose target set can't be constrained, any
computed ID → mark the reducer **wildcard** (conflicts with everything). An
under-approximation (missing a table the reducer actually touches) is a
**data-corruption bug** if the scheduler trusts it; an over-approximation only
costs a missed parallelism opportunity.

**Tooling.** `walrus` for the WASM pass (stable, widely used). `waffle` (SSA CFG)
would give cleaner dataflow for branch joins and is a possible later swap if
false-wildcard rate is too high, but start with walrus.

**Rejected:**
- *Pure dynamic capture instead of static* — viable and easier (intercept at the
  host functions), but can't prove safety ahead of time; only observes past
  behavior. Kept as a *refinement* layer (see §6), not the basis.
- *Row-level access analysis* — not statically recoverable; table granularity is
  the 90% solution. High-contention single tables will serialize, accepted.

---

## 2. Conflict model

**Decision.** Two reducers conflict iff one's write-set intersects the other's
read-or-write-set, or either is wildcard. Precompute a symmetric N×N conflict
matrix at publish time as a bitset (`bits[i*words + j/64] & (1<<(j%64))`),
O(1) lookup at runtime.

**Why.** Standard read/write conflict rule. Bitset AND is branch-free and
cache-friendly; the matrix is cheap to build (N² small ANDs, N = reducer count).

---

## 3. Scheduling — strict-prefix batching

**Decision.** From the FIFO queue, build a batch by popping the head and
admitting each *following* reducer whose AccessSet is disjoint from everyone
already in the batch, **stopping at the first conflict or wildcard**. The batch
is always a contiguous prefix of the queue.

**Why strict prefix (not maximal independent set).** Pure FIFO order is
preserved exactly — no reordering, no starvation, no causal-ordering surprises
for clients. A skipping/reordering selector yields bigger batches but reorders
observable effects and needs a bounded look-ahead to avoid unbounded reordering.
Start strict; revisit only if batch widths prove too small in practice.

**Wildcard reducers run alone**, sequentially, exactly as today. This is the
correctness fallback.

**Critical refinement (decided 2026, see §5):** prefix-batching is *only* an
enabling mechanism for the fork. In a pure single-threaded world it is pure
overhead (you compute a grouping you then ignore). Therefore **gate batch
construction on the threshold**: if the head reducer is below the fork
threshold, do not build a batch at all — run it inline. Only assemble a prefix
once you have a fork-worthy head.

**Rejected:**
- *Batch as a transactional unit (fused commit)* — breaks per-reducer atomicity
  and per-reducer broadcast; complicates partial failure. The batch is a
  *scheduling* unit only; commits stay independent and per-reducer.
- *Commit coalescing for disjoint prefixes (single-threaded win)* — real but a
  *different feature* that changes transaction boundaries. Filed as possible
  later work; reuses the AccessSet analysis, not the batch selector.

---

## 4. Execution model — three bands

**Decision.** A batch executes in three phases:
1. **Shared read snapshot** — one read guard / `Arc<CommittedSnapshot>` taken at
   batch start; all overlays read from it. Immutable for the batch's life.
2. **Parallel overlays** — each reducer runs with its own `TxState`-style
   staging delta (`BatchTxState`) over the shared snapshot. Disjoint table sets
   mean no overlay reads/writes another's data.
3. **Serial commit drain** — drain in strict FIFO order, one at a time under the
   exclusive write lock: merge delta → committed state, append commitlog, bump
   tx offset, broadcast to subscribers.

**Why this is sound and avoids the hard problem.** Mutation of committed state
happens **only** in the serial drain, single-threaded. So we never have to make
`CommittedState` *mutation* thread-safe — only its *reads* concurrent (which is
just a shared immutable borrow). The hardest engine problem (concurrent writers)
is *avoided by construction*, not solved.

**Disjointness makes execution order irrelevant** for user-table effects:
committing X-then-Y equals Y-then-X when their tables don't overlap. Only the
commit drain's order matters, and that's kept FIFO.

---

## 5. BatchTxState decomposition (confronting `!Send`)

**Context.** `MutTxId` is marked `!Send` (PR #4039) "to avoid incorrect
cross-thread usage." It fuses three things: a lock guard, a borrow into
committed state, and a mutable delta. The fusion is what makes it `!Send`.

**Decision.** Factor into three types with honest markers:
- `Arc<CommittedSnapshot>` — `Send + Sync`, immutable shared read base.
- `BatchTxState` — `Send`, owns its delta (inserted rows owned outright; deletes
  recorded by **value** as stable `RowPointer`/key, not by borrow), holds no
  lock and no borrow into committed state. This is the thing that moves to a
  worker thread.
- `DrainGuard`/commit handle — `!Send`, holds the lock, lives only on the drain
  thread.

**Why deletes are the hard part.** A delete references a committed row. If
recorded as a borrow/pointer-into-committed-state, it re-breaks `Send`. Record
it **by value** (RowPointer or primary key) resolved against the immutable
snapshot at drain time. Sound because the snapshot is immutable for the batch's
life *and* disjointness guarantees no other overlay touches that table, so the
pointer can't be invalidated before the drain. **Verify:** RowPointer stability
across snapshot lifetime, and that the delete path accepts a by-value pointer
captured earlier.

**Integration placement — REVISED after PR #5095.** Originally planned to build
on the procedure-side per-call-context machinery (`HashMap<CallId, CallContext>`
/ `current_call()` from issue #4697). **PR #5095 split reducers and procedures
onto separate runtimes**: procedures keep the async Tokio runtime; reducers now
run on a *synchronous single-OS-thread* runtime. So:
- Do **NOT** build on procedure-side `CallContext`/`current_call()`. That would
  re-couple the two lanes #5095 just separated.
- Treat the new synchronous reducer runtime as the untouched fast path. Attach
  the batch scheduler + a plain OS-worker-pool *beside* it. Worker threads are
  plain OS threads, not tasks on the procedure async runtime.
- A synchronous lane is *easier* to extend than a Tokio one (no async state
  machine, no `.await`/`!Send` friction). `BatchTxState: Send` moves to a worker;
  orthogonal to the procedure runtime.

**Why reducers are an easier OCC/retry case than procedures.** Procedures
interleave external I/O (HTTP) and their `with_tx` body "may be invoked multiple
times" (OCC, re-runnable — confirmed in docs). Reducers are wholly transactional
with no external side effects, so speculative-execute-and-retry is *more* sound
for reducers than for the procedure case the OCC machinery was built for.

---

## 6. Tiered confidence + dynamic capture (refinement, not v1 core)

**Decision.** Per reducer, a confidence tier:
- **Statically proven** sets → trust freely.
- **Learned** sets (from dynamic capture at the host functions) → trust
  speculatively, guarded by a runtime trap: if a reducer touches a table outside
  its predicted set, abort-and-retry serially (reuse STDB's existing
  anomaly/replay machinery).
- **Wildcard** → run alone.

A reducer migrates up on clean observations, down on any violation.

**Shadow mode first.** Before the scheduler acts on any matrix: run static
analysis + dynamic capture together, act on neither, and **diff predicted vs.
observed** access sets on real workloads. Goal: prove **zero
under-approximations** (the corruption case) and measure the false-conflict
(over-approximation) rate. Cheap to build, de-risks the dangerous part.

**Why dynamic capture is easy here.** Every table op already calls into
host-side Rust (`InstanceEnv`) with the resolved ID in hand. Recording observed
read/write sets is an insert into a per-invocation set — near-free.

---

## 7. Autoinc / sequences — VERIFIED against source

**Question:** how do concurrent reducers allocate autoinc IDs without collision,
given read-back semantics (`insert(...)` returns the row with the assigned ID,
usable mid-reducer)?

**Decision.** Make the existing chunk-allocator's draw/refill **atomic**. Nothing
else. No sub-ranges, no deferral, no wildcarding autoinc tables.

**Why (from docs/source):**
- The sequence allocator is **non-transactional**: chunked (4096), eager,
  gap-tolerant, incremented even on rollback, never reclaimed. (STDB appendix.)
- Each autoinc column gets its **own per-table sequence**. Two disjoint-table
  reducers therefore **never share a sequence** — collision is unreachable under
  prefix-disjoint batching.
- Read-back works unchanged: the value is real the instant it's drawn (it was
  never transactional), so an atomic draw gives each concurrent overlay a unique
  value immediately.

**Rejected:**
- *Sub-ranges per overlay (my earlier idea)* — reinvents the chunk allocator
  that already exists; unnecessary once the draw is atomic.
- *Wildcard writes to autoinc tables (alternative considered)* — would wildcard
  *most* write reducers (autoinc is the idiomatic PK pattern), collapsing the
  batch. And it punishes the disjoint case to solve a non-problem: same-table
  writers already conflict on the table; different-table writers use different
  sequences.
- *Deferring ID assignment to the drain* — breaks the documented read-back
  contract (`inserted.id` used mid-reducer). Ruled out by source.

**Consequence to document for users:** autoinc IDs will interleave across
concurrently-executing reducers (X:1, Y:2, X:3). Already true across separate
transactions today; gaps/non-contiguity are documented behavior. No contract
broken; worth a doc note.

**Read-your-own-writes:** already fully solved by the `TxState`/`CommittedState`
overlay for a single tx (confirmed by existing delete-reinsert test in
`relational_db.rs`). Transfers per-overlay unchanged.

---

## 8. Performance gating — the viability crux

**The concern.** Reducers are tiny (often 1–50µs). Thread handoff is ~1–5µs.
Parallelizing a tiny reducer is net-negative — the dispatch *is* the workload.
This is almost certainly why concurrency hasn't shipped, and #5095 (making the
serial lane *faster*) is evidence the team's current thesis is "optimize the
serial path," not "parallelize it."

**Decision — the design only wins if it:**
1. **Degrades to the bare synchronous lane** (#5095's lane, untouched) for the
   width-1 / all-cheap case, with sub-100ns residual cost (one comparison).
2. **Gates forking on measured profitability**, not just disjointness.

So the three scheduler changes (§9) are **one indivisible feature**: batching
exists to feed the fork; the threshold decides if the fork fires; if nothing is
fork-worthy, don't batch at all.

**Where the win actually is.** Not the median tiny reducer. The win is:
(a) reducers with real per-call compute (≥ ~50–100µs: table scans, pathfinding,
physics, matchmaking, bulk imports, scheduled jobs), and (b) **head-of-line
blocking relief** — letting cheap reducers proceed off-thread instead of waiting
behind one expensive disjoint reducer under strict FIFO. (b) helps latency even
when total CPU work rises slightly.

**User's bet (accepted as design premise):** most STDB modules are minimally
optimized, so the ≥50–100µs regime occurs often and grows as servers get more
complex. The shadow-mode harness should validate this (batch-width distribution
+ runtime-vs-handoff ratio) before the BatchTxState surgery, as the go/no-go.

---

## 9. The three scheduler changes — EVALUATED

### Change 1 — warm synchronous default
Post-#5095 this is "don't break the sync lane." The inline path *is* the
unmodified synchronous runtime. The only addition is the decision point in
front. High-water time init-to-0 means an unmeasured reducer always runs inline
first → you never fork a reducer you have no timing data for (closes a
profitability-correctness gap). First expensive invocation always runs inline
(measured going in at 0); forked only on later calls. Accepted: all-distinct
run-once expensive reducers get no parallelism — pathological, fine.

### Change 2 — learned execution time
Mechanism (init 0, store if `time > stored`) is sound, with three refinements:
- **Placement:** keep timing in a *parallel array* `runtime_stats[r]`, NOT inside
  the immutable `AccessSet`. Different lifetimes (static-immutable vs
  hot-mutable); mixing invites cache/sync awkwardness.
- **Robustness:** high-water alone is poisoned permanently by one outlier. Store
  a **pair: high-water + EMA**, and fork only when *both* exceed threshold.
  High-water catches "can be expensive"; EMA confirms "routinely expensive."
- **Limit (document, don't fix yet):** per-reducer timing can't see
  input-dependence (insert 1 row vs 10k). Cheap invocations of
  generally-expensive reducers get forked needlessly — bounded waste. Input-size
  bucketing is a possible later refinement; don't build it in v1.

### Change 3 — threshold-gated prefix dispatch
Revised control flow (ordering matters):
1. Pop head. **Check head's runtime stat vs threshold BEFORE building a batch.**
   If below threshold → run inline on sync lane, return. (Cheap path pays one
   comparison; never touches the matrix. This is what keeps #5095's lane fast.)
2. If head is above threshold → build the disjoint prefix (bitset ANDs, stop at
   conflict/wildcard).
3. Fork above-threshold members onto worker threads; run below-threshold members
   **inline on the home thread while forks are in flight** (home thread does
   useful cheap work during the parallel window, never idle-waits).
4. Join forks (barrier — all `BatchTxState`s exist).
5. Serial commit drain in **original FIFO order** regardless of which thread ran
   which body (disjointness makes execution interleaving invisible; commit order
   is the only observable order and stays FIFO).

**Threshold must be self-calibrating:** `fork if runtime > k × measured_handoff_cost`,
with k ≈ 3–5×. Measure `handoff_cost` once at startup (time a no-op worker
round-trip) rather than hardcoding ns. Self-calibrates across hardware (cheap
threads → fork eagerly; expensive → stay inline longer). A fixed ns threshold
regresses on slow-handoff hardware or misses parallelism on fast hardware.

**Net:** in the all-tiny workload #5095 targets, the scheduler is one comparison
per reducer over the existing lane. Parallelism switches on only when timing
proves a reducer heavy enough to pay for the thread.

---

## 10. Worker pool + handoff mechanics — DECIDED

**The constraint that drives everything here:** running a reducer body requires a
live wasm **instance** with its transaction slot pointing at the right tx; the
instance is pinned to its worker thread.

> **CORRECTION (TASK A, source wins):** the slot is **not** a thread-local. Source
> (`instance_env.rs`) is `pub struct InstanceEnv { pub tx: TxSlot, .. }` with
> `pub struct TxSlot { inner: Arc<Mutex<Option<MutTxId>>> }` — a plain field on
> `InstanceEnv`, reached through `self.tx`, not a `thread_local!`. The conclusion
> below (instances pinned to workers; only `BatchTxState` crosses the boundary) is
> unaffected. New consequence: `MutTxId: !Send` ⇒ `Mutex<Option<MutTxId>>` and thus
> `InstanceEnv` are `!Send` *today*; Phase 5 must run a worker's `InstanceEnv` with
> a `BatchTxState` (Send) in the slot instead of a `MutTxId`. Running a reducer body therefore requires (a) a
wasm **instance** to run it in, and (b) that instance's thread-local `TxSlot`
pointing at the right tx. There is one wasm instance per worker (#4663 made the
main lane a single instance; #5095 made its executor synchronous).

**Consequence:** "fork a reducer to a worker thread" actually means "run a wasm
instance on that worker with its thread-local `TxSlot` → this reducer's
`BatchTxState`." So the pool is **not** a pool of bare threads — it is a pool of
**(OS thread + warm wasm instance)** pairs.

**Decisions:**
- **Pool = N warm, pre-instantiated instance-workers.** Small N, tied to core
  count. Each is an OS thread owning a ready instance of the current module,
  blocking for jobs. **Warm, not spawn-on-demand:** instance instantiation
  (linear memory alloc, etc.) is exactly the overhead #5095 removed from the
  critical path; paying it per-fork would reintroduce it on the fork path. Pay
  once at module-load/pool-grow, reuse.
- **Topology: 1 home worker (the #5095 sync lane, untouched) + N pool workers.**
  Inline/wildcard reducers run on the home worker exactly as today. Pool workers
  *only* execute reducer bodies into their `BatchTxState`; they **never commit**.
  The home worker is the drain thread and the only place the `!Send` `DrainGuard`
  / write lock lives.
- **What crosses the thread boundary (all `Send`):** to worker —
  `Arc<CommittedSnapshot>`, reducer id + owned arg bytes, an empty
  `BatchTxState`. Back — the populated `BatchTxState`. **Never crosses:** the
  wasm instance (pinned to its worker thread; instances are not `Send` — we never
  move them), the `DrainGuard` (home thread only). Instance pinning *helps*: we
  sidestep instance-`Send`-ness entirely because instances never leave their
  thread.
- **Handoff:** per-worker job channel or shared work-queue + counting join
  barrier. **[VERIFY] resolved (TASK A): do NOT reuse `crates/core/src/util/jobs.rs`.**
  It provides `JobCores` — a pool of single-threaded **Tokio** executors (async,
  optional core-pinning), i.e. exactly the async-runtime infra §5/§10 require the
  reducer workers to stay *off*. Use plain OS threads + a channel/barrier instead.
- **Join + drain:** home dispatches forks → runs its inline reducers → blocks on
  counting join until all forks return their `BatchTxState` → runs serial drain
  over forked + inline in FIFO order. No work-stealing in v1 (batches small and
  bounded); revisit only if batch sizes grow.

**New lifecycle surface — module hot-swap.** Warm pool instances go stale on
republish. The pool's instances must be replaced atomically with the main lane's
on module reload (same hook as `AutoReplacingModuleInstance`). Net-new; tracked
in the guide.

**Rejected:**
- *Pool of bare OS threads receiving `BatchTxState`* — can't run reducer code
  without an instance + thread-local tx. Doesn't work given the ABI.
- *Spawn instance per fork* — reintroduces instantiation cost on the fork path,
  defeating the profitability gate.

## 11. Threshold calibration — REFINED

The fork cost is not just thread handoff — a pool worker runs in its **own wasm
instance / linear memory**, so it reads committed state cold in its own
cache/NUMA node. True fork cost = handoff + cold-instance effects.

**Decision:** measure the threshold base as a **no-op reducer round-trip on a
pool worker** (dispatch → trivial reducer → return populated empty
`BatchTxState`), NOT a bare thread ping. Then `THRESHOLD = k × measured_fork_cost`,
k ≈ 3–5. Measure once at startup / pool init; store globally. Self-calibrates
across hardware. Whether k adapts at runtime: **still open**, default to fixed k
for v1.

## 12. Pool sharing granularity — DECIDED (per-module, not shared)

**Question:** is the fork worker pool shared across modules, or
thread-instance-module (per database)?

**Source facts (DeepWiki arch overview, `module_host.rs`, `host_controller.rs`):**
- `ModuleHost` is a **per-database actor**; each owns its `WasmtimeModuleHost` /
  `V8ModuleHost` + immutable `ModuleInfo`. The #5095 home lane lives inside the
  per-database `WasmtimeModuleHost`.
- `HostController` is the only host-wide owner (cheap-clone, all `Arc`/`Copy`),
  manages all per-database `ModuleHost`s and replicas.
- **There is NO existing shared cross-module compute/thread pool.** All compute
  is currently per-database. A shared pool would be *new* host-wide
  infrastructure on `HostController`, not an extension of something.

**Decision: Option A — per-module pool (thread-instance-module), scoped inside
the per-database `WasmtimeModuleHost`, with LAZY sizing.** A wasm instance is an
instantiation of one specific module, so a warm instance-worker is inherently
module-bound; pinning is therefore at the (thread, instance, module) granularity.
- **Lazy sizing:** zero pool workers until a module actually forks. Grow to a
  small cap on demand; reap idle workers. A module that never forks (the common
  all-cheap workload per the profitability gate) has a pool of size 0 and costs
  nothing.
- This gives the scaling property we wanted — thread count scales with *number of
  modules doing heavy concurrent work*, not number of modules published — WITHOUT
  a shared pool.

**Why NOT a shared cross-module pool (rejected for v1):**
- No existing shared pool to extend; it's net-new host-wide infrastructure
  cutting across the per-database actor isolation the whole architecture rests on.
- **Tenant isolation:** on multi-tenant hosts (maincloud), running two customers'
  wasm on the same pooled thread is a materially weaker isolation posture than
  the current strict per-database separation — fault isolation (panic on a shared
  worker), fairness, and security all become open questions. Could be a
  non-starter for the maincloud security model.
- The many-idle-modules concern that motivated sharing is solved better by lazy
  per-module pools (idle modules → 0 workers) than by sharing.

**Future option (do not reopen casually):** a shared host-wide pool on
`HostController` is a possible later optimization IF profiling shows per-module
pools waste threads on a single-tenant host running many busy modules. It carries
a tenant-isolation decision that must be made deliberately, not defaulted into.

**The home lane stays per-module-pinned regardless** (it owns the drain + `!Send`
write lock + FIFO identity; see §10). Only the *fork capacity* was ever a
sharing question, and that's now per-module too.

**[VERIFY]** warm instance memory cost for a typical module (sizes the idle-reap
TTL / cap). Replica model (`HostController` tracks replica ids, #4160) coexists
with per-module pools — confirm no interaction.

## 13. v1 pool cap — DECIDED (max 1 worker, width-2 with home)

**Decision:** for v1, cap the per-module fork pool at **1 lazily-spawned worker**,
making a configurable max (default 1) — not a hard-coded constant — so Phase 2
measurement can raise it without a redesign.

**Crucial framing: home thread + 1 worker = effective width-2, BOTH lanes
execute.** The home thread is an execution resource, not just a dispatcher. So:
- `{expensive-A, cheap-B, cheap-C}` → A on worker ∥ B,C inline on home. Full
  overlap.
- `{expensive-A, expensive-B}` → fork A to worker, **run B inline on home** →
  two expensive reducers in parallel, width-2 fully utilized. Home runs the
  second-heaviest itself rather than idling.
- `{A, B, C}` all heavy → A∥B (width-2), C runs after on whichever lane frees.
  This is the only real ceiling of cap=1: 2-wide out of a possible 3+. Capturing
  2-wide (vs sequential) is most of the win; capping the rest is defensible v1
  scoping.

**Design rule that follows:** ~~home forks the *single heaviest* batch member to
the worker~~ **SUPERSEDED (user decision, 2026-06-11, post-build): the HEAD forks.**
The head is the only member with *proven* threshold-clearing weight (it gated
batch construction; other members' stats may be cold zeros), and head-fork lets
the dispatch happen after one admission scan instead of after full batch
construction. Named cost, accepted: when a later member is much heavier than the
head, the worker idles after the head finishes while home grinds the big member
(heaviest-fork minimized that makespan). Guard kept from the old rule's intent:
if nothing admissible is queued behind the head, do NOT fork — run the head
inline (a lone fork leaves home idle-waiting, strictly worse than inline).
Bonus simplification: members are admitted→begun→view-checked→executed in a
per-member pipeline, so a view overlap just STOPS the prefix (member runs later)
instead of aborting the whole batch; each member's view check runs under its own
read guard (sound — read_sets mutate only on this executor thread).

**Lazy spin-up timing:** do NOT spawn the worker during the hot path of the
reducer that first trips the threshold — that reducer runs inline (you only
learned it was slow as it finished). Spawn the worker **after that commit, in the
background**, so thread+instance instantiation is paid off the critical path and
the worker is warm before the *next* fork-warranting batch. The triggering
reducer never benefits; the worker pays off on subsequent batches.

**Cost of cap=1 (named honestly):** head-of-line-blocking relief and
multiple-heavy-reducer throughput partially trade off on one worker — if worker
(A) and home (B) are both busy with expensive reducers, a burst of cheap reducers
behind them waits for a lane to free. Bounded; acceptable v1. The configurable
max is the escape hatch once measured.

## 14. Feasibility checks — RowPointer RESOLVED, CommittedState RESOLVED

### RowPointer stability (Phase 3 blocker) — RESOLVED, favorably
**Source:** the delete API is already
`delete(tx, table_id, row_ids: impl IntoIterator<Item = RowPointer>)`
(`relational_db.rs`); `spacetimedb-table` exposes `PointerMap`
(`RowHash → RowPointer`). `RowPointer` is a plain value handle (page index +
slot + `SquashedOffset` discriminant separating committed vs tx-local rows),
i.e. `Copy`/`Send`, not a borrow.

**Conclusion:** recording deletes by value in `BatchTxState` is **not a
workaround — it's the existing model.** `BatchTxState` holds `Vec<RowPointer>`
for deletes (naturally `Send`); the drain calls the existing
`delete(tx, table_id, pointers)`. Stability from capture→drain is guaranteed by:
(a) `CommittedState` is the immutable snapshot during the batch, so no page is
moved/compacted/freed before the drain; (b) disjointness means no *other* batch
member touches this table, so nothing invalidates the pointer; (c) intra-reducer
delete-then-reread is handled by the existing per-overlay `TxState` semantics
(the committed-deleted-reinserted-seen-once case in `relational_db.rs`).
**This simplifies `BatchTxState` rather than complicating it.**

### CommittedState interior mutability (Phase 3 blocker) — RESOLVED (cloned-repo read)
**Resolved-ish:** row data in committed pages is stable/immutable (good). The
snapshot crate confirms "committed state at a tx offset" is a coherent
capturable thing; #4804 is actively factoring concerns *out of* `CommittedState`
(trend toward cleaner separability). NOTE the snapshot crate is *on-disk* — NOT
our mechanism; the shared read base must be an **in-memory** immutable view, not
a disk read on the hot path.

**Still-open risk (highest-risk item in the plan):** whether any *read* path
lazily mutates shared structure —
1. lazily-built/cached **indexes** (a read that builds a missing index),
2. **blob store** refcounting/caching touched on var-len reads,
3. memoized **aggregates** (row counts) updated on read.
If any exist, "N overlays share an immutable `&CommittedState`" needs either
synchronization (contention on the hot read path — bad) or eager precompute.

**RESOLVED AGAINST SOURCE (cloned repo read, June 2026) — all three risks
REFUTED. The foundation holds and is simpler than designed.**

- **`committed_state: Arc<RwLock<CommittedState>>`** (`datastore.rs:68`). Already
  behind `Arc<RwLock>`. Concurrent reads are ALREADY a first-class pattern (the
  snapshot worker holds a read guard; commit takes the write guard). Our
  "share immutable `&CommittedState` across N overlays" = taking additional
  RwLock **read** guards — already supported. **This replaces the need to invent
  a separate `CommittedSnapshot` type** (see §15 refinement).
- **All `StateView` read methods are `&self`, no interior mutation**
  (`committed_state.rs:133-187`):
  - `table_row_count` reads a **stored `u64` field** `table.row_count`
    (`table.rs:95`, inc/dec only in `&mut` insert/delete) — NOT memoized-on-read.
    **Risk 3 refuted.**
  - `iter_by_col_*` index paths: `get_index_by_cols(...)` returns an EXISTING
    index or, when `None`, **falls back to a table scan — it does NOT build an
    index** (`committed_state.rs:166-184`). All `seek_point`/`seek_range`/
    `get_index_by_cols` are `&self` (`table.rs`, `table_index/mod.rs`).
    **Risk 1 (lazy index build) refuted.**
  - `retrieve_blob(&self)` is a pure `HashMap::get` returning a borrowed `&[u8]`
    (`blob_store.rs:219-221`); refcount mutation is only in `clone_blob`/`free_blob`
    (`&mut self`), hit on insert/delete, NOT on read. Trait is `BlobStore: Sync`.
    **Risk 2 (blobstore refcount on read) refuted.**
- **Page pool:** shared across all modules on a host; `MutTxId` steals pages only
  in **insert paths** (`mut_tx.rs` `insert_physically_*`), writer-side. "Between
  transactions, untouched." Pure reads never touch it. No read concern.

**=> The warm-before-freeze mitigation is NOT needed.** No read path mutates
shared state. Overlays can read committed state under shared RwLock read guards
directly. Phase 3's highest risk is cleared.

**BONUS FINDING — existing read-set/write-set conflict machinery for VIEWS.**
`CommittedState` holds `read_sets: ViewReadSets` and already does exactly the
conflict detection this project proposes, but for materialized views: "We track
the read sets for each view... check each reducer's write set against these read
sets. Any overlap will trigger a re-evaluation" (`committed_state.rs:78-82`).
**Study `ViewReadSets` and the view re-evaluation path** — it may provide a
reusable conflict-tracking abstraction (and proves the maintainers already accept
read-set/write-set conflict detection as a design pattern). Added to open items.

**Mitigation (likely viable):** the home-thread drain is the only writer and runs
*after* the join, so the snapshot only needs immutability during the **parallel
execution window**. Warm the caches at batch start while still single-threaded
(force-build indexes, materialize counts), then freeze — converting "lazy
mutation during concurrent reads" into "eager population before concurrency,"
paid once per batch on the home thread. Whether this is cheap enough depends on
what the caches actually are.

**RESOLVED (cloned-repo read complete):** all read paths confirmed `&self` with no
lazy mutation — see the resolution block above in §14. No warm-before-freeze
needed. `committed_state` is already `Arc<RwLock<CommittedState>>`; overlays read
under shared read guards.

## 15. BatchTxState decomposition — REFINED against actual structs

Source (`mut_tx.rs:275`, `tx.rs:26`):
```
struct MutTxId {                                   struct TxId {
  tx_state: TxState,                                 committed_state_shared_lock:
  committed_state_write_lock:                          SharedReadGuard<CommittedState>,
    SharedWriteGuard<CommittedState>,  // EXCLUSIVE   lock_wait_time, timer, ctx, metrics
  sequence_state_lock: SharedMutexGuard<..>,       }   // NO _not_send marker
  lock_wait_time, read_sets, timer, ctx, metrics,
  _not_send: PhantomData<Rc<()>>,    // SYNTHETIC !Send
}
```

**Realizations this forces:**
1. **Why reducers serialize today is explicit:** `MutTxId` holds a
   `SharedWriteGuard<CommittedState>` (RwLock WRITE guard) for its whole lifetime.
   One write guard = one reducer at a time. Nothing subtler.
2. **The `!Send` is synthetic** — a deliberately embedded `PhantomData<Rc<()>>`,
   not thread-bound internals (matches #4039 "avoid incorrect cross-thread use").
   A Send variant means *upholding that discipline*, not fighting real affinity.
3. **`TxId` (read path) already holds a `SharedReadGuard` and has NO `_not_send`
   marker** — plausibly already `Send`. The read-over-shared-committed-state shape
   we need ALREADY EXISTS as `TxId`.

**Refined design — `BatchTxState` = "`MutTxId`'s delta over a READ guard":**
- Hold `SharedReadGuard<CommittedState>` (like `TxId`), not the write guard. N
  overlays each hold a read guard — RwLock permits concurrent readers; the batch
  runs while no writer holds the lock.
- Keep `tx_state: TxState` (owned staged delta: inserts + `Vec<RowPointer>`
  deletes — already the right shape).
- Drop `committed_state_write_lock`. The WRITE guard is acquired ONLY by the
  serial drain (home thread), one reducer at a time, to merge each `TxState` via
  the existing `CommittedState::merge(&mut self, tx_state, read_sets, ctx)`
  (`committed_state.rs:474`) — reuse verbatim.
- `sequence_state` is already `Arc<Mutex<SequencesState>>` (`datastore.rs:70`) —
  concurrent overlays draw under the existing mutex (matches §7 autoinc decision).
- Make `BatchTxState` Send by NOT embedding `_not_send` and ensuring fields are
  Send. Discipline upheld by construction: an overlay holds only a read guard, so
  it cannot mutate committed state → cross-thread use is safe.

**Net:** `BatchTxState` ≈ `TxId`'s read guard + `MutTxId`'s `tx_state`, minus the
write guard, minus the synthetic `!Send`. **We are NOT inventing a
`CommittedSnapshot` type** — the `Arc<RwLock<CommittedState>>` read guard IS the
shared immutable view. Decomposition is smaller than originally planned.

**Remaining Phase-3 verification (small):**
- ~~Confirm `SharedReadGuard<CommittedState>` is `Send`.~~ **CONFIRMED (TASK A).**
  `cargo check` passes `assert_send::<SharedReadGuard<CommittedState>>()`. The
  alias is `ArcRwLockReadGuard<RawRwLock, CommittedState>`; the workspace builds
  parking_lot with the `send_guard` feature (`Cargo.toml:249`), so the guard is
  `Send` for `T: Send + Sync`, and `CommittedState` is already `Send + Sync`. See
  the verification log [A1].
- Host-fn read path (`InstanceEnv`) resolves reads via the per-overlay read guard +
  its `tx_state`. Replicate how `MutTxId` layers `tx_state` over committed reads,
  but over a read guard. The `StateView` trait already abstracts this.

## 16. Phase-0 static access-set analyzer — DECIDED (TASK B, grilled 2026-06-10)

Design settled against the actual bindings codegen and a disassembled module
(`modules/perf-test`, `wasm-tools print`). Build plan lives in
`IMPLEMENTATION_GUIDE.md` Phase 0.

**Empirical findings that shaped it:**
- **14 table-touching host imports, not 5.** The full set (in
  `crates/bindings-sys/src/lib.rs`, `wasm_import_module = "spacetime_10.x"`):
  - table_id-keyed: `datastore_insert_bsatn` (W), `datastore_update_bsatn` (W,
    also takes index_id), `datastore_delete_all_by_eq_bsatn` (W), `datastore_clear`
    (W), `datastore_table_scan_bsatn` (R), `datastore_table_row_count` (R).
  - index_id-keyed: `datastore_index_scan_point_bsatn` (R),
    `datastore_index_scan_range_bsatn` (R, plus deprecated `btree_scan` alias),
    `datastore_delete_by_index_scan_point_bsatn` (W),
    `datastore_delete_by_index_scan_range_bsatn` (W, plus deprecated alias).
  - name resolvers: `table_id_from_name`, `index_id_from_name`.
  - id-free iterator ops: `row_iter_bsatn_advance`, `row_iter_bsatn_close`.
  **Supersedes §1's illustrative 5-symbol list.** Soundness rule extended: any
  *unrecognized* `spacetime_*` import a reducer can reach ⇒ wildcard (guards against
  future ABI ops the analyzer doesn't know).
- **TableId/IndexId are never compile-time constants.** Generated code resolves
  them at runtime: `static OnceLock<TableId>; get_or_init(|| table_id_from_name("X"))`,
  cached and read back. Confirmed in the wat: the per-table accessor is
  `i32.const <oncelock_addr>; call get_or_init; i32.load`, and `table_id_from_name`
  is called from exactly one site per table with a constant data-segment string.
  ⇒ recover the name by reading the data segment at the const ptr; bind the id's
  provenance to the per-table/index OnceLock static address / accessor fn. This
  fingerprint survives release inlining (the const address is stable).
- **Reducer dispatch:** single `__call_reducer__(id, …)` export dispatches by
  index through a funcref table → analyzing *through* it is unnecessary. Instead
  enter at each reducer body, found via the wasm **name section** (which survives
  Debug AND Release — release runs `wasm-opt -all -g -O2`, `-g` keeps names) with
  the always-present `__preinit__20_register_describer_<name>` **exports** as the
  ordering cross-check and a name-section-independent fallback. Follow **direct
  call edges** from the body.
- **Schema carries no per-reducer access metadata** (`ReducerDef`/`RawReducerDefV9`
  = name/params/lifecycle only). So access sets can only come from (a) static wasm
  analysis or (b) runtime capture — never the schema alone.

**Decisions:**
- **New standalone crate `spacetimedb-access-analysis`** (isolates the `walrus`
  dep; unit-testable against compiled `.wasm` with no runtime deps). Not a module
  inside `core`.
- **Full id-provenance tracker**, not reachability-only. Reachability-only would
  mark every reached table read∧write, proving zero disjointness and making TASK
  C's batch-width measurement an analyzer artifact. The tracker classifies each of
  the 14 ops as read/write and ties each op's id-arg to a resolved table/index
  name; index → parent table via `ModuleDef` (`TableDef.indexes`).
- **`call_indirect` handling:** constrain possible targets by element-segment ∩
  type-signature; wildcard the reducer only if a *table-relevant* target is
  possible. (Naive "any call_indirect ⇒ wildcard" would wildcard every reducer that
  formats/panics/logs via fmt vtables — confirmed those paths use call_indirect.)
- **Non-Rust modules (C#/C++/TS) and any unmatched pattern ⇒ wildcard** in Phase 0.
  Sound; those modules get no parallelism in v1. The **language-agnostic** recovery
  is **dynamic capture** at the host fns (every language calls the same resolved-id
  imports) — that is TASK C/Phase 1 + Phase 6 (§6), not Phase 0.
- **Tooling:** `walrus` (latest; absent from the lockfile, free to pin). `wasm-opt`
  v123 and `wasm-tools` 1.251 are installed locally. Tests obtain `ModuleDef` via
  `CompiledModule::compile(name, Debug|Release)` → `.extract_schema_blocking()`
  (`crates/testing/src/modules.rs`).
- **Acceptance bar: zero under-approximation** on perf-test (Debug + Release) and a
  new 2-table fixture. Over-approximation (false conflicts) is measured later in
  shadow mode (TASK C), not gated here.

**Out of Phase-0 scope:** conflict matrix, dynamic capture, scheduler, any
publish-path wiring or runtime change.

## 17. Phase-1+2 shadow harness — BUILT (TASK C, grilled + implemented 2026-06-10)

**Decisions (grilled with user; do not relitigate):**
- **Enablement:** env var `STDB_SHADOW_ACCESS=1`, default OFF. Off = one
  Option/None check on the hot paths; the #5095 lane is untouched. Report sink:
  `STDB_SHADOW_ACCESS_REPORT=<dir>` → `<database_identity>.json` (tmp+rename),
  periodic (`STDB_SHADOW_ACCESS_REPORT_INTERVAL_SECS`, default 60) + Drop;
  under-approximations ALSO `log::error!` per occurrence.
- **Sim is ungated:** admits by conflict matrix only, records width + per-member
  runtimes; the profitability threshold (k × handoff) is applied OFFLINE to the
  data. One sim, full information.
- **Handoff reference:** in-process proxy measured at shadow init (boxed-closure
  thread round-trip median, `handoff_proxy_ns_lower_bound`). Explicitly a LOWER
  BOUND — the §11 true measure (no-op reducer on a warm pool worker) requires the
  TASK D pool and replaces this.
- **Workload:** dedicated `sdk-test-shadow-access` module + SDK client bursts.
  HONESTY NOTE (per HANDOFF §4): a self-authored workload shapes its own width
  distribution — it validates the machinery and demonstrates the profitable
  regime *exists*, it cannot prove real-world *frequency*. §8's bet remains a bet.
- **Host-internal writes: doc-only** (see finding 1).

**Findings (source/run-confirmed):**
1. **Host-internal writes inside reducer txs are invisible to BOTH sides.**
   `delete_st_client` (OnDisconnect, `module_host_actor.rs:~1025`) writes
   `st_client` via direct tx methods, not instance_env host fns — so neither the
   analyzer (sees only module wasm) nor capture (hooks only module-initiated ops)
   sees it. Exclusion is symmetric → no false under-approx in shadow. **BUT
   Phase 5 needs an explicit rule before batching: lifecycle reducers
   always-conflict, or host-write capture.** CORRECTNESS-CRITICAL for TASK D.
2. **The wasm executor queue is opaque** — `SingleThreadedExecutor` jobs are
   boxed closures (`util/jobs.rs`); no introspection, no len(). Hence the shadow
   FIFO mirror fed at enqueue (`CallReducerParams.shadow_seq`). Executions that
   bypass registration (scheduled reducers via `from_system`, init, lifecycle,
   V8) appear as **sim barriers** → widths UNDERESTIMATE when scheduled reducers
   dominate. Note: scheduled jobs are part of §8's profitable regime — v1 shadow
   accepts the undercount (conservative direction); registering the scheduler
   path is a possible refinement if its traffic matters.
3. **View jobs share the executor queue but are not mirrored** → the sim can
   overcount width across an interleaved view job (a real prefix batcher cannot
   batch across non-reducer work). The test module has no views; interpret
   real-workload widths accordingly.
4. **Release-inlining over-approximation is worse than perf-test suggested.**
   On `sdk-test-shadow-access`, Release+wasm-opt wildcards even trivial `write_a`
   (accessor-chain inlining loses id attribution); perf-test stayed exact (TASK
   B), so it is shape-dependent. Over-approx is exactly what shadow measures, but
   for GO/NO-GO: Release modules may show far fewer non-wildcard reducers.
   Per-call-site attribution is the analyzer improvement if false conflicts kill
   widths.
5. **Width behavior (first run, `{1:126, 2:123, 32:1, 67:1}`):** alternating
   disjoint WRITERS cap at width 2 — a writing reducer self-conflicts
   (writes∩writes) with the next queued instance of itself, stopping the prefix.
   Pure readers batch arbitrarily wide (67 observed; read∩read is free). Real
   batch width therefore depends heavily on reducer diversity within queue
   bursts, not just table disjointness.

**Temporary-vs-permanent:** see the table in `IMPLEMENTATION_GUIDE.md` (TASK C
section). `shadow_access.rs`, `shadow_seq` threading, and the per-call diff are
measurement scaffolding to be removed/replaced at TASK D; the matrix, capture
hooks, and publish-wiring shape are permanent foundations.

**Release-server measurement run (2026-06-10, GO/NO-GO input data):**
fixture workload on release server + release module, one machine (laptop-class):
- under_approx = 0 again (both debug and release runs; gate holds).
- Widths `{1:126, 2:123, 37:1, 62:1}` — consistent with debug run.
- **Handoff proxy barely moved: 62µs (debug) → 53µs (release).** It is NOT a
  debug artifact — it is real thread park/wake latency on this machine. **DESIGN
  §8 assumed handoff ~1–5µs; measured proxy is ~10–50× that.** This raises the
  fork threshold from the assumed 50–100µs to ~160–265µs (k=3–5), before adding
  cold-instance cost the proxy doesn't capture. Caveats: std mpsc park/unpark
  round-trip may overstate a hot-looping pool worker; CPU governor unverified.
- Runtimes (release): heavy_a/b (500-row insert+scan, naive) mean **~295µs**,
  max ~445µs — clears k=3, MARGINAL at k=5. All cheap reducers collapse to
  **1–3µs** (debug had shown 85–100µs — 30–50× debug inflation). The deliberately
  heavy fixture reducer only *just* qualifies: the profitable regime on this
  hardware starts around several hundred row-ops per call.
- `write_c_2` wildcard in release on BOTH runs — deterministic attribution loss,
  not a coin flip (same twin `write_c_1` stays exact both times).
- Net honest read for the GO/NO-GO: machinery sound; profitable regime exists
  but is NARROWER than §8's bet because measured handoff is an order of
  magnitude above the assumed 1–5µs. Head-of-line-relief (§8b) remains the
  sturdier half of the thesis. Decision is a human design call (HANDOFF §5).

**GO/NO-GO — DECIDED: GO (user decision, 2026-06-10).** Basis: soundness
de-risked (under_approx = 0 on debug AND release real-server runs); head-of-line
relief is robust to the higher-than-assumed handoff (only ≥k×handoff reducers
ever fork, the benefit lands on the cheap lane behind them); the design
self-protects on slow hardware (self-calibrating threshold §11, all-cheap
workload pays one comparison, worst case = feature rarely fires); the §8
frequency bet stands as accepted premise — naive ~500-row reducers ≈ 295µs
already clear k=3 on laptop-class hardware.
- **Pre-checks deliberately skipped** (governor pin + hot-worker proxy variant):
  user judged them not worth the effort vs measuring the real thing — Phase 5's
  own startup measurement (§11: no-op reducer round-trip on the actual warm pool
  worker) supersedes any sharpened proxy. The 53µs shadow proxy remains a
  rough lower bound only.
- **Post-implementation validation gate (kill criterion, recorded per HANDOFF
  §4):** after Phases 3+5 land behind the threshold gate, compare shadow data
  vs k × *true measured* fork cost on a representative workload. If width≥2
  batches whose members clear that bar never materialize at a meaningful rate,
  the pool cap stays at its dormant default and is NOT raised — the feature
  remains inert scaffolding rather than being forced.

## 18. Phase-5 scheduler + worker — DECIDED (grilled with user, 2026-06-11)

Four forks resolved before any Phase-5 code. Source facts that shaped them are in
the verification log ("TASK D Phase 5 design facts").

### 18.1 Host-write rule → per-reducer VIEW-AWARE ADMISSION
**batchable(R) = tx=None ∧ non-lifecycle ∧ ¬wildcard ∧
predicted_writes(R) ∩ {tables with any view read set} = ∅**, evaluated at batch
construction under the batch's read window.
- Complete host-write inventory (source-verified): OnConnect `insert_st_client`
  (module_host.rs:694, tx pre-created `Some(tx)`); OnDisconnect `delete_st_client`
  (module_host_actor.rs:1083, tx=None but lifecycle-flagged); init `st_module`
  (`Some(tx)`); st_sequence refill (any autoinc reducer — covered by the Phase-3
  invariant: same sequence ⇒ same table ⇒ matrix conflict; merge is
  RowPointer-keyed); **view materialization** (`call_views_with_tx`, fires for ANY
  reducer whose tx wrote a table some subscribed view reads — runs module wasm +
  writes view backing tables + st_view_sub INSIDE the reducer tx); scheduled
  row-delete (scheduler.rs:593 — separate internal tx on the happy path, NOT a
  reducer-tx write; the `Some(tx)` delete sites are error paths + procedures).
- The view gate is exact and cheap: `views_for_refresh()` (mut_tx.rs:438) =
  written tables ∩ `CommittedState.read_sets`; empty ⇒ the view loop never runs ⇒
  zero view writes. Read sets exist only for views that have actually
  materialized (at subscribe, own tx, executor thread) — so a table-level
  over-approximation of that check is sound and **per-reducer**, not per-module.
  Modules that define views but have no overlapping live subscriptions batch
  freely (this was the decisive refinement — module-wide exclusion would have
  killed batching for most real modules).
- Structural bonus: every excluded path except OnDisconnect already arrives with
  a pre-created `Some(tx)` `MutTxId`, so the mechanical predicate is nearly free.
- **Rejected:** runtime host-write capture + abort/retry (pulls Phase-6 trap
  machinery forward; only actually buys OnDisconnect co-batching — views and
  scheduled both need separate refactors regardless — and introduces st_sequence
  false-conflict aborts unless special-cased); st_*-aware matrix (fragile
  enumeration; view writes are subscription-dynamic ⇒ degenerates to
  always-conflict).
- **Phase-6 forward-compatibility:** the trap converts the two biggest exclusions
  into optimistic admissions (view-overlap writers: admit, check
  `views_for_refresh` at overlay end, abort+rerun inline if non-empty; wildcards:
  learned sets per §6). Scheduled reducers need scheduler tx-plumbing work, not
  trap machinery. The admission predicate just gains "∨ trap-guarded".
- **[VERIFY at build]:** no off-executor-thread path mutates
  `ViewReadSets`/st_view_sub (procedures shouldn't touch subscriptions). Add a
  drain-time debug_assert: each member's written tables overlap no read set.

### 18.2 Scheduler placement → HOST-OWNED LOOP, typed reducer jobs
- Today: ALL per-module work (both reducer lanes, subscriptions, view jobs,
  scheduled calls) serializes through `SingleThreadedExecutor<WasmtimeModuleState>`
  (util/jobs.rs:289-460) — one `mpsc::UnboundedChannel<ExecutorJob<S>>` of opaque
  boxed closures drained by one pinned OS thread (loop at jobs.rs:334-345).
- **Decision:** fork the drain loop into host code for `WasmtimeModuleHost` only;
  the job type gains a typed `Reducer { params, … }` variant alongside opaque
  closures. Loop: typed head below threshold → run inline exactly as today (the
  one-comparison invariant); above → `try_recv` already-queued jobs into a local
  `VecDeque`, build the disjoint prefix from contiguous typed jobs (any opaque
  job = barrier — preserves total FIFO order across job kinds, which is
  observable semantics), dispatch. `util/jobs.rs` untouched for other users.
  V8ModuleHost keeps the old executor (JS modules are wildcard ⇒ unbatchable).
- Honest cost: the #5095 lane is re-implemented identically rather than literally
  untouched; the invariant kept is "all-cheap pays one comparison".
- **Rejected:** growing util/jobs.rs with a scheduler hook (reducer semantics leak
  into shared infra); side reducer queue (reorders reducers vs
  subscription/view jobs — breaks client-observable ordering).
- **shadow_seq replacement:** none needed — the scheduler sees the real typed
  queue. It records native stats (width histogram, fork count, per-fork
  runtime-vs-threshold, calibrated fork cost) for the kill criterion. The shadow
  harness (mirror, sim, report writer, `shadow_seq` field + registration sites,
  fixture module/client/integration test) is DELETED in the same phase per the
  GUIDE temp-table; capture hooks + matrix + publish wiring stay (wiring becomes
  unconditional).

### 18.3 Worker construction → SELF-CONSTRUCTING PINNED WORKER
- **CORRECTION (source wins):** `AutoReplacingModuleInstance` does not exist.
  Instance replacement today is trap-driven (`needs_replacement()` after every
  call → `create_instance()`, module_host.rs:397-406); republish replaces the
  WHOLE `ModuleHost` via `watch::Sender::send_replace` + `old_module.exit()`
  (host_controller.rs). §10's hot-swap surface mostly dissolves: a pool scoped
  inside the host dies with it on republish; the new host starts dormant.
- Spawn: lazy, post-commit of the reducer that first trips the threshold —
  `std::thread::spawn`; the worker clones the `InstancePre` (cheap), builds its
  own `InstanceEnv` (fresh `TxSlot` — sharing the main instance's would race) +
  wasm instance ON ITS OWN THREAD (instantiation off the home lane for free;
  Store never crosses threads), then signals ready. Pool treated as absent until
  ready.
- Dispatch: per-worker `std::sync::mpsc` job channel (plain OS threads, §10/§12);
  job = reducer params + owned args + `BatchTxState`. Worker runs the body via a
  new `TxSlot::set_batch` (the Phase-3-deferred setter), calls `finish()` ON THE
  WORKER (read guard drops there), returns `FinishedBatchTx` over the reply
  channel. Home joins by blocking recv after running its inline members. Nothing
  `!Send` crosses.
- Trap: worker self-checks `trapped` after each job and rebuilds its own instance
  (mirror of `with_instance` logic). Shutdown: host exit drops the sender → loop
  ends → thread exits (join-with-timeout in exit for hygiene); batches can't
  straddle exit since the home loop finishes the in-flight batch cycle first.
- No idle reap in v1 (cap=1, one warm instance; TTL deferred pending the
  warm-instance memory [VERIFY]).
- **Rejected:** home-built instance handed to the worker (instantiation on the
  home lane + Store thread-movement subtleties, no benefit); idle-reap TTL now
  (respawn churn exactly when load returns, unmeasured saving).

### 18.4 Threshold wiring → TWO-STAGE CALIBRATION, no wasm entry
- Chicken-and-egg: THRESHOLD = k × true fork cost measured on the warm worker
  (§11), but the worker is lazy and the first spawn decision needs a threshold.
- **Decision:** (1) at host init, THRESHOLD₀ = k × in-process thread-round-trip
  proxy — used ONLY to decide when to spawn the worker; (2) once the worker is
  ready, home dispatches a calibration job — read-guard acquire + `BatchTxState`
  setup + one trivial committed read + `finish()` + reply — THRESHOLD = k ×
  measured median; forking enabled only after calibration. Self-correcting: true
  cost ≫ proxy ⇒ threshold rises before any real fork fires.
- The calibration job deliberately does NOT enter wasm: no universal no-op
  reducer exists, fabricating args for user reducers is unsound, and
  `__describe_module__` does real schema work (overestimates). Wasm-entry cost is
  paid by the inline lane too (≈ cancels); cold-linear-memory effects are missed —
  documented lower bound, k absorbs.
- `runtime_stats[reducer_id] = { high_water, ema }` lives in the scheduler loop's
  own state (home-thread-only ⇒ no atomics; NOT in immutable `ModuleInfo`).
  Updated post-commit for inline and forked runs; reset on republish (ids
  change; init-0 ⇒ first run always inline per §9). EMA α = 1/8. Fork condition:
  both high_water AND ema > THRESHOLD (§9, settled).
- Config: `STDB_REDUCER_BATCHING` (default on; **0 = hard kill switch** — the
  kill-criterion "leave dormant" lever) and `STDB_REDUCER_FORK_K` (default 4 —
  GO data: heavy fixture cleared k=3, marginal at k=5). Same env-var pattern as
  the shadow flags it replaces.
- **Rejected:** eager measure-at-publish (every module pays instance creation,
  breaks lazy zero-cost); static ns threshold (§11 already rejected); wasm-entry
  calibration (no sound entry point).

## 19. Phase-6 learned sets + early-cut trap — DECIDED (grilled with user, 2026-06-12)

Lifts the two big admission exclusions (wildcard reducers incl. the
Release-inlining losses; view-overlap writers) per §6's tier model and §18.1's
forward-compat note. Eight forks resolved; full unit plan in
`design/PHASE6_PLAN.md`. Source facts that shaped them are in the log entry
"Phase 6 design grilled".

### 19.1 Storage → PER-BOOT, SchedulerState-owned
Learned state (tiers, accumulators, strikes) lives in the scheduler loop's own
state — loop-thread-only, zero locks, same placement rationale as
`runtime_stats` (§18.4). Republish replaces the whole `ModuleHost`
(host_controller `send_replace`) ⇒ publish/migration invalidation is FREE.
Re-learning rides the per-boot threshold warmup that already exists.
- **Rejected:** persistent system table keyed on module hash (schema +
  invalidation + cross-binary trust story; benefit is only skipping ~8 warmup
  runs). Revisit only if production shows warmup pain (per-boot shape kept
  serializable-friendly).

### 19.2 Static-matrix interplay → IMMUTABLE MATRIX + TIER OVERLAY
Per-reducer `AccessTier { Static, Unknown(ObsState), Learned(LearnedSet),
Parked }` + strike counter, derived at `SchedulerState::new` from the analyzer
wildcard flags. The `Option`-ness of static analysis IS the learn-decision
signal (user's framing). Admission: `Static×Static` → matrix bit (today's path
untouched); any `Learned` side → direct IntSet intersection of effective sets;
`Unknown`/`Parked` → reject. Learned never overrides a static non-wildcard set
(§6 ordering). Capture yields `IntSet<TableId>` directly — learned sets need NO
name resolution, immune to both release inlining and the convert_case
snake-casing bug class (stage-1 log).
- Static side of a mixed intersection needs static READ tables as TableIds:
  extend the lazy `resolved()` pass (`ResolvedWrites` → `ResolvedAccess`).
  Rejected alternative: capture on static reducers (puts per-op cost on the
  proven path; erodes static-tier trust).
- **Rejected:** matrix rebuild on promotion (Arc copy-on-write surgery;
  uniformity buys nothing measurable — the intersection runs only when a fork
  is already being considered, µs-scale).

### 19.3 Promotion/demotion → UNION-STABLE GATE + STRIKE CAP
Promote `Unknown → Learned` when total committed-run observations ≥ 8 AND the
last 4 added no new table to the union (learned set = union of all
observations). Soundness does NOT depend on the gate — the trap catches any
escape — so the gate only tunes trap frequency; union-stability filters
data-dependent access patterns that would churn. Demotion on any escape:
strike++, re-seed accumulator with `old-learned ∪ escape observation`; second
strike → `Parked` for the boot (capture off, plain wildcard behavior, zero
further cost). Only committed runs feed the accumulator.
- **Rejected:** fixed-N promotion (promotes unstable patterns into churn);
  promote-on-first-run (max churn); unlimited re-learning (unbounded
  oscillation); one-strike-park (kills sets that were merely incomplete).

### 19.4 Trap → EARLY-CUT (user's design + conflict refinement)
The key structural fact: non-head members execute SEQUENTIALLY on the home
thread, so the trap check runs immediately after each member's body — before
any later member is admitted. Victims after a violator are structurally
impossible; the drain-time cascade machinery dissolves.
- **Serial-order blast-radius theorem (grilled):** all members run against the
  same committed snapshot; serial-equivalence for member i needs
  `reads(i) ∩ writes(earlier) = ∅` and `writes(i) ∩ writes(earlier) = ∅`.
  An escape by k can therefore never invalidate the head or members before k
  (their conditions don't mention k); only k itself and members after it are
  at risk. Whole-batch abort is strictly dominated.
- **Member case:** maintain `earlier_writes` IntSet (head + executed members'
  effective writes) during batch build. Post-body, on escape: demote; if
  observed sets are disjoint from `earlier_writes` (harmless escape) the
  member STAYS and commits — its delta is provably serial-correct; if they
  intersect, abort the overlay (drop `FinishedBatchTx` uncommitted — clean,
  calibration precedent) and requeue the payload at the front of `pending` =
  its exact serial FIFO slot (the existing prefix-stop mechanism). Either way
  CUT (stop admitting).
- **Head case:** head's observed arrives at join, which precedes the drain —
  no committed-victim window exists. Head always commits. Walk members in
  order vs head's full observed writes; first conflict ⇒ that member + all
  later abort + requeue in order (cut-the-rest, not skip-per-member — keeps
  the settled FIFO-commit invariant); clean prefix commits.
- Requeued payloads keep their reply channels; the eventual committed run
  answers the caller. Requeued violators are demoted ⇒ re-enter as `Unknown`
  ⇒ run inline with capture ⇒ feed re-learning. Autoinc draws from aborted
  overlays leak as gaps — sequences are non-transactional and never reclaimed
  (§7), already documented; nothing to restore across retry.
- **Rejected:** drain-time hazard-check with running union + inline reruns at
  drain (superseded — strictly more machinery than early-cut for the same
  precision); escape-always-aborts (extra reruns of provably-correct deltas);
  whole-suffix abort at drain (coarse); whole-batch abort (dominated, reruns
  the expensive head for zero correctness gain).

### 19.5 Energy → CHARGE COMMITTED RUN ONLY
A trap-aborted execution burned real cycles, but the mis-batch was the host's
speculative bet — the host eats it (aligns with the zero-tax dormant posture).
**[VERIFY at build]:** locate the budget debit site; if un-billing the aborted
run is invasive, surface options instead of hacking.

### 19.6 View-overlap writer lift → TIERED SPECULATION, MEMBERS ONLY
The real gap: admission's `view_read_overlap` is TABLE-level, while
`views_for_refresh` is KEY-level precise — a writer whose table overlaps a
view's read set but whose written keys miss the read keys is batchable, and
only provably so post-execution.
- Member pre-filter: written table in any view's FULL-SCAN read set ⇒ refresh
  certain ⇒ excluded pre-execution (speculation always loses); overlap only
  via INDEX-KEY entries ⇒ admit speculatively; no table overlap ⇒ admit, skip
  post-check (table-level is a sound superset).
- Speculative members get a post-body key-level check
  (`view_refresh_nonempty()`, a read-guard port of `views_for_refresh` — BOTH
  branches, parity-tested; under-detection is the corruption case). Non-empty
  ⇒ abort + requeue + cut via the same early-cut path; NO strike (view misses
  are subscription-data-dependent, not learned-set wrongness). The inline
  rerun executes the real view machinery.
- **Integrity argument (user's acceptance condition):** the pre-filter is pure
  economics — safety rests ONLY on the post-body check, which runs for every
  speculative member before anything commits. Check-to-drain stability: read
  sets mutate only on the executor thread under the write guard, and that
  thread is inside `run_batch` for the whole window.
- Head stays fully conservative (existing table-level pre-check): a head
  view-trap cannot commit through the batch drain (no view machinery) and
  would abort the entire batch.
- **Rejected:** speculate on all overlap (guaranteed-loss reruns for full-scan
  overlaps); speculate the head (whole-batch abort risk, likely net-negative).

### 19.7 Non-Rust modules → SPLIT: C# batches NOW (learning-only); only TS/V8 deferred
**Corrected post-build (2026-06-12), see verification log "Cross-language
benchmarks".** The original note lumped C#, C++ and TS as "deferred non-Rust";
that is wrong about C#/C++.
- **C# (and C++) compile to wasm and run on the SAME wasmtime host +
  `BatchingExecutor` as Rust** (`module_host.rs:1727`; both are `HostType::Wasm`,
  indistinguishable from Rust at the host). The static analyzer is
  Rust-symbol-keyed, so it wildcards every C# reducer → `Unknown` tier → no
  static (v1) batching. But Phase-6 LEARNING recovers them: capture fires on the
  language-agnostic bindings ABI, so C# wildcard reducers promote and batch
  exactly like the Rust Release-wildcard `run_game_ia_loop`. **Measured: a C#
  port of the synthetic mixed-burst fixture promotes all 7 reducers and runs
  1.57× (base cap=0 28.6 ms vs v2 cap=1 18.2 ms, width-5 batches in 20/21
  rounds).** So for a C# module, `v1 ≡ base` (no batching) and `v2` is a real
  win — the language-agnostic claim holds and is live, not deferred.
- **TypeScript (V8) IS deferred.** V8ModuleHost uses its own single-threaded
  worker thread, never `BatchingExecutor`; dispatch bypasses the batch lane
  (`module_host.rs:2294`) and `batch_stats()` is hardcoded `None` for Js. There
  is no v2 code path for TS — `base ≡ v2`. Enabling it is new host work (V8
  adopting a fork scheduler), out of Phase 6.

The learning machinery is language-agnostic by construction — capture lives in
the datastore, keyed on TableId, no wasm analysis involved — so learned sets
work the moment a host adopts `BatchingExecutor` (C#/C++ already have; V8 has
not).

### 19.8 Fast-path cost
`fork_eligible`'s wildcard-vec check becomes a tier-discriminant check — same
O(1) cost class. All-static batches skip trap validation entirely. Capture is
enabled only on tiers that today cannot batch at all (`Unknown` learning
inline, `Learned` trap evidence).

### 19.9 Fork profitability — TAIL-COST GATE (decided 2026-06-19, benchmark-driven)
The v1 executor is 2-lane: the head runs on the single worker; admitted members
run on the home/loop thread sequentially; commits drain FIFO (head first). Fork
saving vs serial is therefore `min(head_body, Σtail_body)` — the commit drain is
serial either way and cancels. The head already cleared the entry threshold, so
the binding question is the TAIL: **fork only when the admissible disjoint tail's
estimated body cost also clears the fork threshold.**

- **Replaces the lone-head guard** ("fork iff any companion exists"), which
  over-forked: a heavy head with only cheap members ties up the worker for the
  head's whole duration to overlap microseconds of cheap work — net loss once
  fork overhead + per-member tx/view-check cost are paid.
- `tail_est` = sum of per-reducer runtime EMAs over the admissible front-prefix
  (same walk as the Step-4 admission loop). It is an upper bound (Step 4 may cut
  members on a view/trap) ⇒ only over-forks at the margin, never under.
- **Bar = the live fork threshold** (`k × fork_cost`). Swept alternatives —
  break-even (`1×fork_cost`), `2×fork_cost`, head-relative `0.75×(head+fork_cost)`
  — all lost or tied. Real per-member batch overhead exceeds the calibrated
  `fork_cost`, so the conservative full-threshold bar is both best AND simplest
  (`tail_est >= threshold`; no multiply/divide/head-EMA). The bar knobs were
  prototyped, measured, and **dropped**.
- **Always-on** (no env toggle — locked in). Diagnostic only: `STDB_BATCH_LOG=1`
  logs `batches/forks/widths` every 100 batches.
- Measured (keynote-2 transfer harness, network + confirmed reads, seeded +
  5-run avg, vs the lone-head guard): **+6.4%** cheap-tail mix, **+7.1%** medium,
  **+15.1%** realistic tri-class (10% heavy / 30% medium / 60% cheap); width-2
  balanced pairs 1.56×→**1.66×**. Wins or ties in every workload tested.

### 19.10 Feature switch — `STDB_REDUCER_POOL_CAP` → `STDB_REDUCER_BATCHING` (2026-06-19)
The numeric pool cap was redundant: v1 honors exactly one worker, and the bench
sweep showed no gain from a higher cap (the win is the 2-lane overlap, not worker
count — §19.9). Collapsed to a **boolean feature switch**: `STDB_REDUCER_BATCHING`,
**default on**; disable (the synchronous #5095 kill-switch lane) with
`0`/`false`/`off`/`no`. The internal worker count stays 1 (raising it is a Phase-2
question, gated on evidence of 3+ simultaneously-runnable heavy disjoint reducers).
`STDB_REDUCER_FORK_K` (threshold multiple) is unchanged. Code: `BATCHING_ENV` +
`read_env_bool` in `reducer_scheduler.rs`; the `reducer_batching_off_test` kill-
switch test and the keynote/bench harnesses use the new name.

## Open questions (not yet decided)- **k runtime adaptation:** fixed k for v1; revisit whether k should adapt to
  observed mis-fork rate.
- **Per-module pool cap + idle-reap TTL:** v1 caps at **1 worker (configurable
  max, default 1)** — see §13. Raising the default past 1 wants Phase 2 evidence
  of 3+ simultaneously-runnable heavy disjoint reducers. Idle-reap TTL wants the
  warm-instance memory-cost measurement.
- **Shared host-wide pool:** deferred future option (§12); reopen only on
  profiling evidence + a deliberate tenant-isolation decision.
- **ViewReadSets reuse:** study the existing view read-set/write-set conflict
  machinery (`committed_state.rs:78-82`, `ViewReadSets`) — may provide a reusable
  conflict-tracking abstraction for reducer access sets (§14 bonus finding).
- **Shadow-mode metrics:** precise batch-width distribution + runtime/handoff
  histograms to make the go/no-go call before BatchTxState work.

## Verification log (source-confirmed facts)

- `MutTxId` is `!Send` (PR #4039) — discipline marker, not raw-pointer-bound.
- Reducer lane is synchronous single-OS-thread post-PR #5095 (merged 2026-05-28);
  procedures stay on async Tokio runtime. Two separate runtimes per module.
- Table ABI: insert/delete_all_by_eq keyed on `table_id`; scan/update/delete_by_index
  keyed on `index_id` (`crates/bindings-sys/src/lib.rs`).
- Sequences: per-table, non-transactional, chunked (4096), gap-tolerant (STDB appendix).
- Autoinc read-back: `insert` returns assigned ID, usable mid-reducer (docs).
- Procedures `with_tx`: OCC / re-runnable, "may be invoked multiple times" (docs).
- Datastore is `Locking` wrapping committed state; snapshots exist at tx offsets.
- `InstanceEnv` holds `TxSlot` as a **thread-local**; module code reaches the
  active tx through it, not via ABI arg (DeepWiki arch overview, `instance_env.rs`).
  → running a reducer body requires a wasm instance with its thread-local set.
- One wasm instance per worker (#4663 single instance + FIFO; #5095 sync executor).
- #4973 "pipeline wasm module operations" exists → check `util/jobs.rs` for a
  reusable worker-job abstraction.
- `ModuleHost` is a **per-database actor** (one per DB), containing
  `WasmtimeModuleHost`/`V8ModuleHost` + immutable `ModuleInfo`
  (`module_host.rs`). The #5095 home lane lives inside `WasmtimeModuleHost`.
- `HostController` is the only host-wide owner (cheap-clone, `Arc`/`Copy` fields),
  manages all per-DB `ModuleHost`s + replica ids (#4160). **No existing shared
  cross-module compute/thread pool** — all compute is currently per-database.
- Delete API is `delete(tx, table_id, IntoIterator<Item = RowPointer>)`
  (`relational_db.rs`); `RowPointer` is a `Copy`/`Send` value handle (page+slot+
  `SquashedOffset`), NOT a borrow. Deletes-by-value is the existing model.
- `spacetimedb-table` exposes `PointerMap` (`RowHash → RowPointer`); rows live in
  pages, var-len in a blob store.
- Snapshot crate (`spacetimedb-snapshot`) = **on-disk** view of committed state at
  a tx offset (NOT the in-memory read base we need). #4804 factoring replay out of
  `CommittedState`.
- **CONFIRMED (cloned repo):** `committed_state: Arc<RwLock<CommittedState>>`
  (`datastore.rs:68`). All `StateView` reads are `&self`, no lazy mutation:
  `table_row_count` reads stored `u64` field; missing index → table scan (no
  build); `retrieve_blob(&self)` is pure `HashMap::get`; refcount only in
  `&mut clone_blob`/`free_blob`. `BlobStore: Sync`. Page-pool steal is insert-only.
  All 3 read-mutation risks REFUTED.
- **CONFIRMED:** `MutTxId` holds `SharedWriteGuard<CommittedState>` for its whole
  life (= why reducers serialize); `!Send` is a synthetic `PhantomData<Rc<()>>`.
  `TxId` holds `SharedReadGuard<CommittedState>` and has NO `_not_send` marker
  (read path already ~Send). `sequence_state: Arc<Mutex<SequencesState>>`.
- **BONUS:** `CommittedState.read_sets: ViewReadSets` — existing read-set/write-set
  conflict detection for materialized views; the exact pattern this project
  proposes, already accepted by maintainers. Candidate to reuse/study.

### TASK A verification (completed — this session, against current clone)

All three Phase-3 verification items from `HANDOFF.md` §TASK A are resolved.
Source anchors re-confirmed exact (datastore.rs:68/70, committed_state.rs:63/133/474,
mut_tx.rs:275, tx.rs:26, indexes.rs RowPointer, bindings-sys ABI symbols).

- **[A1] `SharedReadGuard<CommittedState>: Send` — EMPIRICALLY CONFIRMED.**
  `cargo check -p spacetimedb-datastore` passes with
  `assert_send::<SharedReadGuard<CommittedState>>()`. Two enabling facts:
  - Workspace `Cargo.toml:249`:
    `parking_lot = { version = "0.12.1", features = ["send_guard", "arc_lock"] }`.
    `send_guard` flips `RawRwLock::GuardMarker` `GuardNoSend → GuardSend`, so
    `ArcRwLockReadGuard<RawRwLock, T>: Send` whenever `T: Send + Sync`. `arc_lock`
    provides the `read_arc()`/`ArcRwLockReadGuard` API the aliases use.
  - `CommittedState: Send + Sync` already holds (it is shared cross-thread today —
    `Arc<RwLock<CommittedState>>` is handed to the snapshot worker thread).
  - Corollary: this is also why `MutTxId`'s `!Send` is *synthetic* — the write
    guard is itself `Send` under `send_guard`; only the `PhantomData<Rc<()>>`
    marker makes `MutTxId` `!Send`. Dropping the marker (per §15) yields a `Send`
    `BatchTxState`. Confirmed.
  - (Toolchain note: local `clang`/LLVM is broken — wants `libLLVM.so.22.1`, only
    21.1/19.1 present — so cargo must be run with
    `CARGO_TARGET_X86_64_UNKNOWN_LINUX_GNU_LINKER=cc RUSTFLAGS="--cfg tokio_unstable"`.
    Pure environment issue, not a code issue.)

- **[A2] No background mutation of committed pages while read guards held — CONFIRMED SAFE.**
  No compaction / vacuum / GC / defrag / page-reclamation runs on committed
  tables. All page-free and blob-refcount paths are `&mut self`
  (`page.delete_row`, `blob_store.clone_blob`/`free_blob`), reachable only from
  the write-side merge/insert/delete. `CommittedState` has no interior mutability.
  The only async maintenance worker is `snapshot_watching_commitlog_compressor`,
  which compresses *commitlog segments* (durability layer), never in-memory pages.
  - **Latency interaction to track for Phase 5 (not a correctness issue):** the
    snapshot worker takes the committed-state **write** lock
    (`take_snapshot_internal` → `committed_state.write()`, datastore.rs:245) because
    it caches page hashes (`page.save_or_get_content_hash`, `&mut`). So a snapshot
    in flight blocks a reducer batch from acquiring read guards, and an in-flight
    batch blocks a snapshot. RwLock serializes them — correct, but a throughput
    interaction the scheduler should be aware of.

- **[A3] `ViewReadSets` reuse — DECISION: mirror the pattern, do NOT literally reuse.**
  `ViewReadSets` (`mut_tx.rs:87-90`) is `IntMap<TableId, TableReadSet>` answering
  "which *views* read table X?" — view-centric, asymmetric, and row/key-granular
  for index seeks (`HashMap<ColList, HashMap<AlgebraicValue, HashSet<ViewCallInfo>>>`).
  The reducer feature needs the transpose: per-reducer `AccessSet { reads, writes }`
  at table granularity (DESIGN §1) + a symmetric N×N conflict matrix (DESIGN §2),
  computed *statically at publish* (different lifecycle from the runtime-populated,
  commit-merged `ViewReadSets`). It also carries view-only `replacements` /
  `ViewCallInfo(view_id+sender)` semantics. So: **Phase 0 analyzer = new code;
  conflict matrix = new symmetric bitset.** Two concrete borrows survive:
  (1) it proves maintainers already bless read/write-set conflict detection;
  (2) the dynamic-capture hooks `record_table_scan` / `record_index_scan_*`
  (`mut_tx.rs:294-378`) already take `op: &FuncCallType` and branch on
  `FuncCallType::View(_)` — **Phase 1 shadow capture should add a reducer arm to
  these existing hooks rather than build fresh capture.**

### Source-vs-doc CORRECTIONS found during TASK A (source wins, per HANDOFF §4)

- **TxSlot is NOT a thread-local.** §10 and the verification log below state
  `InstanceEnv` holds `TxSlot` as a thread-local reached without an ABI arg
  (sourced from a DeepWiki overview). Actual source (`instance_env.rs`):
  `pub struct InstanceEnv { pub tx: TxSlot, .. }` with
  `pub struct TxSlot { inner: Arc<Mutex<Option<MutTxId>>> }` — a plain field, not a
  thread-local. The *conclusion* of §10 (a reducer body needs a live wasm instance,
  and instances are pinned to their worker thread) still holds, but the
  *mechanism* ("reaches the tx through a thread-local") is wrong — it reaches the
  tx through the `InstanceEnv.tx` field. Consequence for Phase 5: because
  `MutTxId: !Send`, `Mutex<Option<MutTxId>>` and therefore `InstanceEnv` are
  `!Send` *today*; Phase 5 must give a worker an `InstanceEnv` whose slot holds a
  `BatchTxState` (Send) instead of a `MutTxId`. Flagged for TASK D; does not affect
  TASK A/B.
- **`util/jobs.rs` is async (Tokio), not an OS-thread pool.** It provides
  `JobCores` — a pool of single-threaded **Tokio** executors with optional core
  pinning. DESIGN §10/§5 require reducer workers to be *plain OS threads separate
  from the procedure async runtime*. **[VERIFY] resolved: do NOT reuse `jobs.rs`
  for the reducer worker pool** — it would re-couple to the async runtime #5095
  separated. (It may still be relevant to the *procedure* side, out of scope here.)

### TASK D feasibility — off-thread DB access CONFIRMED (this session)

The load-bearing premise of Phase 5 (run reducer bodies on worker threads) is
**source-confirmed**: SpacetimeDB already runs the datastore read+write path from a
non-reducer OS thread today.
- **Direct precedent — V8/JS procedures.** Each JS procedure runs on its OWN
  `std::thread::spawn`ed thread (`crates/core/src/host/v8/mod.rs:1546`), acquires
  the committed-state lock (`InstanceEnv::start_mutable_tx` → `begin_mut_tx`,
  `instance_env.rs:741`), runs the full host-fn read path (`datastore_table_scan_bsatn`,
  `index_scan_*`, etc., registered in `v8/syscall/v2.rs`) and commits — all off the
  reducer thread. (WASM procedures do NOT: they async-interleave on the single
  executor thread. The snapshot worker reads off-thread but via raw page access, not
  `StateView`. The StateView-off-thread proof is the V8 path.)
- **No read-path thread-affinity landmine.** All `thread_local!`s on the read path
  are per-thread scratch buffers (safe); metrics are per-tx + global prometheus
  atomics; logging is an `mpsc` channel (Send+Sync); no thread-id / "must be reducer
  thread" assertions. Wasmtime `Store` is "one thread at a time", not "all share one
  thread" (the per-worker, never-moved instance model satisfies this). The single
  reducer thread is a **scheduling choice, not a safety constraint** (`module_host.rs`
  comment: "Reducers are not executed concurrently, and so there is no pool").
- **Concrete Phase-3 work item (refactor, NOT a blocker):** today's read host
  functions take `&mut MutTxId` (e.g. `datastore_table_scan_bsatn_chunks`). A worker
  holds a read-only `BatchTxState` (a `SharedReadGuard` + its own delta, shaped like
  `TxId`), so **the read host-fn path must be generalized over a read-only
  `StateView`-style tx** rather than `&mut MutTxId`. The data structures + threading
  already support it; `MutTxId: !Send` is deadlock-hygiene, and the worker holds a
  read tx, not a `MutTxId`.

### TASK D Phase 3 — BatchTxState BUILT (2026-06-10)

Implemented per §15; independent opus review: **adequate** (2 MINOR findings fixed,
1 NIT dedupe fixed). All green: datastore 87 (`--features test` — required for this
crate's tests, bare invocation never compiled), core lib 196, standalone check.
(`tries_older_snapshots` flaked once on a tmp-dir race — passes isolated and on
rerun; untouched by this diff.)

- **Discovery that shrank the work:** the row-op cores were ALREADY free functions
  over an immutable `&CommittedState` — `insert` (mut_tx.rs:2937), `delete` (:3287),
  `get_next_sequence_value` (:1730), and the `iter*`/`table_row_count` StateView
  helpers. `update` was the one exception; its body was extracted verbatim into a
  matching free fn. `clear_table` verified read-safe (immutable
  `get_table_and_blob_store`, stages deletes in tx_state; mut_tx.rs:3267). **No
  reducer-reachable op needs `&mut CommittedState`; only DDL does** — so
  `BatchTxState` shares the same fns instead of duplicating logic, and DDL is
  unreachable on it by construction (no such methods; `rollback()` debug_asserts
  `pending_schema_changes.is_empty()`).
- **Shapes:** `BatchTxState` (batch_tx.rs) = `tx_state` + `SharedReadGuard` +
  `Arc<Mutex<SequencesState>>` (locked per draw, not tx-life) + `read_sets`/ctx/
  timer/metrics/`observed`, NO `_not_send`; `Send` compile-asserted.
  `finish() -> FinishedBatchTx` drops the read guard BEFORE the drain — forced:
  the drain's `write_arc()` deadlocks while sibling overlays hold read guards, so
  guards drop at the join barrier. `Locking::begin_batch_tx` /
  `Locking::commit_batch_tx` (write_arc + `CommittedState::merge` verbatim, same
  return shape as `commit_mut_tx`); a debug_assert checks staged delete pointers
  still resolve (cheap tripwire for a broken disjointness contract).
- **Host-fn generalization (the known refactor) — DONE via enum dispatch:**
  `ReducerTx: StateView` trait (reducer_tx.rs) = the reducer-reachable surface;
  impls for `MutTxId` (pure delegation) + `BatchTxState`;
  `ReducerTxVariant { Mut, Batch }` impls both by match (iterator types identical
  across arms, so StateView assoc types stay concrete). `TxSlot` holds the enum
  (set/take keep `MutTxId`-shaped APIs; `take` panics on `Batch` — documents the
  v1 invariant that only `set_raw` writes the slot). instance_env host fns call
  trait methods; relational_db reducer wrappers generic over `ReducerTx`
  (DDL/publish/SQL/subscription/views stay concrete `MutTxId`). Hot-lane cost:
  one predicted match per host call behind the existing slot mutex. **Nothing
  constructs `Batch` until Phase 5** — zero behavior change, verified by the
  untouched shadow + core test suites. module_host_actor / v8 / wasmtime glue
  needed NO edits.
- **Sequence semantics:** per-draw locking means the whole `insert` call holds the
  sequences mutex (MutTxId holds it for the whole TX today, so no regression);
  Phase 4 can narrow to a fast-path atomic draw. Rollback drops draws — gaps per
  §7 (allocator non-transactional).
- **NEW FINDING (fold into the Phase-5 §17-finding-1 decision):** sequence REFILL
  writes `st_sequence` rows through the overlay's own `tx_state` — a
  shared-SYSTEM-table write invisible to the conflict matrix, same family as §17
  finding 1 (host-internal writes). Sound under current batching rules because:
  same sequence ⇒ same user table ⇒ matrix conflict ⇒ never co-batched; different
  sequences ⇒ different `st_sequence` rows, and merge deletes by `RowPointer`
  while rows never relocate on other rows' delete/insert. But the Phase-5
  lifecycle/host-write rule MUST account for st_* writes explicitly, not by
  accident.
- **Tests** (batch_tx.rs): compile-time `assert_send`; MutTx-vs-Batch equivalence
  (insert/update-via-unique-index/delete-committed-row/clear_table/
  read-your-own-writes; final row sets + per-table TxData rows compared); autoinc
  draw across a forced 4096-chunk refill; two-thread overlay smoke (delete
  pre-committed row + insert per thread, FIFO drain, offsets consecutive).

### TASK D Phase 4 — autoinc interplay VERIFIED (2026-06-11, no code change)

Both remaining Phase-4 items resolved by source verification; nothing to build.

- **(a) MutTxId-vs-Batch sequence interplay — SAFE, and the concern was vacuous.**
  - Lock hierarchy is documented at `datastore.rs:58-66`: `memory` →
    `committed_state` → `sequence_state`, and both acquirers comply:
    `begin_mut_tx` takes the committed-state write guard (`:902`) **then** the
    sequence mutex (`:903`); `BatchTxState` holds the committed-state read guard
    as a field for its whole life and locks the sequence mutex per-draw inside
    `insert`/`update`/`get_next_sequence_value` (batch_tx.rs). Same order on
    every path ⇒ no deadlock cycle. (The drain `commit_batch_tx` never touches
    the sequence mutex — merge does not draw.)
  - The GUIDE's "a batch can't draw while a serial reducer runs — correct,
    possibly slow" is stronger than feared: it is **vacuous**. A `MutTxId` holds
    the committed-state WRITE guard for its whole life; live overlays hold READ
    guards for theirs. The RwLock makes them mutually exclusive, so by the time
    a MutTx holds the sequence mutex, no overlay exists to block on it. A batch
    member never waits on a serial reducer's tx-life sequence lock.
  - The REAL contention surface is overlay-vs-overlay: all sequences share ONE
    global `SequencesState` mutex, so two overlays inserting into disjoint
    autoinc tables still contend per-draw. Critical section is a counter bump
    (rare refill adds an st_sequence read+write through the overlay's tx_state).
    At v1 cap=1 there are at most 2 contenders.
- **(b) Fast-path atomic fetch-add — DEFERRED (decision).** No correctness need;
  the refill path must remain under the mutex regardless; v1 width is 2.
  Revisit only if Phase-5 contention data shows per-draw mutex pressure.
- The user-docs note (autoinc IDs interleave across concurrent reducers) remains
  a release-notes item for when the feature ships — no code carrier yet.

### TASK D Phase 5 design facts (source-verified 2026-06-11, pre-build)

Anchors gathered for the §18 decisions (3 parallel explorers + spot-checks):
- Queue: `WasmtimeModuleHost { executor: SingleThreadedExecutor<WasmtimeModuleState>, .. }`
  (module_host.rs ~:407-420); `ExecutorJob::{Sync(Box<dyn FnOnce(&mut S)+Send>), Async}`
  (util/jobs.rs:276-279); drain loop jobs.rs:334-345 (also services Async jobs via
  LocalSet — the new loop must keep that arm); producers: sync call lane
  `call_reducer_with_params` (module_host.rs:2288-2304, `run_sync_job` blocks caller
  via oneshot), enqueue lane `enqueue_reducer` (:2377-2423, `enqueue_sync_job`),
  scheduler/lifecycle/init construct `CallReducerParams::from_system` (:777-794).
- Reducer execution core: `InstanceCommon::call_reducer_with_tx`
  (wasm_common/module_host_actor.rs:978-1153) — creates tx when `None` (:1017),
  wasm call (:1026-1028), OnDisconnect `delete_st_client` (:1083), view phase
  `call_views_with_tx` (:1106) gated on `tx.views_for_refresh()`
  (module_host.rs:2947 `call_views_with_tx_at`; mut_tx.rs:438) — empty ⇒ zero
  view work. Phase-5 surgery splits this fn into body-execution (ReducerTx-generic,
  worker-runnable) + commit/broadcast (home drain).
- `ViewReadSets` (mut_tx.rs:80-160): `tables: IntMap<TableId, TableReadSet>` +
  `replacements`; read sets recorded when views materialize (subscribe path,
  module_host_actor.rs:792); `views_for_refresh` early-outs via
  `has_no_views_for_table_scans`.
- Worker inputs: `InstanceEnv::new(replica_ctx: Arc<ReplicaContext>, scheduler:
  Scheduler)` (instance_env.rs:230), both cheap-clone+Send; `TxSlot` is
  `Arc<Mutex<Option<ReducerTxVariant>>>` (:94-98), `set`/`set_raw` Mut-only,
  `take` panics on Batch (:1339) — `set_batch` is the deferred addition;
  `WasmModuleHostActor.module: T::InstancePre` cheap-clone, `create_instance()`
  (module_host_actor.rs:465-476) = Store::new + instantiate + preinits + SETUP.
- Hot-swap reality: NO `AutoReplacingModuleInstance` (stale doc claim, corrected
  in §18.3); trap-driven `needs_replacement()`/`create_instance()`
  (module_host.rs:397-406); republish = `send_replace` + `old_module.exit()`
  (host_controller.rs).
- Scheduled reducers: tx pre-created by scheduler (`Some(tx)` through
  `call_reducer_with_tx`, scheduler.rs:591); happy-path schedule-row delete is a
  SEPARATE internal tx (:593 passes `tx=None`); `Some(tx)` delete sites
  (:505/:544) are error paths + the scheduled-procedure path.
- Subagent-claim correction during fact-gathering: one explorer reported the
  schedule-row delete as inside the reducer tx — source read refuted it (above).
  Verify-agent-claims discipline (HANDOFF) remains warranted.

### TASK D Phase 5 — BUILT + measured (2026-06-11)

Implemented per `design/PHASE5_PLAN.md` U1–U8 (all §18 decisions honored);
independent opus review adequate (3 MINOR findings fixed: production-`admit`
test coverage, batch-member timer_guard timing, worker thread naming);
lean-comment audit (40 kept / 32 dropped / 18 reframed; no task-refs remain).

- **Key shapes:** `BatchingExecutor` (reducer_scheduler.rs) replaces
  `SingleThreadedExecutor` for wasm hosts only (V8 untouched); only the two
  client lanes produce typed jobs — init/lifecycle/scheduled stay closures
  (admission rule partly enforced structurally). Home begins ALL batch overlays
  before dispatch (first read guard closes the admission window — reviewer
  verified the TOCTOU argument holds since read_sets mutate only under the
  committed-state write guard). Worker death recovery re-runs the lost member as
  a fresh home overlay so it drains in original FIFO position. Failed members:
  overlay rolled back at body site, event still broadcast from the drain.
- **Stats observability:** `BatchStatsSnapshot` via `WasmJob::StatsQuery`
  (oneshot through the loop — zero hot-path cost), exposed
  `ModuleHost::batch_stats()`; serves both integration tests and the kill
  criterion.
- **Release measurement (dev box, fixture workload):** calibrated threshold at
  k=4 = **246.6µs** ⇒ true fork cost ≈ **62µs** — the 53µs shadow proxy was an
  honest lower bound; §17's predicted 160–265µs threshold range confirmed at
  the k=4 midpoint. Width-2 fork fired on the first concurrent round; FIFO
  offsets + exact row counts held; trap-on-worker self-heal verified; cap=0
  verified (no spawn, no forks, all commits correct).
- **Kill criterion status:** machinery proven; the REAL-workload question
  (do width≥2 batches with members ≥ k×62µs occur at meaningful rates?) is
  unanswered by design — self-authored fixture caveat (§17 honesty note). The
  feature self-protects: lazy pool = zero threads until a heavy reducer
  appears; `STDB_REDUCER_BATCHING=0` is the hard kill switch; worker count stays 1
  pending real-workload evidence (HANDOFF §5 design call).
- **Post-build amendments (user, 2026-06-11):** (1) wildcard short-circuits
  `fork_eligible` FIRST via a new static `ReducerAccessInfo.wildcard` vec (no
  datastore resolution on the fast path; resolved-wildcard still gates `admit`);
  (2) **head-fork replaces heaviest-fork** — see the §13 supersession note for
  rationale, the lone-head inline guard, and the stop-prefix-on-view-overlap
  simplification. Re-measured after rework: threshold 258µs @ k=4 (same ~62-65µs
  fork cost), fork + FIFO + trap-recovery integration tests green (forks=10 in
  the panic-recovery rounds — head-fork exercised hard).
- **A/B benchmark (2026-06-12, release host, same binary, cap=1 vs cap=0 —
  isolates the feature):** harness `crates/testing/tests/reducer_batching_bench.rs`
  (`cargo test -p spacetimedb-testing --test reducer_batching_bench --release --
  --ignored --nocapture`, set `STDB_REDUCER_BATCHING=0` for the off arm).
  Workload: 40 rounds of concurrent disjoint pairs `heavy_a(20k)‖heavy_b(20k)`
  (debug-wasm module) + 2000 sequential `cheap_a`. Three runs per arm:
  - heavy: **38.8 ms/round (cap=1) vs 59.4 ms/round (cap=0) ⇒ 1.53× speedup**;
    39–40/40 rounds forked, all width-2; calibrated threshold ~221µs.
  - cheap fast path: 68–80µs/call (cap=1) vs 54–84µs/call (cap=0) — ranges
    overlap; **no measurable fast-path penalty** (client-roundtrip jitter
    dominates).
  - Read: 1.53× of the theoretical 2.0× width-2 ceiling; the gap is the serial
    commit drain + join + per-call client roundtrips. Demonstrates the feature
    pays on its target regime; real-workload frequency (kill criterion) still
    open.
- **Mixed representative workload (2026-06-12, user-requested):** fixture grew
  to 7 tables (a–g) + cheap_c..g; bench gained a MIXED phase — 40 rounds of
  12-job bursts `[heavy_a, c, d, e, heavy_b, f, g, c, d, e, f, g]` (2 heavy +
  10 cheap, each cheap table twice so same-table repeats exercise prefix
  truncation). Two runs/arm, release host:
  - mixed: **~48.4 ms/round (cap=1) vs ~64.1 (cap=0) ⇒ 1.32×**; 40/40 rounds
    formed width≥4–5 batches (head heavy_a forks; heavy_b + cheap members run
    as home overlays).
  - heavy pairs on pre-populated tables: ~44.9 vs ~62.8 ⇒ 1.40× (1.53× on
    empty tables — insert cost grows with index size; within-run A/B is the
    valid comparison).
- **ANALYZER BUG found + fixed en route (2026-06-12) — debug-mode all-wildcard
  cliff.** Extending the fixture past 2 tables wildcarded EVERY reducer
  (silently killing all batching; the integration test's graceful fork-skip
  masked it — the bench's machine-readable fork counts caught it). Root cause:
  name-recovery false positive in `crates/access-analysis/src/ids.rs` — the
  `Once::call_once_force` monomorphizations (one per table's `OnceLock<TableId>`)
  transitively reach the resolver, so name recovery ran on them and misread a
  trailing `(closure_data_ptr, 1)` const pair as a `(ptr,len)` string, binding
  a bogus 1-byte name (`"$"` — last byte of a rodata panic-path string). Bogus
  name ∉ ModuleDef ⇒ attribute fails ⇒ wildcard, and the wrapper sits on every
  table-op path ⇒ whole module. The "≥3 tables" cliff was a data-layout
  lottery, not a count threshold (2-table fixtures pass only because their
  pointer lands on unreadable bytes). **Fix: leaf-only name binding**
  (`keep_leaf_sites_only`): drop a recovered name on any function whose forward
  closure reaches another name-bound function — genuine leaf accessors (the
  unique fns that materialize the literal name and call the resolver) never
  reach another accessor, so every true name survives; only wrapper pickups
  die. Sound by the leaf invariant (over-approximation preserved; argument in
  the fix report). New regression test reproduces the `"$"` pickup (fails with
  the filter off); analyzer suite now 17 green incl. all exactness gates.

### Kill-criterion validation — stage 1: static batchability audit (2026-06-12)

Scope decided with user: in-repo benches only (`benchmarks`, `keynote-benchmarks`,
`perf-test`); out-of-process `batch_stats()` exposure deferred until an external
deployment workload enters scope. New diagnostic tool:
`crates/access-analysis/tests/batchability_audit.rs` (`#[ignore]`d; run with
`-- --ignored --nocapture`) — compiles each module Debug+Release, runs the
analyzer, prints per-reducer access sets, the pairwise conflict matrix
(`ConflictMatrix::build`, self-pairs included), admissible-pair lists and density.

**The first audit run exposed TWO analyzer precision bugs — same root cause,
two lookup sites.** Schema validation snake-cases identifiers into `ModuleDef`
via `convert_case` 0.6 `Case::Snake` (`v9.rs:1481`), splitting letter-digit
boundaries (`u32` → `u_32`), while the wasm keeps raw Rust identifiers:
1. **Reducer body location** (`reducers.rs`): the `<len><name>6invoke` needle is
   built from the ModuleDef name → misses any digit-bearing reducer name →
   sound-but-needless wildcard. Fix: describer-export fallback — on primary miss,
   candidate raw names = `__preinit__20_register_describer_<raw>` exports whose
   `Snake(raw) == name` (and `raw != name`); accept iff exactly one DISTINCT
   `FunctionId` across candidates, else wildcard. Sound: `find_body` anchors on
   `::<raw>::invoke`, which only reducers/procedures emit, so table/view
   describers can never wrong-bind; ambiguity always collapses to wildcard.
2. **Table attribution** (`analyze.rs` `attribute`/`table_identifier`): the raw
   accessor name recovered from the wasm const was looked up against snake-cased
   ModuleDef table names → miss → wildcard for ALL table-keyed ops (insert,
   row_count, table_scan) on digit-bearing tables. Index-keyed ops were immune
   (index `source_name` keeps the raw form). Fix: `build_accessor_to_table` map
   keyed on `TableDef.accessor_name` (preserves raw form on the V10 path,
   `rt.rs:987`; on V9 `accessor_name == name`, identity lookup, still sound),
   consulted before the canonical-name fallback.

Regression fixtures in `modules/access-analysis-fixture`: `writes_a_v2`
(digit-bearing reducer name → bug 1) and table accessor `c2` + digit-free
reducer `writes_table_two` (isolates bug 2). Integration asserts the snake-cased
keys (`writes_a_v_2`, `c_2`) exactly in Debug, tight sound-superset in Release.
Suite 22 green; clippy clean. Independent opus review round 1: inadequate
(missing genuine multi-candidate test, wrong comment rationale, loosened Release
universe, consecutive-only dedup) → fixed; round 2: adequate, no findings above
MINOR. Verified convert_case facts: `writes_aV2`/`writesA_v2` → `writes_a_v_2`
(genuine collision pair); `writesAV2` → `writes_av_2` (no collision).

**Audit numbers (post-fix).** benchmarks: Debug 0/64 wildcard, density 93.4%;
Release 2/64 wildcard — only `run_game_ia_loop` + `game_loop_enemy_ia`, lost to
release inlining (the known §16-class over-approximation, now QUANTIFIED on the
§8-regime reducers) — density 87.6%. keynote-benchmarks: Release 1 wildcard
(`init`, lifecycle). perf-test: 0 wildcard, 66.7%.

**Kill-criterion readings:**
- Real batchable structure EXISTS in shipping shape: the circles family resolves
  exactly in Release with heavy admissible pairs (`run_game_circles` ×
  `update_position_*`, `cross_join_*` × disjoint `insert_bulk_*`).
- The Debug matrix shows `run_game_circles × run_game_ia_loop` admissible
  (disjoint table families) — a true heavy×heavy pair that Release analysis
  cannot admit because inlining wildcards the ia_loop side. This is concrete
  evidence FOR Phase 6 learned sets (dynamic recovery is immune to both
  inlining and any name normalization).
- Real-world footgun (now fixed): any reducer/table name with a letter-digit
  boundary (`spawn_v2`, `tick_60hz`, `schema_v2`, `chunk16`) silently
  wildcarded — exactly the "forks=0 while pool Ready" failure class.

**New gotcha:** `perf_test_release_wasm_opt` writes fixed `/tmp` filenames —
two concurrent suite runs race (symptom: wasm-opt "unexpected end-of-file at
offset 0x0"). Rerun isolated before suspecting the tree.

**Stage 2 (next):** dynamic validation — drive circles-family mixes through the
in-process harness, read `ModuleHost::batch_stats()` widths + member runtimes
vs the calibrated threshold; then the design verdict on the data.

### Kill-criterion validation — stage 2: dynamic circles bench (2026-06-12)

New harness `crates/testing/tests/circles_batching_bench.rs` (`#[ignore]`d,
mirrors `reducer_batching_bench` idioms; off-arm via `STDB_REDUCER_BATCHING=0`)
driving the REAL `benchmarks` module compiled **Release** (shipping-shape
analyzer sets gate admission). Seed: entity/circle/food 100 each (so
`run_game_circles` ≈ 10⁶ pure-read inner iterations ≈ 35ms/call), position/
velocity 30k (update-in-place, no table growth anywhere — per-round cost
stable). Phases (40 rounds each): HETERO `run_game_circles ‖
update_position_with_velocity` (the audit's heavy×heavy admissible pair), HOMO
`run_game_circles ‖ run_game_circles` (pure-reader self-pair = many-clients-
one-reducer regime), MIXED 5-call burst with `update_position_all` as the
deliberate write-write truncation point. Opus review: adequate. Sonnet codegen,
no escalation needed.

**A/B results (cap=1 vs cap=0, single machine, run by the main session):**
- HETERO: 73.9 vs 104.0 ms/round ⇒ **1.41×** (ceiling 1.49× — long pole is
  update_position_with_velocity ≈ 70ms); 39/40 width-2 forks.
- HOMO: 37.9 vs 70.6 ms/round ⇒ **1.86×** of the 2.0× ceiling — pair time ≈
  single-call time + ~2.6ms; read∩read parallelism nearly ideal; 39/40 width-2.
- MIXED: 135.1 vs 188.4 ms/round ⇒ **1.39×**; width-3 batches in 38/40 rounds
  (truncation at the position write-write conflict, exactly as designed).
- Totals: forks=157, widths=[0,119,38,0,0], calibrated_ns≈258µs — members run
  ~135× the calibrated threshold. Off arm: pool Off, forks=0, control clean.

**Kill-criterion reading:** on a real game-shaped module NOT written for
batching, width≥2 batches whose members clear k×fork-cost by two orders of
magnitude form in essentially every concurrent round, yielding 1.39–1.86×.
The structural and cost conditions of the criterion are now demonstrated
outside the synthetic fixture. What the in-repo tier cannot show is
deployment-side arrival concurrency (whether real client traffic presents
concurrent admissible calls); that remains the residual open question for
production observation via `batch_stats()` (HANDOFF §5).

### Kill-criterion VERDICT (user, 2026-06-12) — ACCEPTED; Phase 6 before PR

Decision on the stage 1+2 data: the kill criterion is satisfied at the
in-repo tier (machinery + cost conditions proven on real game-shaped code;
synthetic 1.53×/1.32×, real-module 1.41×/1.86×/1.39×). Posture: ship
dormant-by-default (cap=1 + threshold gate self-arms only when traffic earns
it; zero measured all-cheap tax); deployment arrival concurrency continues to
be observed post-merge via `batch_stats()` — the criterion is honored, not
bypassed. **Sequencing decision: build Phase 6 (learned sets + abort/retry
trap, §18.1 shape) BEFORE upstream PR prep**, so the initial upstream offering
lifts the two big admission exclusions (release-inlining/wildcard reducers
incl. non-Rust modules, view-overlap writers). Evidence motivating Phase 6:
the named lost pair `run_game_circles × run_game_ia_loop` — admissible in
Debug, killed in Release by inlining wildcard (stage-1 log). Phase 6 is
design-heavy: grill forks with the user before any codegen.

### Phase 6 design grilled (2026-06-12) — eight forks DECIDED, plan approved

Design session, no code. Decisions recorded in §19; approved unit plan in
`design/PHASE6_PLAN.md` (U1–U8). Source facts established this session:

- **Capture machinery is fully dormant and ready:** the `record_*` reducer
  arms + `enable_access_capture`/`take_observed` exist on BOTH `MutTxId`
  (mut_tx.rs:399-440) and `BatchTxState` (batch_tx.rs:243-302), exposed via
  the `ReducerTx` trait (reducer_tx.rs:103-104) — zero production callers
  since the shadow-harness deletion. Phase 6 adds call sites only.
- **`ObservedAccess = { reads, writes: IntSet<TableId> }`** (mut_tx.rs:178) —
  capture is TableId-keyed; learned sets bypass name resolution entirely
  (immune to release inlining AND the convert_case digit-boundary class).
- **Dropping `FinishedBatchTx` without commit is a clean abort** — the worker
  calibration path already does it ("never advances the tx offset"); the read
  guard is released at `finish()` on the worker, so nothing is held at drain.
- **Sequences need nothing across retry** (§7): draws are non-transactional,
  never reclaimed — an aborted overlay's autoinc draws leak as documented
  gaps; the rerun draws fresh.
- **The view-lift gap is table-level vs key-level:** admission's
  `view_read_overlap` is table-level; `views_for_refresh` (mut_tx.rs:443) is
  key-precise, and committed view read sets already distinguish full-table-scan
  entries from index-key entries — exactly the speculation boundary §19.6 uses.
- **Trap-check stability:** `ViewReadSets`/read sets mutate only on the
  executor thread under the write guard; the executor thread is inside
  `run_batch` for the whole check-to-drain window (standing §18.1 [VERIFY]
  re-confirmed at build).
- **Requeue mechanism already exists:** the per-member view-overlap stop
  (reducer_scheduler.rs:695-701) pushes the member back to the front of
  `pending` — early-cut reuses this; front-of-pending IS the member's serial
  FIFO slot.
- **Members execute sequentially on home during batch build** (admit → overlay
  → check → execute, one at a time) — the structural fact that makes early-cut
  checks (user's design) strictly simpler than drain-time cascades: later
  members haven't run when a violation is detected.

Build-time [VERIFY] items carried into the plan: energy debit site
(charge-committed-only feasibility); typed reducer lane never carries
pre-created-tx calls; `views_for_refresh` read-guard port parity (must-trap
test — under-detection is the corruption case); no off-executor-thread
read-set mutation.

### Phase 6 BUILT (2026-06-12) — learned sets + early-cut trap, review adequate

Executed `design/PHASE6_PLAN.md` U1–U8 via dev-time tiered codegen with
per-unit test gates and an independent opus review of the full diff. **Review
verdict: adequate — zero CRITICAL, zero MAJOR**; every soundness lever the
plan flagged as a corruption surface was traced and confirmed closed. Two
cosmetic MINORs fixed post-review (stale `#[allow(dead_code)]` on
`ResolvedAccess::read_tables`; the fixture's call_indirect comment now states
the wildcard is Debug-only — Release inlines the constant index).

The four build-time [VERIFY] items, RESOLVED:
- **Energy debit site → charge-committed-only is clean (§19.5).** `call_function`
  gained a `record_energy: bool`; batch bodies pass `false` (skip
  `record_reducer`); the drain bills only committed outcomes via a new
  `record_reducer_energy` whose `FunctionFingerprint` matches `call_function`'s.
  Requeued/aborted runs bill nothing; their eventual committed inline rerun
  bills once. No un-billing surgery was needed — un-recording is just not
  recording, so nothing invasive surfaced.
- **Typed reducer lane never carries pre-created-tx calls — re-confirmed.**
  `call_reducer_with_tx` asserts `tx.is_none() || !capture_access`; capture is
  set true only in `run_inline` on the closure lane (`tx: None`). Scheduled /
  connect / v8 paths carry `capture_access: false`.
- **`views_for_refresh` read-guard port parity — must-trap test green.** The
  port is the free fn `view_refresh_required(committed, tx_state)` (single
  implementation; `view_refresh_nonempty` delegates), replicating BOTH branches
  (full-table-scan + index-seek, delete and insert sides) over the read guard.
  The datastore batch-commit debug_assert now calls it (key-level, replacing the
  table-level `view_read_overlap`). `view_refresh_parity_mut_vs_batch` asserts
  batch ⇔ mut across write patterns and flags `batch=false, mut=true` as the
  corruption direction.
- **No off-executor-thread read-set mutation — held.** Capture writes only to
  the tx-local `ObservedAccess`; head capture returns by value in
  `BatchRunReply`; `ViewReadSets` still mutate only on the executor thread under
  the write guard, which is inside `run_batch` for the whole check-to-drain
  window.

One soundness refinement beyond the literal plan, surfaced during U4 self-review
and confirmed by the U8 review: in `tier_conflicts`, a resolved-wildcard Static
side (analyzer-OK but a table name failed to resolve ⇒ empty resolved slices,
unknown true access) is treated as **universal-conflict**, never set-intersected
against its empty slices — both candidate and admitted sides guarded, with the
candidate-side `admit` precheck rejecting first (defense in depth). Test
`tier_admit_learned_vs_unresolved_static_rejected`.

End-to-end evidence the trap fires for real (not just unit-level): the U6
`trap_demotes_and_stays_correct` integration test observed `head_traps=1,
demotions=1` when `heavy_learn` (learned set `{table_a}`) escaped into `table_b`
after the flag flipped, with the requeued run's committed effect equal to serial
execution. The U7 learned bench promoted the Release-wildcard `run_game_ia_loop`
at run 8 and produced 20/20 width-2 forks paired with `run_game_circles` at
cap=1 (forks=0 at cap=0) — the verdict-named lost pair, now batchable via
learning. Speedup was modest (~5.7%) because the two bodies are cost-matched;
the load-bearing result is that the learned path forks where the static path
could not. Suites at hand-off: datastore 94 (`--features test`),
access-analysis 17, core --lib 242, batching test 5 + off 1; clippy clean.

Deviation (sanctioned by the plan's U8 note): the view-overlap **end-to-end**
integration test was deferred — the testing harness has no ergonomic
subscription helper and the corruption-critical integrity is already gated at
the datastore level (the parity must-trap test) plus U5's view-trap unit tests.
The lift's economics (KeyOnly speculation) and the abort path are exercised by
unit tests; only the full subscribe→speculate→refresh round-trip is unproven
in-tree.

### Cross-language benchmarks (2026-06-13) — base vs v1 vs v2, Rust + C#; TS N/A

Measurement-only (no engine changes). All numbers are single-run point
estimates on one machine; the qualitative signals (forks vs no-forks, width
histograms) are the robust part. base = `STDB_REDUCER_BATCHING=0` (pool Off,
synchronous #5095 lane); v1 = cap=1, static-only batching; v2 = cap=1, learned.

**Rust three-way** (`crates/testing/tests/circles_batching_bench.rs::bench_three_way`,
Release `benchmarks` module, run under cap=0 and cap=1):
- **Static pair** `run_game_circles ‖ update_position_with_velocity`:
  base 105.6 ms (forks 0) → v1 71.8 ms (39/39 width-2) → v2 71.8 ms (≡ v1).
  **1.47×**; v1 ≡ v2 confirms Phase 6 is byte-equivalent / zero-overhead on the
  all-Static path.
- **Lost pair** `run_game_circles ‖ run_game_ia_loop` (ia_loop is Release-wildcard
  via inlining): base 39.2 ms (forks 0) → v1 38.2 ms (**forks 0** — wildcard
  excluded, ≈ base) → v2 36.1 ms (**20/20 width-2**, ia_loop promoted at run 8).
  v1 cannot batch it at all; v2 learns it. The speedup is small (1.09×) because
  the pair is cost-imbalanced (circles ≫ ia_loop); the win is qualitative
  (forks 0 → 20).

**C# base vs v2** (C# has NO static path — analyzer wildcards all C# wasm — so
v1 ≡ base; all batching is learning):
- `crates/testing/tests/csharp_mixed_bench.rs::bench_csharp_mixed` on a C# port
  of the synthetic mixed-burst fixture (`modules/reducer-batching-fixture-cs`,
  7 tables, 2 heavy + 10 cheap burst): **base 28.6 ms (forks 0) → v2 18.2 ms
  (21 forks, width-5 in 20/21 batches) = 1.57×.** All 7 reducers promoted at
  run 8. Larger relative win than Rust's static path because Mono's per-call
  overhead makes body overlap save relatively more.
- `crates/testing/tests/csharp_batching_bench.rs::bench_csharp_base_v2` on the
  C# circles/ia_loop pairs: forks fire (19-20/20 width-2) but ~1.00× wall —
  every natural circles pair is cost-imbalanced (solo: circles 1672 ms, ia_loop
  81 ms, upv 109 ms), so overlap is hidden under circles. Kept as the
  imbalance-lesson exhibit; the mixed-burst bench is the clean C# demonstration.

**TypeScript: N/A.** V8ModuleHost never reaches `BatchingExecutor` (§19.7
corrected); `batch_stats()` is `None`, dispatch bypasses the batch lane. base ≡
v2 by construction — nothing to measure until V8 adopts a fork scheduler.

Toolchain note: C# wasm requires .NET SDK 8.0 + the `wasi-experimental`
workload (`dotnet publish` → `bin/<cfg>/net8.0/wasi-wasm/AppBundle/StdbModule.wasm`);
the testing harness builds it via `spacetimedb_cli::build` (language-agnostic).
New artifacts (measurement-only, no engine change): `bench_three_way`,
`csharp_batching_bench.rs`, `csharp_mixed_bench.rs`, `modules/reducer-batching-fixture-cs/`.

### Keynote-2 network benchmark + tail-cost gate (2026-06-19)

Ran the `templates/keynote-2` fund-transfer benchmark (real network + WS +
confirmed-reads, the flagship contention harness) against a workspace server,
toggling batching off vs on (the env is `STDB_REDUCER_BATCHING`; 0 = off baseline,
default on — at the time this knob was the numeric `STDB_REDUCER_POOL_CAP`, since
collapsed to a boolean, see §19.10).
Built Rust + TS modules for several workloads; server + publish/seed via the
workspace `spacetimedb-cli`/`-standalone` (not the stale installed binary).

**Executor shape confirmed (load-bearing for everything below):** v1 honors
**only 1 worker** (`reducer_scheduler.rs` "only 1 honored in v1"). Per batch the
**head runs on that worker, members run on the home thread sequentially**, then
commits **drain FIFO (head first)**. Consequences:
- Per-batch parallelism is ~2-way; **width-2 (one head + one member) is the
  sweet spot** (`W/(W-1)` saving → 2× at W=2, 1.25× at W=5). Wider batches pile
  members onto the home thread, so K=5 underperformed K=2.
- Batches are **serial** (loop blocks at drain on the head) — no cross-batch
  pipelining of the offloaded head.
- FIFO drain **yokes member acks to the head**: a cheap member batched behind a
  heavy head can't ack until the heavy finishes (latency, not decoupling).

**Batching efficacy by workload (relative TPS, same binary, cap 0 vs 1):**
- Single-table `transfer` (the actual keynote workload): self-conflicts on the
  one `accounts` table ⇒ unbatchable ⇒ TS ≈ Rust base ≈ Rust batching (commit/
  WS-latency-bound). Batching is **regression-safe** here (≈ flat), not a speedup.
- Batching needs reducers that are **both** heavy (> fork threshold) **and**
  access-disjoint **and** a deep enough queue (pipelining) to pair them; then
  width-2 heavy pairs hit **~1.66–1.73×**.

**Tail-cost gate (§19.9) decision data** — keynote workloads, seeded + 5-run
avg (cv < 1%), vs the lone-head guard:

| workload | batching no-gate | + full-threshold gate |
|---|---:|---:|
| cheap-tail mix | +1.1% | **+6.4%** |
| medium members | +6.3% | **+7.1%** |
| tri-class (10% heavy/30% med/60% cheap) | +11.4% | **+15.1%** |
| width-2 balanced heavy | 1.56× | **1.66×** |

Bar sweep (`MULT×fork_cost` for M∈{1,2,4}, head-relative `0.75×(head+fork_cost)`)
confirmed the **full threshold** (`tail_est >= threshold`) wins or ties and is
simplest; aggressive bars over-fork because real per-member overhead > calibrated
`fork_cost`. Knobs removed; gate is unconditional. Code: `record_width`'s
`STDB_BATCH_LOG` diagnostic + the Step-2 tail-cost guard in `run_batch`.

Bench artifacts (measurement-only, in `templates/keynote-2/`): modules
`rust_module_sharded`, `spacetimedb_sharded`, `rust_module_mixed`,
`spacetimedb_mixed`, `rust_module_medium`, `rust_module_tri`; the `STDB_MIX` /
`STDB_SHARDS` / seeded-PRNG connector path in `src/connectors/spacetimedb.ts`;
runner scripts `run_bench_matrix.sh` + `verify_*.sh`.
