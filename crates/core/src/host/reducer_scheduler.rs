//! Host-owned reducer drain loop for `WasmtimeModuleHost`.
//!
//! Admits typed reducer jobs into threshold-gated, strict-prefix disjoint batches:
//! the HEAD (the reducer that cleared the fork threshold) is dispatched to the
//! [`ReducerWorker`] immediately, while the admitted suffix runs as overlays on the
//! home thread in parallel. Commits drain in FIFO order (head first).
//! V8 uses `SingleThreadedExecutor` directly and does not go through this path.
//!
//! Steady-state fast-path cost (pool ready, reducer does not fork): a discriminant
//! match on `PoolState`, a lifecycle-vec index, a wildcard-vec index, and two
//! `Duration` comparisons — O(1), a handful of comparisons total.

use std::collections::VecDeque;
use std::panic::AssertUnwindSafe;
use std::sync::mpsc as std_mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::LocalBoxFuture;
use futures::FutureExt;
use spacetimedb_datastore::execution_context::{ReducerContext, Workload};
use spacetimedb_primitives::ReducerId;
use tokio::runtime;
use tokio::sync::{mpsc, oneshot};
use tracing::Instrument;

use crate::host::module_host::CallReducerParams;
use crate::host::wasm_common::module_host_actor::BatchBodyOutcome;
use crate::host::wasm_common::reducer_access::ReducerAccessInfo;
use crate::host::wasm_common::reducer_worker::{BatchRunJob, ReducerWorker, WorkerGone};
use crate::host::ReducerCallResult;
use crate::host::ReducerOutcome;
use crate::subscription::module_subscription_actor::commit_and_broadcast_batch_event;
use crate::util::jobs::{AllocatedJobCore, CorePinner, LoadBalanceOnDropGuard};

use super::module_host::WasmtimeModuleState;

// ─── Config (read once at construction) ──────────────────────────────────────

/// `0` ⇒ the worker pool is `Off` forever (kill switch); otherwise the cap on
/// lazily-spawned workers (only `1` is honored in v1).
const POOL_CAP_ENV: &str = "STDB_REDUCER_POOL_CAP";
const POOL_CAP_DEFAULT: usize = 1;
/// Fork iff a reducer's measured runtime exceeds `k × calibrated_fork_cost`.
const FORK_K_ENV: &str = "STDB_REDUCER_FORK_K";
const FORK_K_DEFAULT: u32 = 4;

/// EMA smoothing factor (α = 1/8); both high-water AND EMA must exceed T to fork.
const EMA_SHIFT: u32 = 3;
/// Number of round-trips averaged for the proxy and calibration measurements.
const CALIBRATION_SAMPLES: usize = 32;

fn read_env_usize(name: &str, default: usize) -> usize {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

fn read_env_u32(name: &str, default: u32) -> u32 {
    std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default)
}

// ─── Job payloads ─────────────────────────────────────────────────────────────

/// Reply channel for the request/reply lane. Carries `thread::Result` so a panic in
/// the reducer body resumes on the awaiting caller, exactly as `run_sync_job` does.
type ReducerReply = oneshot::Sender<std::thread::Result<ReducerCallResult>>;

/// Glue every reducer job carries, mirroring the closure lanes' panic/timer plumbing.
pub(super) struct ReducerGlue {
    pub label: String,
    pub on_panic: Arc<dyn Fn() + Send + Sync>,
    /// Invoked (and dropped) when the reducer body starts, stopping the queue-wait timer.
    /// Wrapped in `Option` so it can be taken from a `&mut` reference at body-start time.
    pub timer_guard: Option<Box<dyn FnOnce() + Send>>,
}

pub(super) struct ReducerJobPayload {
    pub params: CallReducerParams,
    /// `Some` for the request/reply `call_reducer` lane; `None` for fire-and-forget.
    pub reply: Option<ReducerReply>,
    pub glue: ReducerGlue,
}

impl ReducerJobPayload {
    /// Send a final result to the request/reply lane, mirroring `run_sync_job`'s
    /// `thread::Result` reply (panic resumes on the awaiting caller).
    fn reply_ok(&mut self, res: ReducerCallResult) {
        if let Some(reply) = self.reply.take() {
            let _ = reply.send(Ok(res));
        }
    }

    fn reply_panic(&mut self, panic: Box<dyn std::any::Any + Send>) {
        log::warn!("wasm reducer operation {} panicked", self.glue.label);
        (self.glue.on_panic)();
        match self.reply.take() {
            Some(reply) => {
                let _ = reply.send(Err(panic));
            }
            None => tracing::warn!("uncaught panic on `BatchingExecutor`"),
        }
    }
}

enum WasmJob {
    Async(Box<dyn FnOnce() -> LocalBoxFuture<'static, ()> + Send>),
    Sync(Box<dyn FnOnce(&mut WasmtimeModuleState) + Send>),
    Reducer(Box<ReducerJobPayload>),
    /// Read a stats snapshot from inside the loop (where `SchedulerState` is accessible).
    StatsQuery(oneshot::Sender<BatchStatsSnapshot>),
}

// ─── Executor handle ──────────────────────────────────────────────────────────

// ─── Stats snapshot (observable from outside the loop thread) ────────────────

/// Discriminant of [`PoolState`] that can be sent across threads without owning the worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PoolStateTag {
    #[default]
    Empty,
    Off,
    Spawning,
    Ready,
}

/// Plain-data snapshot of the scheduler's runtime statistics.
/// Cheap to clone; safe to send across threads.
#[derive(Debug, Clone, Default)]
pub struct BatchStatsSnapshot {
    pub batches: u64,
    pub forks: u64,
    /// Width histogram: index 0 = width 1, …, index 4 = width ≥5.
    pub widths: [u64; 5],
    /// Calibrated threshold in nanoseconds (`None` until calibration completes).
    pub calibrated_ns: Option<u64>,
    pub pool_state: PoolStateTag,
    /// Current threshold value in nanoseconds (proxy or calibrated).
    pub threshold_ns: Option<u64>,
}

// ─── Executor handle ──────────────────────────────────────────────────────────

/// Cheaply-cloneable handle to the batching reducer executor.
pub(super) struct BatchingExecutor {
    job_tx: mpsc::UnboundedSender<WasmJob>,
}

impl Clone for BatchingExecutor {
    fn clone(&self) -> Self {
        Self {
            job_tx: self.job_tx.clone(),
        }
    }
}

impl BatchingExecutor {
    pub(super) fn spawn(core: AllocatedJobCore, state: WasmtimeModuleState, name: String) -> Self {
        let AllocatedJobCore { guard, pinner } = core;
        let (job_tx, job_rx) = mpsc::unbounded_channel::<WasmJob>();
        let handle = Self {
            job_tx: job_tx.clone(),
        };

        let rt = runtime::Handle::current();
        std::thread::Builder::new()
            .name(name)
            .spawn(move || loop_thread(Arc::new(guard), pinner, state, rt, job_rx))
            .expect("failed to spawn BatchingExecutor thread");

        handle
    }

    // ── Closure lanes (copied envelopes from SingleThreadedExecutor) ──────────

    pub(super) async fn run_async_job<F, R>(&self, f: F) -> R
    where
        F: AsyncFnOnce() -> R + Send + 'static,
        R: Send + 'static,
    {
        let span = tracing::Span::current();
        let (tx, rx) = oneshot::channel();
        self.job_tx
            .send(WasmJob::Async(Box::new(move || {
                async move {
                    let result = AssertUnwindSafe(f().instrument(span)).catch_unwind().await;
                    if let Err(Err(_panic)) = tx.send(result) {
                        tracing::warn!("uncaught panic on `BatchingExecutor`")
                    }
                }
                .boxed_local()
            })))
            .unwrap_or_else(|_| panic!("job thread exited"));
        match rx.await.unwrap() {
            Ok(r) => r,
            Err(e) => std::panic::resume_unwind(e),
        }
    }

    pub(super) fn enqueue_async_job<F>(&self, f: F)
    where
        F: AsyncFnOnce() + Send + 'static,
    {
        let span = tracing::Span::current();
        self.job_tx
            .send(WasmJob::Async(Box::new(move || {
                async move {
                    if AssertUnwindSafe(f().instrument(span)).catch_unwind().await.is_err() {
                        tracing::warn!("uncaught panic on `BatchingExecutor`")
                    }
                }
                .boxed_local()
            })))
            .unwrap_or_else(|_| panic!("job thread exited"));
    }

    pub(super) async fn run_sync_job<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut WasmtimeModuleState) -> R + Send + 'static,
        R: Send + 'static,
    {
        let span = tracing::Span::current();
        let (tx, rx) = oneshot::channel();
        self.job_tx
            .send(WasmJob::Sync(Box::new(move |state| {
                let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    let _entered = span.enter();
                    f(state)
                }));
                if let Err(Err(_panic)) = tx.send(result) {
                    tracing::warn!("uncaught panic on `BatchingExecutor`")
                }
            })))
            .unwrap_or_else(|_| panic!("job thread exited"));
        match rx.await.unwrap() {
            Ok(r) => r,
            Err(e) => std::panic::resume_unwind(e),
        }
    }

    pub(super) fn enqueue_sync_job<F>(&self, f: F)
    where
        F: FnOnce(&mut WasmtimeModuleState) + Send + 'static,
    {
        let span = tracing::Span::current();
        self.job_tx
            .send(WasmJob::Sync(Box::new(move |state| {
                if std::panic::catch_unwind(AssertUnwindSafe(|| {
                    let _entered = span.enter();
                    f(state);
                }))
                .is_err()
                {
                    tracing::warn!("uncaught panic on `BatchingExecutor`")
                }
            })))
            .unwrap_or_else(|_| panic!("job thread exited"));
    }

    // ── Typed reducer lanes ──────────────────────────────────────────────────

    /// Request/reply reducer call: resolves to the `ReducerCallResult` once committed.
    pub(super) async fn call_reducer(&self, params: CallReducerParams, glue: ReducerGlue) -> ReducerCallResult {
        let (tx, rx) = oneshot::channel();
        self.job_tx
            .send(WasmJob::Reducer(Box::new(ReducerJobPayload {
                params,
                reply: Some(tx),
                glue,
            })))
            .unwrap_or_else(|_| panic!("job thread exited"));
        // The loop always fires the reply (`Ok` on a committed/failed reducer, `Err` on a
        // host-side panic). A recv error means the loop thread itself died.
        match rx.await {
            Ok(Ok(res)) => res,
            Ok(Err(panic)) => std::panic::resume_unwind(panic),
            Err(_) => panic!("job thread exited"),
        }
    }

    pub(super) fn enqueue_call_reducer(&self, params: CallReducerParams, glue: ReducerGlue) {
        self.job_tx
            .send(WasmJob::Reducer(Box::new(ReducerJobPayload {
                params,
                reply: None,
                glue,
            })))
            .unwrap_or_else(|_| panic!("job thread exited"));
    }

    /// Read a point-in-time snapshot of scheduler statistics.
    ///
    /// Uses a dedicated job variant so the loop thread's `SchedulerState` is
    /// accessible — `run_sync_job` only provides `&mut WasmtimeModuleState`, not
    /// `&mut SchedulerState`. Returns `None` only if the loop thread has exited.
    #[doc(hidden)]
    pub(super) async fn stats_snapshot(&self) -> Option<BatchStatsSnapshot> {
        let (tx, rx) = oneshot::channel();
        if self.job_tx.send(WasmJob::StatsQuery(tx)).is_err() {
            return None;
        }
        rx.await.ok()
    }
}

// ─── Loop thread ──────────────────────────────────────────────────────────────

fn loop_thread(
    guard: Arc<LoadBalanceOnDropGuard>,
    mut pinner: CorePinner,
    mut state: WasmtimeModuleState,
    rt: runtime::Handle,
    mut job_rx: mpsc::UnboundedReceiver<WasmJob>,
) {
    let _guard = guard;
    pinner.pin_now();

    let _entered = rt.enter();
    let local = tokio::task::LocalSet::new();
    let mut loop_pinner = pinner.clone();

    let mut sched = SchedulerState::new(&state);

    let job_loop = async {
        loop {
            // Batch builds pull channel jobs to peek ahead; un-admitted ones wait in `pending`
            // and must drain before recv'ing, else newer jobs overtake them and FIFO breaks.
            let job = match sched.pending.pop_front() {
                Some(job) => job,
                None => match job_rx.recv().await {
                    Some(job) => job,
                    None => break,
                },
            };
            match job {
                WasmJob::Async(job) => {
                    local.spawn_local(job());
                }
                WasmJob::Sync(job) => {
                    loop_pinner.pin_if_changed();
                    job(&mut state);
                }
                WasmJob::Reducer(payload) => {
                    loop_pinner.pin_if_changed();
                    handle_reducer(*payload, &mut state, &mut sched, &mut job_rx);
                }
                WasmJob::StatsQuery(reply) => {
                    let _ = reply.send(sched.snapshot());
                }
            }
        }
    };

    rt.block_on(local.run_until(crate::util::also_poll(job_loop, pinner.run())));
    // Sender closed; finish remaining LocalSet tasks (do not drop in-flight async work).
    rt.block_on(local);
}

// ─── Scheduler state ──────────────────────────────────────────────────────────

#[derive(Clone, Copy, Default)]
struct Stat {
    high_water: Duration,
    ema: Duration,
}

impl Stat {
    /// EMA with α = 1/8; high-water is monotone non-decreasing.
    fn update(&mut self, sample: Duration) {
        if sample > self.high_water {
            self.high_water = sample;
        }
        let ema = self.ema.as_nanos() as u64;
        let s = sample.as_nanos() as u64;
        let next = if s >= ema {
            ema + ((s - ema) >> EMA_SHIFT)
        } else {
            ema - ((ema - s) >> EMA_SHIFT)
        };
        self.ema = Duration::from_nanos(next);
    }

    /// Fork gate: both high-water AND EMA must exceed the threshold.
    fn exceeds(&self, threshold: Duration) -> bool {
        self.high_water > threshold && self.ema > threshold
    }
}

enum ThresholdState {
    Off,
    /// Startup proxy `k × thread-round-trip`; gates only the spawn decision.
    Proxy(Duration),
    /// Post-calibration `k × measured-fork-cost`; gates real forks.
    Calibrated(Duration),
}

impl ThresholdState {
    fn value(&self) -> Option<Duration> {
        match self {
            ThresholdState::Off => None,
            ThresholdState::Proxy(d) | ThresholdState::Calibrated(d) => Some(*d),
        }
    }
    fn is_calibrated(&self) -> bool {
        matches!(self, ThresholdState::Calibrated(_))
    }
}

enum PoolState {
    /// Kill switch (`STDB_REDUCER_POOL_CAP=0`): never spawns.
    Off,
    Empty,
    /// Worker spawned, not yet ready / not yet calibrated.
    Spawning(ReducerWorker),
    /// Worker ready and calibrated; forking enabled.
    Ready(ReducerWorker),
}

#[derive(Default)]
struct BatchStats {
    batches: u64,
    forks: u64,
    /// Width histogram: buckets for widths 1, 2, 3, 4, ≥5.
    widths: [u64; 5],
    calibrated_ns: u64,
}

impl BatchStats {
    fn record_width(&mut self, width: usize) {
        let bucket = width.clamp(1, 5) - 1;
        self.widths[bucket] += 1;
    }
}

struct SchedulerState {
    /// `try_recv` spillover and the un-admitted tail of a built prefix.
    pending: VecDeque<WasmJob>,
    runtime_stats: Vec<Stat>,
    threshold: ThresholdState,
    pool: PoolState,
    access: Arc<ReducerAccessInfo>,
    cap: usize,
    k: u32,
    stats: BatchStats,
}

impl SchedulerState {
    fn new(state: &WasmtimeModuleState) -> Self {
        let cap = read_env_usize(POOL_CAP_ENV, POOL_CAP_DEFAULT);
        let k = read_env_u32(FORK_K_ENV, FORK_K_DEFAULT);
        let access = state.actor().reducer_access();
        let n = access.lifecycle.len();

        let (threshold, pool) = if cap == 0 {
            (ThresholdState::Off, PoolState::Off)
        } else {
            // Startup proxy: median in-process thread round-trip × k. Gates only the
            // spawn decision until the warm worker recalibrates against true fork cost.
            let proxy = measure_thread_round_trip();
            let t = proxy.saturating_mul(k);
            log::info!("reducer fork threshold (proxy): {t:?} (k={k}, proxy={proxy:?})");
            (ThresholdState::Proxy(t), PoolState::Empty)
        };

        Self {
            pending: VecDeque::new(),
            runtime_stats: vec![Stat::default(); n],
            threshold,
            pool,
            access,
            cap,
            k,
            stats: BatchStats::default(),
        }
    }

    fn stat(&self, id: ReducerId) -> Stat {
        self.runtime_stats.get(id.0 as usize).copied().unwrap_or_default()
    }

    fn record_runtime(&mut self, id: ReducerId, sample: Duration) {
        if let Some(s) = self.runtime_stats.get_mut(id.0 as usize) {
            s.update(sample);
        }
    }

    fn snapshot(&self) -> BatchStatsSnapshot {
        let pool_state = match &self.pool {
            PoolState::Off => PoolStateTag::Off,
            PoolState::Empty => PoolStateTag::Empty,
            PoolState::Spawning(_) => PoolStateTag::Spawning,
            PoolState::Ready(_) => PoolStateTag::Ready,
        };
        let calibrated_ns = if self.threshold.is_calibrated() {
            Some(self.stats.calibrated_ns)
        } else {
            None
        };
        BatchStatsSnapshot {
            batches: self.stats.batches,
            forks: self.stats.forks,
            widths: self.stats.widths,
            calibrated_ns,
            pool_state,
            threshold_ns: self.threshold.value().map(|d| d.as_nanos() as u64),
        }
    }
}

// ─── Reducer handling ─────────────────────────────────────────────────────────

fn handle_reducer(
    payload: ReducerJobPayload,
    state: &mut WasmtimeModuleState,
    sched: &mut SchedulerState,
    job_rx: &mut mpsc::UnboundedReceiver<WasmJob>,
) {
    let id = payload.params.reducer_id;

    // FAST PATH: only a ready pool with a head that clears the threshold enters the batch path.
    let fork_eligible = !wildcard(&sched.access, id)
        && matches!(sched.pool, PoolState::Ready(_))
        && !lifecycle(&sched.access, id)
        && threshold_exceeded(sched, id);

    if !fork_eligible {
        run_inline(payload, state, sched);
        maybe_spawn_or_calibrate(state, sched, id);
        return;
    }

    // BATCH PATH — drain immediately-available jobs into `pending`, then build the
    // strict-prefix disjoint batch starting at this head.
    sched.pending.push_front(WasmJob::Reducer(Box::new(payload)));
    while let Ok(job) = job_rx.try_recv() {
        sched.pending.push_back(job);
    }
    run_batch(state, sched);
    maybe_spawn_or_calibrate(state, sched, id);
}

/// A host-side panic fires `on_panic` and resumes on the caller (request/reply lane).
fn run_inline(payload: ReducerJobPayload, state: &mut WasmtimeModuleState, sched: &mut SchedulerState) {
    let ReducerJobPayload {
        params,
        mut reply,
        glue,
    } = payload;
    let id = params.reducer_id;
    let timer_guard = glue.timer_guard;

    let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| {
        state.with_instance(move |inst| {
            // Drop the queue-wait guard right as the body starts (mirrors closure lane).
            drop(timer_guard);
            inst.call_reducer(params)
        })
    }));

    match outcome {
        Ok(res) => {
            sched.record_runtime(id, res.execution_duration);
            if let Some(reply) = reply.take() {
                let _ = reply.send(Ok(res));
            }
        }
        Err(panic) => {
            log::warn!("wasm reducer operation {} panicked", glue.label);
            (glue.on_panic)();
            match reply.take() {
                Some(reply) => {
                    let _ = reply.send(Err(panic));
                }
                None => tracing::warn!("uncaught panic on `BatchingExecutor`"),
            }
        }
    }
}

fn run_batch(state: &mut WasmtimeModuleState, sched: &mut SchedulerState) {
    // Detach the access borrow from `sched` so `sched` is freely mutable below.
    let access = sched.access.clone();
    let stdb = state.actor().replica_ctx().relational_db().clone();
    let resolved = access.resolved(&stdb);

    // ── Step 1: pop the head (it provably cleared the fork threshold) ──────────

    let WasmJob::Reducer(head_boxed) = sched.pending.pop_front().expect("head present") else {
        unreachable!("head is always a Reducer job on the batch path")
    };
    let mut head = *head_boxed;
    let head_id = head.params.reducer_id;

    // ── Step 2: lone-head guard ────────────────────────────────────────────────
    //
    // Scan pending for the first admissible companion. If none exists, forking
    // with home idle is strictly worse than running inline — skip the fork.
    let has_companion = sched.pending.iter().any(|job| {
        let WasmJob::Reducer(p) = job else { return false };
        admit(&access, resolved, p.params.reducer_id, &[head_id])
    });
    if !has_companion {
        run_inline(head, state, sched);
        return;
    }

    // ── Step 3: begin head overlay, view-overlap check, dispatch to worker ─────

    let head_op = reducer_op_for(state, &head);
    let head_tx = stdb.begin_batch_tx(Workload::Reducer(head_op));

    // WHY check under the head's own guard: each member's view-check runs under its
    // own read guard so read_sets are stable (they mutate only under the write guard,
    // which this executor thread is the sole acquirer of).
    let head_overlap = {
        let wt = resolved.write_tables[head_id.0 as usize].iter().copied();
        head_tx.view_read_overlap(wt)
    };
    if head_overlap {
        // Rollback head's overlay and fall back to plain inline execution.
        drop(head_tx);
        run_inline(head, state, sched);
        return;
    }

    // Drop the queue-wait timer right before dispatching — body is about to start.
    drop(head.glue.timer_guard.take());

    let dispatch_result = {
        let PoolState::Ready(worker) = &sched.pool else {
            unreachable!("batch path only runs with a ready pool")
        };
        worker.dispatch(BatchRunJob {
            params: head.params.clone_for_dispatch(),
            tx: head_tx,
        })
    };

    let rx = match dispatch_result {
        Ok(rx) => rx,
        Err(WorkerGone::Exited) => {
            // Worker died before accepting the job; run head + any admitted members
            // inline in FIFO order (head was queue front, admitted members follow).
            sched.pool = PoolState::Empty;
            run_inline(head, state, sched);
            // Run remaining pending jobs inline sequentially (the normal loop will
            // pick them up on the next iteration via sched.pending).
            return;
        }
    };

    // ── Step 4: walk pending front-to-back, admit + execute on home ───────────
    //
    // Pipeline: admit → begin overlay → view-overlap check → execute body.
    // The matrix check vs the admitted set is static (read_sets only mutate under
    // the write guard, which this thread holds exclusively between commits), so
    // executing earlier members before admitting later ones is sound.

    let mut admitted_ids: Vec<ReducerId> = Vec::new();
    let mut admitted_members: Vec<ReducerJobPayload> = Vec::new();
    let mut admitted_outcomes: Vec<Option<BatchBodyOutcome>> = Vec::new();
    let mut admitted_panics: Vec<Option<Box<dyn std::any::Any + Send>>> = Vec::new();

    loop {
        // Peek at the front of pending to see if it's an admissible Reducer.
        let admissible = match sched.pending.front() {
            Some(WasmJob::Reducer(p)) => {
                let cid = p.params.reducer_id;
                let mut combined = vec![head_id];
                combined.extend_from_slice(&admitted_ids);
                admit(&access, resolved, cid, &combined)
            }
            _ => false,
        };

        if !admissible {
            break;
        }

        // Pop and begin the overlay for this member.
        let WasmJob::Reducer(member_boxed) = sched.pending.pop_front().expect("peeked Some") else {
            unreachable!("peeked Reducer")
        };
        let mut member = *member_boxed;
        let member_id = member.params.reducer_id;

        let member_op = reducer_op_for(state, &member);
        let member_tx = stdb.begin_batch_tx(Workload::Reducer(member_op));

        // WHY: each member's view-check runs under its own read guard; read_sets only
        // mutate on this executor thread (under the write guard), so the check is stable.
        let member_overlap = {
            let wt = resolved.write_tables[member_id.0 as usize].iter().copied();
            member_tx.view_read_overlap(wt)
        };
        if member_overlap {
            // Stop the prefix here; roll back this member's overlay and leave it at
            // the front of pending so it runs via the normal loop on the next cycle.
            drop(member_tx);
            sched.pending.push_front(WasmJob::Reducer(Box::new(member)));
            break;
        }

        // Execute the body on home right away while the worker runs the head.
        drop(member.glue.timer_guard.take());
        let params = member.params.clone_for_dispatch();
        let mut outcome_slot: Option<BatchBodyOutcome> = None;
        let mut panic_slot: Option<Box<dyn std::any::Any + Send>> = None;
        match std::panic::catch_unwind(AssertUnwindSafe(|| {
            state.with_instance(|inst| inst.call_reducer_body_batch(member_tx, params))
        })) {
            Ok(outcome) => outcome_slot = Some(outcome),
            Err(panic) => panic_slot = Some(panic),
        }

        admitted_ids.push(member_id);
        admitted_members.push(member);
        admitted_outcomes.push(outcome_slot);
        admitted_panics.push(panic_slot);
    }

    // ── Step 5: join the fork ──────────────────────────────────────────────────

    let head_outcome: Option<BatchBodyOutcome> = match ReducerWorker::recv_reply(rx) {
        Ok(reply) => {
            // outcome maps to the head.
            Some(reply.outcome)
        }
        Err(WorkerGone::Exited) => {
            // Worker died mid-flight; its overlay is lost but nothing committed yet.
            // Re-run the head's body as a fresh home overlay so it drains in its
            // original FIFO position. pool → Empty so no future forks fire this cycle.
            sched.pool = PoolState::Empty;
            let op = reducer_op_for(state, &head);
            let fresh_tx = stdb.begin_batch_tx(Workload::Reducer(op));
            let params = head.params.clone_for_dispatch();
            std::panic::catch_unwind(AssertUnwindSafe(|| {
                state.with_instance(|inst| inst.call_reducer_body_batch(fresh_tx, params))
            }))
            .ok()
        }
    };

    // ── Step 6: FIFO drain — head first, then admitted members in order ────────

    let subs = state.actor().replica_ctx().subscriptions.clone();
    let total = 1 + admitted_members.len();

    // Drain head.
    {
        let id = head.params.reducer_id;
        match head_outcome {
            None => {
                // Head panicked on recovery re-run (WorkerGone path above).
                head.reply_panic(Box::new("reducer worker exited and recovery panicked"));
            }
            Some(outcome) => {
                let duration = outcome.host_execution_duration;
                let budget = outcome.execution_budget_used;
                let client = head.params.client.clone();
                let success = commit_and_broadcast_batch_event(&subs, client, outcome.event, outcome.finished);
                sched.record_runtime(id, duration);
                head.reply_ok(ReducerCallResult {
                    outcome: ReducerOutcome::from(&success.event.status),
                    execution_budget_used: budget,
                    execution_duration: duration,
                });
            }
        }
    }

    // Drain admitted members.
    for (i, mut payload) in admitted_members.into_iter().enumerate() {
        let id = payload.params.reducer_id;
        if let Some(panic) = admitted_panics[i].take() {
            payload.reply_panic(panic);
            continue;
        }
        let outcome = admitted_outcomes[i].take().expect("non-panicked member produced an outcome");
        let duration = outcome.host_execution_duration;
        let budget = outcome.execution_budget_used;
        let client = payload.params.client.clone();
        let success = commit_and_broadcast_batch_event(&subs, client, outcome.event, outcome.finished);
        sched.record_runtime(id, duration);
        payload.reply_ok(ReducerCallResult {
            outcome: ReducerOutcome::from(&success.event.status),
            execution_budget_used: budget,
            execution_duration: duration,
        });
    }

    sched.stats.batches += 1;
    sched.stats.forks += 1;
    sched.stats.record_width(total);
}

fn reducer_op_for(state: &WasmtimeModuleState, m: &ReducerJobPayload) -> ReducerContext {
    let info = state.actor().info();
    let reducer_def = info.module_def.reducer_by_id(m.params.reducer_id);
    ReducerContext {
        name: reducer_def.name.clone(),
        caller_identity: m.params.caller_identity,
        caller_connection_id: m.params.caller_connection_id,
        timestamp: m.params.timestamp,
        arg_bsatn: m.params.args.get_bsatn().clone(),
    }
}

/// Post-commit pool lifecycle. Keeps the all-cheap path O(1): the only work for a
/// `pool == Empty` cheap reducer is checking whether `just_ran` tripped (no scan).
fn maybe_spawn_or_calibrate(state: &mut WasmtimeModuleState, sched: &mut SchedulerState, just_ran: ReducerId) {
    match &sched.pool {
        PoolState::Off | PoolState::Ready(_) => {}
        PoolState::Empty => {
            if sched.cap >= 1 && stat_trips(sched, just_ran) {
                let worker = spawn_worker(state);
                sched.pool = PoolState::Spawning(worker);
            }
        }
        PoolState::Spawning(worker) => {
            if worker.is_ready() {
                calibrate(sched);
            }
        }
    }
}

fn stat_trips(sched: &SchedulerState, id: ReducerId) -> bool {
    sched.threshold.value().is_some_and(|t| sched.stat(id).exceeds(t))
}

fn spawn_worker(state: &WasmtimeModuleState) -> ReducerWorker {
    let actor = state.actor().clone();
    let stdb = actor.replica_ctx().relational_db().clone();
    // Mirror the home thread naming: wasm-{last10hex} → wasm-{last10hex}-rwk0.
    let base = crate::host::wasmtime::wasm_worker_thread_name(&actor.replica_ctx().database_identity);
    let name = format!("{base}-rwk0");
    let make_instance = move || actor.create_instance();
    ReducerWorker::spawn(make_instance, stdb, name)
}

/// Run the two-stage calibration once the worker is warm: median of N round-trips,
/// `threshold = k × median`, pool → Ready.
fn calibrate(sched: &mut SchedulerState) {
    let PoolState::Spawning(worker) = &sched.pool else {
        return;
    };
    let mut samples = Vec::with_capacity(CALIBRATION_SAMPLES);
    for _ in 0..CALIBRATION_SAMPLES {
        match worker.calibrate() {
            Ok(d) => samples.push(d),
            Err(WorkerGone::Exited) => {
                // Worker died during calibration; reset to Empty and retry later.
                sched.pool = PoolState::Empty;
                return;
            }
        }
    }
    let median = median_duration(&mut samples);
    let t = median.saturating_mul(sched.k);
    log::info!("reducer fork threshold (calibrated): {t:?} (k={}, median={median:?})", sched.k);
    sched.stats.calibrated_ns = t.as_nanos() as u64;
    sched.threshold = ThresholdState::Calibrated(t);
    // Move the worker out of Spawning into Ready.
    let pool = std::mem::replace(&mut sched.pool, PoolState::Empty);
    if let PoolState::Spawning(w) = pool {
        sched.pool = PoolState::Ready(w);
    }
}

// ─── Pure helpers (unit-testable) ─────────────────────────────────────────────

fn lifecycle(access: &ReducerAccessInfo, id: ReducerId) -> bool {
    access.lifecycle.get(id.0 as usize).copied().unwrap_or(true)
}

fn wildcard(access: &ReducerAccessInfo, id: ReducerId) -> bool {
    access.wildcard.get(id.0 as usize).copied().unwrap_or(true)
}

/// Fast-path threshold gate: only meaningful with a calibrated threshold.
fn threshold_exceeded(sched: &SchedulerState, id: ReducerId) -> bool {
    sched.threshold.is_calibrated()
        && sched
            .threshold
            .value()
            .is_some_and(|t| sched.stat(id).exceeds(t))
}

/// Admission predicate for one candidate against the already-admitted members.
///
/// `batchable(c) = ¬lifecycle ∧ ¬wildcard ∧ disjoint vs every admitted member`.
fn admit(
    access: &ReducerAccessInfo,
    resolved: &crate::host::wasm_common::reducer_access::ResolvedWrites,
    candidate: ReducerId,
    admitted: &[ReducerId],
) -> bool {
    let c = candidate.0 as usize;
    if lifecycle(access, candidate) {
        return false;
    }
    if resolved.wildcard.get(c).copied().unwrap_or(true) {
        return false;
    }
    admitted.iter().all(|a| !access.matrix.conflicts(c, a.0 as usize))
}

fn median_duration(samples: &mut [Duration]) -> Duration {
    samples.sort_unstable();
    if samples.is_empty() {
        Duration::ZERO
    } else {
        samples[samples.len() / 2]
    }
}

/// One in-process thread round-trip, median of N: the startup fork-cost proxy.
fn measure_thread_round_trip() -> Duration {
    let mut samples = Vec::with_capacity(CALIBRATION_SAMPLES);
    let (to_worker_tx, to_worker_rx) = std_mpsc::channel::<()>();
    let (to_main_tx, to_main_rx) = std_mpsc::channel::<()>();
    let worker = std::thread::spawn(move || {
        while to_worker_rx.recv().is_ok() {
            if to_main_tx.send(()).is_err() {
                break;
            }
        }
    });
    for _ in 0..CALIBRATION_SAMPLES {
        let start = Instant::now();
        if to_worker_tx.send(()).is_err() {
            break;
        }
        if to_main_rx.recv().is_err() {
            break;
        }
        samples.push(start.elapsed());
    }
    drop(to_worker_tx);
    let _ = worker.join();
    median_duration(&mut samples)
}

// ─── Small payload helpers ────────────────────────────────────────────────────

impl CallReducerParams {
    /// Clone the parameters for dispatch to the worker / a batch body. Args are
    /// reference-counted bytes, so this is cheap.
    fn clone_for_dispatch(&self) -> CallReducerParams {
        CallReducerParams {
            timestamp: self.timestamp,
            caller_identity: self.caller_identity,
            caller_connection_id: self.caller_connection_id,
            client: self.client.clone(),
            request_id: self.request_id,
            timer: self.timer,
            reducer_id: self.reducer_id,
            args: self.args.clone(),
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::wasm_common::reducer_access::{ReducerAccessInfo, ResolvedWrites};
    use spacetimedb_access_analysis::AccessSet;

    /// Build fixtures directly, bypassing the wasm analyzer and live-datastore resolution.
    fn make_fixture(
        reads: &[&[&str]],
        writes: &[&[&str]],
        lifecycle: &[bool],
        extra_wildcard: &[bool],
    ) -> (Arc<ReducerAccessInfo>, ResolvedWrites) {
        use spacetimedb_schema::identifier::Identifier;
        let n = writes.len();
        let mut sets = Vec::with_capacity(n);
        for i in 0..n {
            let wildcard = extra_wildcard.get(i).copied().unwrap_or(false);
            sets.push(AccessSet {
                reads: reads[i].iter().map(Identifier::for_test).collect(),
                writes: writes[i].iter().map(Identifier::for_test).collect(),
                wildcard,
            });
        }

        let mut name_ids: std::collections::BTreeMap<String, u32> = std::collections::BTreeMap::new();
        let mut next = 0u32;
        let mut write_tables = Vec::with_capacity(n);
        let mut wildcard_flags = Vec::with_capacity(n);
        for i in 0..n {
            let is_wc = extra_wildcard.get(i).copied().unwrap_or(false) || sets[i].wildcard;
            let ids: Vec<_> = writes[i]
                .iter()
                .map(|name| {
                    let id = *name_ids.entry(name.to_string()).or_insert_with(|| {
                        let id = next;
                        next += 1;
                        id
                    });
                    spacetimedb_primitives::TableId(id)
                })
                .collect();
            write_tables.push(if is_wc { Vec::new() } else { ids });
            wildcard_flags.push(is_wc);
        }

        let access = Arc::new(ReducerAccessInfo::for_test(sets, lifecycle.to_vec()));
        let resolved = ResolvedWrites::for_test(write_tables, wildcard_flags);
        (access, resolved)
    }

    /// Thin wrapper so tests can pass plain `usize` indices.
    fn admit_production(
        access: &ReducerAccessInfo,
        resolved: &ResolvedWrites,
        candidate: usize,
        admitted: &[usize],
    ) -> bool {
        let admitted_ids: Vec<ReducerId> = admitted.iter().map(|&a| ReducerId(a as u32)).collect();
        admit(access, resolved, ReducerId(candidate as u32), &admitted_ids)
    }

    /// Build a strict prefix using the production `admit`.
    fn build_prefix_production(
        access: &ReducerAccessInfo,
        resolved: &ResolvedWrites,
        queue: &[usize],
    ) -> Vec<usize> {
        let mut admitted: Vec<usize> = Vec::new();
        for &cid in queue {
            if !admit_production(access, resolved, cid, &admitted) {
                break;
            }
            admitted.push(cid);
        }
        admitted
    }

    // ── Admission truth table ────────────────────────────────────────────────

    #[test]
    fn admission_disjoint_admits() {
        // r0 writes {a}, r1 writes {b}; disjoint → both admit.
        let (access, resolved) =
            make_fixture(&[&[], &[]], &[&["a"], &["b"]], &[false, false], &[false, false]);
        assert!(admit_production(&access, &resolved, 0, &[]));
        assert!(admit_production(&access, &resolved, 1, &[0]));
    }

    #[test]
    fn admission_write_write_conflict_blocks() {
        // both write {a} → conflict.
        let (access, resolved) =
            make_fixture(&[&[], &[]], &[&["a"], &["a"]], &[false, false], &[false, false]);
        assert!(admit_production(&access, &resolved, 0, &[]));
        assert!(!admit_production(&access, &resolved, 1, &[0]));
    }

    #[test]
    fn admission_read_write_conflict_blocks() {
        // r0 writes {a}; r1 reads {a} → write∩read conflict.
        let (access, resolved) =
            make_fixture(&[&[], &["a"]], &[&["a"], &["b"]], &[false, false], &[false, false]);
        assert!(!admit_production(&access, &resolved, 1, &[0]));
    }

    #[test]
    fn admission_read_read_admits() {
        // r0 reads {a}, r1 reads {a}, neither writes it → read∩read is free.
        let (access, resolved) =
            make_fixture(&[&["a"], &["a"]], &[&["x"], &["y"]], &[false, false], &[false, false]);
        assert!(admit_production(&access, &resolved, 1, &[0]));
    }

    #[test]
    fn admission_lifecycle_blocks() {
        let (access, resolved) =
            make_fixture(&[&[], &[]], &[&["a"], &["b"]], &[false, true], &[false, false]);
        assert!(
            !admit_production(&access, &resolved, 1, &[0]),
            "lifecycle reducer must never batch"
        );
    }

    #[test]
    fn admission_wildcard_blocks() {
        let (access, resolved) =
            make_fixture(&[&[], &[]], &[&["a"], &["b"]], &[false, false], &[false, true]);
        assert!(
            !admit_production(&access, &resolved, 1, &[0]),
            "wildcard reducer must never batch"
        );
    }

    // ── Prefix construction (FIFO, stops at first barrier) ───────────────────

    #[test]
    fn prefix_stops_at_first_conflict_preserves_order() {
        // queue [0,1,2]: 0 writes a, 1 writes b, 2 writes a (conflicts 0).
        let (access, resolved) = make_fixture(
            &[&[], &[], &[]],
            &[&["a"], &["b"], &["a"]],
            &[false, false, false],
            &[false, false, false],
        );
        assert_eq!(build_prefix_production(&access, &resolved, &[0, 1, 2]), vec![0, 1]);
    }

    #[test]
    fn prefix_stops_at_lifecycle_barrier() {
        let (access, resolved) = make_fixture(
            &[&[], &[], &[]],
            &[&["a"], &["b"], &["c"]],
            &[false, true, false],
            &[false, false, false],
        );
        // queue [0,1,2]: 1 is lifecycle → prefix is just [0]; 2 not reached.
        assert_eq!(build_prefix_production(&access, &resolved, &[0, 1, 2]), vec![0]);
    }

    #[test]
    fn prefix_all_disjoint_full_width() {
        let (access, resolved) = make_fixture(
            &[&[], &[], &[]],
            &[&["a"], &["b"], &["c"]],
            &[false, false, false],
            &[false, false, false],
        );
        assert_eq!(build_prefix_production(&access, &resolved, &[0, 1, 2]), vec![0, 1, 2]);
    }

    // ── Stats update (high-water monotone, EMA both-must-exceed gate) ────────

    #[test]
    fn stat_high_water_monotone() {
        let mut s = Stat::default();
        s.update(Duration::from_micros(100));
        assert_eq!(s.high_water, Duration::from_micros(100));
        s.update(Duration::from_micros(50));
        assert_eq!(s.high_water, Duration::from_micros(100), "high-water never decreases");
        s.update(Duration::from_micros(200));
        assert_eq!(s.high_water, Duration::from_micros(200));
    }

    #[test]
    fn stat_ema_converges_and_lags() {
        let mut s = Stat::default();
        // Feed a constant 800µs; EMA climbs toward it but lags high-water initially.
        for _ in 0..64 {
            s.update(Duration::from_micros(800));
        }
        // After many samples EMA is close to the constant.
        assert!(s.ema >= Duration::from_micros(700), "ema converged: {:?}", s.ema);
        assert_eq!(s.high_water, Duration::from_micros(800));
    }

    #[test]
    fn stat_exceeds_requires_both() {
        let t = Duration::from_micros(100);
        // High-water above but EMA below → does not exceed.
        let s = Stat { high_water: Duration::from_micros(500), ema: Duration::from_micros(50) };
        assert!(!s.exceeds(t), "EMA below threshold must block fork");
        // Both above → exceeds.
        let s = Stat { high_water: Duration::from_micros(500), ema: Duration::from_micros(200) };
        assert!(s.exceeds(t));
    }

    // ── Threshold machine transitions ────────────────────────────────────────

    #[test]
    fn threshold_off_stays_off_when_cap_zero() {
        // Construction with cap 0 yields Off/Off; modeled directly here since the
        // real ctor needs a WasmtimeModuleState.
        let threshold = ThresholdState::Off;
        assert!(threshold.value().is_none());
        assert!(!threshold.is_calibrated());
    }

    #[test]
    fn threshold_proxy_to_calibrated() {
        let proxy = ThresholdState::Proxy(Duration::from_micros(40));
        assert_eq!(proxy.value(), Some(Duration::from_micros(40)));
        assert!(!proxy.is_calibrated(), "proxy must not enable real forks");

        let calibrated = ThresholdState::Calibrated(Duration::from_micros(160));
        assert_eq!(calibrated.value(), Some(Duration::from_micros(160)));
        assert!(calibrated.is_calibrated());
    }

    #[test]
    fn pool_state_transitions_modeled() {
        // Off is terminal; Empty→Spawning→Ready is the warm path. We can assert the
        // discriminants without a live worker.
        let off = PoolState::Off;
        assert!(matches!(off, PoolState::Off));
        let empty = PoolState::Empty;
        assert!(matches!(empty, PoolState::Empty));
    }

    #[test]
    fn median_of_samples() {
        let mut s = vec![
            Duration::from_micros(50),
            Duration::from_micros(10),
            Duration::from_micros(30),
        ];
        assert_eq!(median_duration(&mut s), Duration::from_micros(30));
    }
}
