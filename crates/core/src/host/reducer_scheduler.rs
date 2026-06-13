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

use spacetimedb_data_structures::map::IntSet;
use spacetimedb_datastore::locking_tx_datastore::{ObservedAccess, ViewOverlapKind};
use spacetimedb_primitives::TableId;

use crate::host::module_host::CallReducerParams;
use crate::host::wasm_common::module_host_actor::{BatchBodyOutcome, CaptureSpec};
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
    pub promotions: u64,
    pub demotions: u64,
    pub parks: u64,
    pub member_traps: u64,
    pub head_traps: u64,
    pub view_trap_aborts: u64,
    pub requeues: u64,
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
        let handle = Self { job_tx: job_tx.clone() };

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
    promotions: u64,
    demotions: u64,
    parks: u64,
    member_traps: u64,
    head_traps: u64,
    view_trap_aborts: u64,
    requeues: u64,
}

impl BatchStats {
    fn record_width(&mut self, width: usize) {
        let bucket = width.clamp(1, 5) - 1;
        self.widths[bucket] += 1;
    }
}

// ─── Learning tier state machine ─────────────────────────────────────────────

const LEARN_MIN_RUNS: u32 = 8;
const LEARN_QUIET_RUNS: u32 = 4;
const LEARN_STRIKES: u8 = 2;

struct TierEntry {
    tier: AccessTier,
    strikes: u8,
}

enum AccessTier {
    Static,              // analyzer set exists — matrix row valid
    Unknown(ObsState),   // static-wildcard — learning in progress
    Learned(LearnedSet), // promoted — trap-guarded
    Parked,              // strike cap hit — wildcard forever (this boot)
}

#[derive(Default)]
struct ObsState {
    reads: IntSet<TableId>,
    writes: IntSet<TableId>,
    runs: u32,
    quiet: u32,
}

struct LearnedSet {
    reads: IntSet<TableId>,
    writes: IntSet<TableId>,
}

/// Returns `true` if the entry was promoted to `Learned` on this call.
fn record_observation(entry: &mut TierEntry, observed: &ObservedAccess) -> bool {
    let AccessTier::Unknown(obs) = &mut entry.tier else {
        return false;
    };
    let is_new = observed.reads.iter().any(|t| !obs.reads.contains(t))
        || observed.writes.iter().any(|t| !obs.writes.contains(t));
    obs.reads.extend(observed.reads.iter().copied());
    obs.writes.extend(observed.writes.iter().copied());
    obs.runs += 1;
    obs.quiet = if is_new { 0 } else { obs.quiet + 1 };
    if obs.runs >= LEARN_MIN_RUNS && obs.quiet >= LEARN_QUIET_RUNS {
        let learned = LearnedSet {
            reads: std::mem::take(&mut obs.reads),
            writes: std::mem::take(&mut obs.writes),
        };
        entry.tier = AccessTier::Learned(learned);
        return true;
    }
    false
}

/// What `record_escape` did to the entry, so the caller bumps the right stat.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EscapeOutcome {
    Demoted,
    Parked,
}

fn record_escape(entry: &mut TierEntry, observed: &ObservedAccess) -> EscapeOutcome {
    let AccessTier::Learned(learned) = &mut entry.tier else {
        return EscapeOutcome::Parked;
    };
    entry.strikes += 1;
    if entry.strikes >= LEARN_STRIKES {
        entry.tier = AccessTier::Parked;
        EscapeOutcome::Parked
    } else {
        let mut seed_reads = std::mem::take(&mut learned.reads);
        let mut seed_writes = std::mem::take(&mut learned.writes);
        seed_reads.extend(observed.reads.iter().copied());
        seed_writes.extend(observed.writes.iter().copied());
        entry.tier = AccessTier::Unknown(ObsState {
            reads: seed_reads,
            writes: seed_writes,
            runs: 0,
            quiet: 0,
        });
        EscapeOutcome::Demoted
    }
}

/// Returns `true` for tiers that allow fork dispatch.
fn tier_fork_eligible(entry: &TierEntry) -> bool {
    matches!(entry.tier, AccessTier::Static | AccessTier::Learned(_))
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
    tiers: Vec<TierEntry>,
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

        let tiers = (0..n)
            .map(|i| {
                let is_wildcard = access.wildcard.get(i).copied().unwrap_or(true);
                let is_lifecycle = access.lifecycle.get(i).copied().unwrap_or(true);
                let tier = if is_wildcard && !is_lifecycle {
                    AccessTier::Unknown(ObsState::default())
                } else {
                    AccessTier::Static
                };
                TierEntry { tier, strikes: 0 }
            })
            .collect();

        Self {
            pending: VecDeque::new(),
            runtime_stats: vec![Stat::default(); n],
            threshold,
            pool,
            access,
            cap,
            k,
            stats: BatchStats::default(),
            tiers,
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
            promotions: self.stats.promotions,
            demotions: self.stats.demotions,
            parks: self.stats.parks,
            member_traps: self.stats.member_traps,
            head_traps: self.stats.head_traps,
            view_trap_aborts: self.stats.view_trap_aborts,
            requeues: self.stats.requeues,
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
    let fork_eligible = sched.tiers.get(id.0 as usize).is_some_and(tier_fork_eligible)
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
        mut params,
        mut reply,
        glue,
    } = payload;
    let id = params.reducer_id;
    let timer_guard = glue.timer_guard;

    let is_unknown = sched
        .tiers
        .get(id.0 as usize)
        .is_some_and(|e| matches!(e.tier, AccessTier::Unknown(_)));
    if is_unknown {
        params.capture_access = true;
    }

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
            if is_unknown
                && matches!(res.outcome, ReducerOutcome::Committed)
                && let Some(obs) = &res.observed
                && let Some(entry) = sched.tiers.get_mut(id.0 as usize)
                && record_observation(entry, obs)
            {
                sched.stats.promotions += 1;
            }
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
    let has_companion = {
        let tiers = &sched.tiers;
        sched.pending.iter().any(|job| {
            let WasmJob::Reducer(p) = job else { return false };
            admit(&access, resolved, tiers, p.params.reducer_id, &[head_id])
        })
    };
    if !has_companion {
        run_inline(head, state, sched);
        return;
    }

    // ── Step 3: begin head overlay, view-overlap check, dispatch to worker ─────

    let head_is_learned = is_learned(&sched.tiers, head_id);

    let head_op = reducer_op_for(state, &head);
    let head_tx = stdb.begin_batch_tx(Workload::Reducer(head_op));

    // WHY check under the head's own guard: each member's view-check runs under its
    // own read guard so read_sets are stable (they mutate only under the write guard,
    // which this executor thread is the sole acquirer of).
    let head_overlap = {
        let wt = effective_write_iter(&sched.tiers, resolved, head_id);
        head_tx.view_read_overlap(wt.iter().copied())
    };
    if head_overlap {
        // Rollback head's overlay and fall back to plain inline execution.
        drop(head_tx);
        run_inline(head, state, sched);
        return;
    }

    // Running union of effective writes in serial order; a member's observed access must be
    // disjoint from it to be provably serial-correct against everything ahead of it.
    let mut earlier_writes: IntSet<TableId> = effective_write_iter(&sched.tiers, resolved, head_id)
        .iter()
        .copied()
        .collect();

    // Drop the queue-wait timer right before dispatching — body is about to start.
    drop(head.glue.timer_guard.take());

    // Learned head: the worker captures access and runs the key-level view check pre-finish.
    let head_capture = CaptureSpec {
        capture_access: head_is_learned,
        check_views: head_is_learned,
    };

    let dispatch_result = {
        let PoolState::Ready(worker) = &sched.pool else {
            unreachable!("batch path only runs with a ready pool")
        };
        worker.dispatch(BatchRunJob {
            params: head.params.clone_for_dispatch(),
            tx: head_tx,
            capture: head_capture,
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

    // ── Step 4: walk pending front-to-back, admit + execute + trap on home ─────
    //
    // Pipeline per member: admit → begin overlay → member view pre-filter → execute body
    // → post-body trap (view / escape / hazard) before admitting the next member. The
    // matrix check vs the admitted set is static (read_sets only mutate under the write
    // guard, which this thread holds exclusively between commits), so executing earlier
    // members before admitting later ones is sound.

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
                admit(&access, resolved, &sched.tiers, cid, &combined)
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
        let member_is_learned = is_learned(&sched.tiers, member_id);

        let member_op = reducer_op_for(state, &member);
        let member_tx = stdb.begin_batch_tx(Workload::Reducer(member_op));

        // WHY: each member's view-check runs under its own read guard; read_sets only
        // mutate on this executor thread (under the write guard), so the check is stable.
        let overlap_kind = {
            let wt = effective_write_iter(&sched.tiers, resolved, member_id);
            member_tx.view_overlap_kind(wt.iter().copied())
        };
        let speculative = match overlap_kind {
            ViewOverlapKind::FullScan => {
                // A full-scan view refresh is one the batch lane cannot perform: stop the
                // prefix here, roll back this overlay, and leave the member at the front of
                // pending so it runs via the normal loop on the next cycle.
                drop(member_tx);
                sched.pending.push_front(WasmJob::Reducer(Box::new(member)));
                break;
            }
            ViewOverlapKind::KeyOnly => true,
            ViewOverlapKind::None => false,
        };

        // learned writes are not a superset of actual writes — the post-body key check is
        // the integrity boundary for a Learned member regardless of the pre-filter result.
        let member_capture = CaptureSpec {
            capture_access: member_is_learned,
            check_views: speculative || member_is_learned,
        };

        // Execute the body on home right away while the worker runs the head.
        drop(member.glue.timer_guard.take());
        let params = member.params.clone_for_dispatch();
        let mut outcome_slot: Option<BatchBodyOutcome> = None;
        let mut panic_slot: Option<Box<dyn std::any::Any + Send>> = None;
        match std::panic::catch_unwind(AssertUnwindSafe(|| {
            state.with_instance(|inst| inst.call_reducer_body_batch(member_tx, params, member_capture))
        })) {
            Ok(outcome) => outcome_slot = Some(outcome),
            Err(panic) => panic_slot = Some(panic),
        }

        // ── Post-body member trap (before the next admission iteration) ────────
        //
        // Panicked members skip all checks and join the admitted lists (drained as today).
        if panic_slot.is_some() {
            admitted_ids.push(member_id);
            admitted_members.push(member);
            admitted_outcomes.push(outcome_slot);
            admitted_panics.push(panic_slot);
            continue;
        }

        let outcome = outcome_slot.as_ref().expect("non-panicked member produced an outcome");
        let view_refresh_needed = outcome.view_refresh_needed;

        // Compute escape/conflict against the learned set BEFORE any tier mutation. A
        // non-captured (non-Learned) member has no observed set and cannot escape.
        let (escaped, conflicting) = match (member_is_learned, outcome.observed.as_deref()) {
            (true, Some(obs)) => match learned_set(&sched.tiers, member_id) {
                Some(ls) => {
                    let escaped = escape_check(obs, ls);
                    (escaped, escaped && hazard_check(obs, &earlier_writes))
                }
                None => (false, false),
            },
            _ => (false, false),
        };

        match member_trap_decision(view_refresh_needed, escaped, conflicting) {
            MemberTrapDecision::RequeueViewTrap => {
                // No demotion: a view miss is subscription-data-dependent, not learned-set wrongness.
                drop(outcome_slot);
                sched.pending.push_front(WasmJob::Reducer(Box::new(member)));
                sched.stats.view_trap_aborts += 1;
                sched.stats.requeues += 1;
                break;
            }
            MemberTrapDecision::RequeueConflict => {
                let observed = observed_of(&outcome_slot);
                if let Some(entry) = sched.tiers.get_mut(member_id.0 as usize) {
                    apply_escape(&mut sched.stats, entry, &observed);
                }
                drop(outcome_slot);
                sched.pending.push_front(WasmJob::Reducer(Box::new(member)));
                sched.stats.member_traps += 1;
                sched.stats.requeues += 1;
                break;
            }
            MemberTrapDecision::KeepHarmlessEscape => {
                // Disjoint from every earlier write ⇒ serial-correct, so it commits at drain;
                // the outcome's observed set stays intact for the head suffix cut.
                let observed = observed_of(&outcome_slot);
                if let Some(entry) = sched.tiers.get_mut(member_id.0 as usize) {
                    apply_escape(&mut sched.stats, entry, &observed);
                }
                admitted_ids.push(member_id);
                admitted_members.push(member);
                admitted_outcomes.push(outcome_slot);
                admitted_panics.push(panic_slot);
                break;
            }
            MemberTrapDecision::Keep => {
                // Survived both checks: extend the running write union with this member's
                // effective-actual writes (observed when captured, else static resolved).
                match outcome_slot.as_ref().and_then(|o| o.observed.as_deref()) {
                    Some(obs) => earlier_writes.extend(obs.writes.iter().copied()),
                    None => {
                        earlier_writes.extend(effective_write_iter(&sched.tiers, resolved, member_id).iter().copied())
                    }
                }
                admitted_ids.push(member_id);
                admitted_members.push(member);
                admitted_outcomes.push(outcome_slot);
                admitted_panics.push(panic_slot);
            }
        }
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
                state.with_instance(|inst| inst.call_reducer_body_batch(fresh_tx, params, head_capture))
            }))
            .ok()
        }
    };

    // ── Step 5b: head join trap (Learned head only) ────────────────────────────
    //
    // Evaluate the head's escape + whole-batch view abort + suffix cut BEFORE any drain;
    // nothing commits before its trap validation passes.
    if head_is_learned {
        let head_obs = head_outcome.as_ref().and_then(|o| o.observed.as_deref());
        let head_view_refresh = head_outcome.as_ref().is_some_and(|o| o.view_refresh_needed);
        let head_escaped = match (head_obs, learned_set(&sched.tiers, head_id)) {
            (Some(obs), Some(ls)) => escape_check(obs, ls),
            _ => false,
        };

        if head_escaped {
            sched.stats.head_traps += 1;
            let observed = head_obs.cloned().unwrap_or_default();
            if let Some(entry) = sched.tiers.get_mut(head_id.0 as usize) {
                apply_escape(&mut sched.stats, entry, &observed);
            }
        }

        if head_view_refresh {
            // Whole-batch view abort: a head view refresh the batch lane cannot perform voids
            // every overlay; the committed reruns do all replies/energy/runtime via normal lanes.
            let members = std::mem::take(&mut admitted_members);
            let aborted = members.len();
            let mut jobs: Vec<WasmJob> = Vec::with_capacity(1 + aborted);
            jobs.push(WasmJob::Reducer(Box::new(head)));
            jobs.extend(members.into_iter().map(|m| WasmJob::Reducer(Box::new(m))));
            push_front_in_order(&mut sched.pending, jobs);
            sched.stats.requeues += 1 + aborted as u64;
            // The fork fired, so the batch/fork/width counters still tick.
            sched.stats.batches += 1;
            sched.stats.forks += 1;
            sched.stats.record_width(1 + aborted);
            return;
        }

        if head_escaped {
            // Suffix cut: the head escaped into a table no admission saw, so abort from the
            // first member that now conflicts with its writes onward; the clean prefix drains.
            let head_writes = head_obs.map(|o| &o.writes);
            if let Some(head_writes) = head_writes {
                let cut = {
                    let members = admitted_member_sets(&admitted_ids, &admitted_outcomes, &sched.tiers, resolved);
                    head_cut_index(head_writes, members.into_iter())
                };
                if let Some(i) = cut {
                    let aborted = admitted_members.len() - i;
                    let suffix: Vec<ReducerJobPayload> = admitted_members.split_off(i);
                    admitted_ids.truncate(i);
                    admitted_outcomes.truncate(i);
                    admitted_panics.truncate(i);
                    let jobs: Vec<WasmJob> = suffix.into_iter().map(|m| WasmJob::Reducer(Box::new(m))).collect();
                    push_front_in_order(&mut sched.pending, jobs);
                    sched.stats.requeues += aborted as u64;
                }
            }
        }
    }

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
                let caller_identity = head.params.caller_identity;
                let client = head.params.client.clone();
                let success = commit_and_broadcast_batch_event(&subs, client, outcome.event, outcome.finished);
                // Charge-committed-only: record energy now that the body has committed.
                state.with_instance(|inst| inst.record_reducer_energy(id, caller_identity, budget, duration));
                sched.record_runtime(id, duration);
                head.reply_ok(ReducerCallResult {
                    outcome: ReducerOutcome::from(&success.event.status),
                    execution_budget_used: budget,
                    execution_duration: duration,
                    observed: None,
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
        let outcome = admitted_outcomes[i]
            .take()
            .expect("non-panicked member produced an outcome");
        let duration = outcome.host_execution_duration;
        let budget = outcome.execution_budget_used;
        let caller_identity = payload.params.caller_identity;
        let client = payload.params.client.clone();
        let success = commit_and_broadcast_batch_event(&subs, client, outcome.event, outcome.finished);
        // Charge-committed-only: record energy now that the body has committed.
        state.with_instance(|inst| inst.record_reducer_energy(id, caller_identity, budget, duration));
        sched.record_runtime(id, duration);
        payload.reply_ok(ReducerCallResult {
            outcome: ReducerOutcome::from(&success.event.status),
            execution_budget_used: budget,
            execution_duration: duration,
            observed: None,
        });
    }

    sched.stats.batches += 1;
    sched.stats.forks += 1;
    sched.stats.record_width(total);
}

/// Clone the captured `ObservedAccess` out of an outcome slot without disturbing it, so the
/// tier mutation can run by value while the outcome (and its observed set) lives on for the
/// drain / head suffix cut. Empty when nothing was captured.
fn observed_of(slot: &Option<BatchBodyOutcome>) -> ObservedAccess {
    slot.as_ref()
        .and_then(|o| o.observed.as_deref())
        .cloned()
        .unwrap_or_default()
}

/// Requeue `jobs` at the FRONT of `pending`, preserving their order (so `jobs[0]` ends up
/// front-most): push in reverse, since each `push_front` prepends.
fn push_front_in_order<T>(pending: &mut VecDeque<T>, jobs: Vec<T>) {
    for job in jobs.into_iter().rev() {
        pending.push_front(job);
    }
}

/// Apply an escape to `entry`, bumping the matching `demotions`/`parks` counter.
fn apply_escape(stats: &mut BatchStats, entry: &mut TierEntry, observed: &ObservedAccess) {
    match record_escape(entry, observed) {
        EscapeOutcome::Demoted => stats.demotions += 1,
        EscapeOutcome::Parked => stats.parks += 1,
    }
}

/// Per-member effective `(reads, writes)` sets for the head suffix cut: the member's
/// observed sets when it has a captured outcome, else its learned/static sets. Panicked
/// members (no outcome) fall back to their static/learned sets.
fn admitted_member_sets<'a>(
    admitted_ids: &'a [ReducerId],
    admitted_outcomes: &'a [Option<BatchBodyOutcome>],
    tiers: &'a [TierEntry],
    resolved: &'a crate::host::wasm_common::reducer_access::ResolvedAccess,
) -> Vec<(TableSet<'a>, TableSet<'a>)> {
    admitted_ids
        .iter()
        .zip(admitted_outcomes)
        .map(
            |(&id, outcome)| match outcome.as_ref().and_then(|o| o.observed.as_deref()) {
                Some(obs) => (TableSet::Set(&obs.reads), TableSet::Set(&obs.writes)),
                None => effective_sets(&tiers[id.0 as usize].tier, resolved, id.0 as usize),
            },
        )
        .collect()
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
    log::info!(
        "reducer fork threshold (calibrated): {t:?} (k={}, median={median:?})",
        sched.k
    );
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

#[allow(dead_code)]
fn wildcard(access: &ReducerAccessInfo, id: ReducerId) -> bool {
    access.wildcard.get(id.0 as usize).copied().unwrap_or(true)
}

/// The learned set for `id`, if its tier is `Learned`.
fn learned_set(tiers: &[TierEntry], id: ReducerId) -> Option<&LearnedSet> {
    match tiers.get(id.0 as usize).map(|e| &e.tier) {
        Some(AccessTier::Learned(ls)) => Some(ls),
        _ => None,
    }
}

/// Whether `id`'s tier is `Learned` (the only tier the executor captures + trap-guards).
fn is_learned(tiers: &[TierEntry], id: ReducerId) -> bool {
    learned_set(tiers, id).is_some()
}

/// Effective writes for `id`: learned writes for a `Learned` tier, the resolved static
/// write slice otherwise. A `Learned` reducer's resolved slice is empty by construction.
fn effective_write_iter<'a>(
    tiers: &'a [TierEntry],
    resolved: &'a crate::host::wasm_common::reducer_access::ResolvedAccess,
    id: ReducerId,
) -> TableSet<'a> {
    match learned_set(tiers, id) {
        Some(ls) => TableSet::Set(&ls.writes),
        None => TableSet::Slice(
            resolved
                .write_tables
                .get(id.0 as usize)
                .map(|v| v.as_slice())
                .unwrap_or(&[]),
        ),
    }
}

/// `true` iff any observed read or write lies outside the learned set — the learned
/// set was wrong for this run and the reducer must be re-tiered.
fn escape_check(observed: &ObservedAccess, learned: &LearnedSet) -> bool {
    observed.reads.iter().any(|t| !learned.reads.contains(t))
        || observed.writes.iter().any(|t| !learned.writes.contains(t))
}

/// `true` iff the observed access (reads ∪ writes) intersects the running union of
/// earlier effective writes — a serial hazard against an already-executed sibling.
fn hazard_check(observed: &ObservedAccess, earlier_writes: &IntSet<TableId>) -> bool {
    observed.reads.iter().any(|t| earlier_writes.contains(t))
        || observed.writes.iter().any(|t| earlier_writes.contains(t))
}

/// Post-body fate of one batch member, computed purely from its three trap signals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MemberTrapDecision {
    /// No trap (or a harmless static set): keep the member and admit the next one.
    Keep,
    /// Key-level view miss: requeue and cut (subscription-data-dependent, no demotion).
    RequeueViewTrap,
    /// Escaped its learned set AND conflicts with an earlier write: requeue and cut.
    RequeueConflict,
    /// Escaped but disjoint from all earlier writes: provably serial-correct, keep but cut.
    KeepHarmlessEscape,
}

/// View trap dominates (precedence: view misses are data-dependent, not learned-set
/// wrongness); then a conflicting escape; then a harmless escape; else keep.
fn member_trap_decision(view_refresh_needed: bool, escaped: bool, conflicting: bool) -> MemberTrapDecision {
    if view_refresh_needed {
        MemberTrapDecision::RequeueViewTrap
    } else if escaped && conflicting {
        MemberTrapDecision::RequeueConflict
    } else if escaped {
        MemberTrapDecision::KeepHarmlessEscape
    } else {
        MemberTrapDecision::Keep
    }
}

/// The index of the first member whose effective sets intersect the head's escaped
/// writes — the suffix `[i, …]` must abort. `None` means the whole batch is clean.
/// `members` yields each member's `(reads, writes)` effective sets in admitted order.
fn head_cut_index<'a>(
    head_obs_writes: &IntSet<TableId>,
    members: impl Iterator<Item = (TableSet<'a>, TableSet<'a>)>,
) -> Option<usize> {
    let head = TableSet::Set(head_obs_writes);
    members
        .enumerate()
        .find_map(|(i, (reads, writes))| (sets_intersect(&head, &reads) || sets_intersect(&head, &writes)).then_some(i))
}

/// Fast-path threshold gate: only meaningful with a calibrated threshold.
fn threshold_exceeded(sched: &SchedulerState, id: ReducerId) -> bool {
    sched.threshold.is_calibrated() && sched.threshold.value().is_some_and(|t| sched.stat(id).exceeds(t))
}

/// Admission predicate for one candidate against the already-admitted members.
///
/// `batchable(c) = ¬lifecycle ∧ tier≠Unknown ∧ tier≠Parked ∧ disjoint vs every admitted member`.
fn admit(
    access: &ReducerAccessInfo,
    resolved: &crate::host::wasm_common::reducer_access::ResolvedAccess,
    tiers: &[TierEntry],
    candidate: ReducerId,
    admitted: &[ReducerId],
) -> bool {
    let c = candidate.0 as usize;
    if lifecycle(access, candidate) {
        return false;
    }
    let c_tier = match tiers.get(c) {
        Some(e) => &e.tier,
        None => return false,
    };
    match c_tier {
        AccessTier::Unknown(_) | AccessTier::Parked => return false,
        AccessTier::Static => {
            if resolved.wildcard.get(c).copied().unwrap_or(true) {
                return false;
            }
        }
        AccessTier::Learned(_) => {}
    }
    admitted.iter().all(|a| {
        let ai = a.0 as usize;
        let a_tier = match tiers.get(ai) {
            Some(e) => &e.tier,
            None => return false,
        };
        !tier_conflicts(access, resolved, c_tier, c, a_tier, ai)
    })
}

/// An accessor over either a slice of `TableId`s (Static resolved) or an `IntSet<TableId>` (Learned).
enum TableSet<'a> {
    Slice(&'a [TableId]),
    Set(&'a IntSet<TableId>),
}

impl<'a> TableSet<'a> {
    fn contains(&self, t: &TableId) -> bool {
        match self {
            TableSet::Slice(s) => s.contains(t),
            TableSet::Set(s) => s.contains(t),
        }
    }
    fn iter(&self) -> impl Iterator<Item = &TableId> {
        match self {
            TableSet::Slice(s) => itertools::Either::Left(s.iter()),
            TableSet::Set(s) => itertools::Either::Right(s.iter()),
        }
    }
}

fn effective_sets<'a>(
    tier: &'a AccessTier,
    resolved: &'a crate::host::wasm_common::reducer_access::ResolvedAccess,
    idx: usize,
) -> (TableSet<'a>, TableSet<'a>) {
    match tier {
        AccessTier::Learned(ls) => (TableSet::Set(&ls.reads), TableSet::Set(&ls.writes)),
        _ => (
            TableSet::Slice(resolved.read_tables.get(idx).map(|v| v.as_slice()).unwrap_or(&[])),
            TableSet::Slice(resolved.write_tables.get(idx).map(|v| v.as_slice()).unwrap_or(&[])),
        ),
    }
}

fn sets_intersect(a: &TableSet<'_>, b: &TableSet<'_>) -> bool {
    a.iter().any(|t| b.contains(t))
}

fn tier_conflicts(
    access: &ReducerAccessInfo,
    resolved: &crate::host::wasm_common::reducer_access::ResolvedAccess,
    c_tier: &AccessTier,
    ci: usize,
    a_tier: &AccessTier,
    ai: usize,
) -> bool {
    match (c_tier, a_tier) {
        (AccessTier::Static, AccessTier::Static) => access.matrix.conflicts(ci, ai),
        _ => {
            // A resolved-wildcard Static side has EMPTY resolved slices but unknown true
            // access; set intersection would silently under-approximate. Treat as universal.
            let static_unresolved = |tier: &AccessTier, idx: usize| {
                matches!(tier, AccessTier::Static) && resolved.wildcard.get(idx).copied().unwrap_or(true)
            };
            if static_unresolved(c_tier, ci) || static_unresolved(a_tier, ai) {
                return true;
            }
            let (c_reads, c_writes) = effective_sets(c_tier, resolved, ci);
            let (a_reads, a_writes) = effective_sets(a_tier, resolved, ai);
            sets_intersect(&c_writes, &a_reads)
                || sets_intersect(&c_writes, &a_writes)
                || sets_intersect(&a_writes, &c_reads)
        }
    }
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
            capture_access: self.capture_access,
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::wasm_common::reducer_access::{ReducerAccessInfo, ResolvedAccess};
    use spacetimedb_access_analysis::AccessSet;

    /// Build fixtures directly, bypassing the wasm analyzer and live-datastore resolution.
    fn make_fixture(
        reads: &[&[&str]],
        writes: &[&[&str]],
        lifecycle: &[bool],
        extra_wildcard: &[bool],
    ) -> (Arc<ReducerAccessInfo>, ResolvedAccess) {
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

        let resolve_names = |names: &[&str],
                             name_ids: &mut std::collections::BTreeMap<String, u32>,
                             next: &mut u32|
         -> Vec<spacetimedb_primitives::TableId> {
            names
                .iter()
                .map(|name| {
                    let id = *name_ids.entry(name.to_string()).or_insert_with(|| {
                        let id = *next;
                        *next += 1;
                        id
                    });
                    spacetimedb_primitives::TableId(id)
                })
                .collect()
        };

        let mut read_tables = Vec::with_capacity(n);
        let mut write_tables = Vec::with_capacity(n);
        let mut wildcard_flags = Vec::with_capacity(n);
        for i in 0..n {
            let is_wc = extra_wildcard.get(i).copied().unwrap_or(false) || sets[i].wildcard;
            let r_ids = resolve_names(reads[i], &mut name_ids, &mut next);
            let w_ids = resolve_names(writes[i], &mut name_ids, &mut next);
            read_tables.push(if is_wc { Vec::new() } else { r_ids });
            write_tables.push(if is_wc { Vec::new() } else { w_ids });
            wildcard_flags.push(is_wc);
        }

        let access = Arc::new(ReducerAccessInfo::for_test(sets, lifecycle.to_vec()));
        let resolved = ResolvedAccess::for_test(read_tables, write_tables, wildcard_flags);
        (access, resolved)
    }

    /// Build an all-Static tiers vec matching the access info length.
    fn all_static_tiers(n: usize) -> Vec<TierEntry> {
        (0..n)
            .map(|_| TierEntry {
                tier: AccessTier::Static,
                strikes: 0,
            })
            .collect()
    }

    /// Thin wrapper so tests can pass plain `usize` indices.
    fn admit_production(
        access: &ReducerAccessInfo,
        resolved: &ResolvedAccess,
        candidate: usize,
        admitted: &[usize],
    ) -> bool {
        let tiers = all_static_tiers(access.lifecycle.len());
        let admitted_ids: Vec<ReducerId> = admitted.iter().map(|&a| ReducerId(a as u32)).collect();
        admit(access, resolved, &tiers, ReducerId(candidate as u32), &admitted_ids)
    }

    /// Build a strict prefix using the production `admit`.
    fn build_prefix_production(access: &ReducerAccessInfo, resolved: &ResolvedAccess, queue: &[usize]) -> Vec<usize> {
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
        let (access, resolved) = make_fixture(&[&[], &[]], &[&["a"], &["b"]], &[false, false], &[false, false]);
        assert!(admit_production(&access, &resolved, 0, &[]));
        assert!(admit_production(&access, &resolved, 1, &[0]));
    }

    #[test]
    fn admission_write_write_conflict_blocks() {
        // both write {a} → conflict.
        let (access, resolved) = make_fixture(&[&[], &[]], &[&["a"], &["a"]], &[false, false], &[false, false]);
        assert!(admit_production(&access, &resolved, 0, &[]));
        assert!(!admit_production(&access, &resolved, 1, &[0]));
    }

    #[test]
    fn admission_read_write_conflict_blocks() {
        // r0 writes {a}; r1 reads {a} → write∩read conflict.
        let (access, resolved) = make_fixture(&[&[], &["a"]], &[&["a"], &["b"]], &[false, false], &[false, false]);
        assert!(!admit_production(&access, &resolved, 1, &[0]));
    }

    #[test]
    fn admission_read_read_admits() {
        // r0 reads {a}, r1 reads {a}, neither writes it → read∩read is free.
        let (access, resolved) = make_fixture(&[&["a"], &["a"]], &[&["x"], &["y"]], &[false, false], &[false, false]);
        assert!(admit_production(&access, &resolved, 1, &[0]));
    }

    #[test]
    fn admission_lifecycle_blocks() {
        let (access, resolved) = make_fixture(&[&[], &[]], &[&["a"], &["b"]], &[false, true], &[false, false]);
        assert!(
            !admit_production(&access, &resolved, 1, &[0]),
            "lifecycle reducer must never batch"
        );
    }

    #[test]
    fn admission_wildcard_blocks() {
        let (access, resolved) = make_fixture(&[&[], &[]], &[&["a"], &["b"]], &[false, false], &[false, true]);
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
        let s = Stat {
            high_water: Duration::from_micros(500),
            ema: Duration::from_micros(50),
        };
        assert!(!s.exceeds(t), "EMA below threshold must block fork");
        // Both above → exceeds.
        let s = Stat {
            high_water: Duration::from_micros(500),
            ema: Duration::from_micros(200),
        };
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

    // ── Tier state machine: helpers ──────────────────────────────────────────

    fn make_observed(reads: &[u32], writes: &[u32]) -> ObservedAccess {
        let mut obs = ObservedAccess::default();
        for &t in reads {
            obs.reads.insert(TableId(t));
        }
        for &t in writes {
            obs.writes.insert(TableId(t));
        }
        obs
    }

    fn unknown_entry() -> TierEntry {
        TierEntry {
            tier: AccessTier::Unknown(ObsState::default()),
            strikes: 0,
        }
    }

    // ── Tier state machine: promotion gate ───────────────────────────────────

    #[test]
    fn tier_promotion_requires_quiet_window() {
        let mut entry = unknown_entry();
        let obs = make_observed(&[], &[1]);
        // Run 6 identical observations — not enough runs yet (need LEARN_MIN_RUNS = 8).
        for i in 0..6u32 {
            let promoted = record_observation(&mut entry, &obs);
            assert!(!promoted, "run {i} should not promote yet");
        }
        // 2 more identical: run 6+1=7 quiet, 6+2=8 total. At run 8, quiet==7≥4 → promote.
        let promoted = record_observation(&mut entry, &obs); // run 7
        assert!(!promoted, "run 7 should not promote (quiet=6 but only 7 runs)");
        let promoted = record_observation(&mut entry, &obs); // run 8, quiet=7≥4
        assert!(promoted, "run 8 with 7 quiet should promote");
        assert!(matches!(entry.tier, AccessTier::Learned(_)));
    }

    #[test]
    fn tier_promotion_requires_both_conditions() {
        // Introduce a brand-new table every 3 runs so quiet always resets before reaching 4.
        // Each fresh TableId has never been in the union, so is_new stays true regularly.
        let mut entry = unknown_entry();
        let stable = make_observed(&[], &[1]);
        for i in 0..30u32 {
            let fresh = make_observed(&[], &[100 + i]);
            let obs = if i % 3 == 2 { &fresh } else { &stable };
            let promoted = record_observation(&mut entry, obs);
            assert!(
                !promoted,
                "run {i} must not promote when truly new tables arrive every 3rd run"
            );
        }
    }

    #[test]
    fn tier_promotion_at_correct_run() {
        // 6 identical runs (quiet=5 after run 6), then 1 new table (quiet reset to 0),
        // then 4 stable runs (quiet reaches 4 at run 11). Promote at run 11.
        let mut entry = unknown_entry();
        let obs_stable = make_observed(&[], &[1]);
        let obs_new = make_observed(&[], &[2]);
        for i in 0..6u32 {
            let p = record_observation(&mut entry, &obs_stable);
            assert!(!p, "run {i} should not promote");
        }
        // run 6: new table, quiet resets.
        let p = record_observation(&mut entry, &obs_new);
        assert!(!p, "run 6 (new table) should not promote");
        // runs 7..10 (4 stable runs): quiet goes 0→1→2→3→4.
        for i in 7..11u32 {
            let p = record_observation(&mut entry, &obs_stable);
            // quiet reaches 4 at i=10, runs=11 ≥ 8 → promote on i=10.
            if i == 10 {
                assert!(p, "should promote at run {i}");
            } else {
                assert!(!p, "should not promote at run {i}");
            }
        }
        assert!(matches!(entry.tier, AccessTier::Learned(_)));
    }

    #[test]
    fn tier_union_semantics_after_promotion() {
        let mut entry = unknown_entry();
        let obs1 = make_observed(&[1], &[10]);
        let obs2 = make_observed(&[2], &[11]);
        let obs3 = make_observed(&[3], &[12]);
        // Build up: 1 each of obs1, obs2, obs3, then obs1 repeatedly until promote.
        for obs in [&obs1, &obs2, &obs3] {
            record_observation(&mut entry, obs);
        }
        // Now run obs1 until promotion (runs=3, need 5 more stable after quiet settles).
        for _ in 0..20 {
            if matches!(entry.tier, AccessTier::Learned(_)) {
                break;
            }
            record_observation(&mut entry, &obs1);
        }
        let AccessTier::Learned(ref ls) = entry.tier else {
            panic!("expected Learned after promotion");
        };
        // Learned set must contain union of all observed reads/writes.
        assert!(ls.reads.contains(&TableId(1)));
        assert!(ls.reads.contains(&TableId(2)));
        assert!(ls.reads.contains(&TableId(3)));
        assert!(ls.writes.contains(&TableId(10)));
        assert!(ls.writes.contains(&TableId(11)));
        assert!(ls.writes.contains(&TableId(12)));
    }

    // ── Tier state machine: record_escape ────────────────────────────────────

    fn promote_entry(entry: &mut TierEntry) {
        let obs = make_observed(&[], &[99]);
        for _ in 0..100 {
            if matches!(entry.tier, AccessTier::Learned(_)) {
                break;
            }
            record_observation(entry, &obs);
        }
        assert!(matches!(entry.tier, AccessTier::Learned(_)), "failed to promote");
    }

    #[test]
    fn tier_escape_demotes_to_unknown_with_seeded_accumulator() {
        let mut entry = unknown_entry();
        // Promote with read=1, write=99.
        let obs_seed = make_observed(&[1], &[99]);
        for _ in 0..100 {
            if matches!(entry.tier, AccessTier::Learned(_)) {
                break;
            }
            record_observation(&mut entry, &obs_seed);
        }
        let escape_obs = make_observed(&[5], &[200]);
        record_escape(&mut entry, &escape_obs);
        assert_eq!(entry.strikes, 1);
        let AccessTier::Unknown(ref obs) = entry.tier else {
            panic!("expected Unknown after first escape");
        };
        // Seed must contain old learned ∪ escape.
        assert!(obs.reads.contains(&TableId(1)));
        assert!(obs.reads.contains(&TableId(5)));
        assert!(obs.writes.contains(&TableId(99)));
        assert!(obs.writes.contains(&TableId(200)));
        assert_eq!(obs.runs, 0);
        assert_eq!(obs.quiet, 0);
    }

    #[test]
    fn tier_second_escape_parks() {
        let mut entry = unknown_entry();
        promote_entry(&mut entry);
        let esc = make_observed(&[], &[1]);
        record_escape(&mut entry, &esc);
        assert_eq!(entry.strikes, 1);
        assert!(matches!(entry.tier, AccessTier::Unknown(_)));
        // Re-promote.
        for _ in 0..100 {
            if matches!(entry.tier, AccessTier::Learned(_)) {
                break;
            }
            record_observation(&mut entry, &esc);
        }
        assert!(matches!(entry.tier, AccessTier::Learned(_)));
        // Second escape → Parked.
        record_escape(&mut entry, &esc);
        assert!(matches!(entry.tier, AccessTier::Parked), "second escape must park");
    }

    #[test]
    fn tier_parked_is_terminal() {
        let mut entry = TierEntry {
            tier: AccessTier::Parked,
            strikes: 2,
        };
        let obs = make_observed(&[], &[1]);
        let promoted = record_observation(&mut entry, &obs);
        assert!(!promoted);
        assert!(matches!(entry.tier, AccessTier::Parked));
        record_escape(&mut entry, &obs);
        assert!(matches!(entry.tier, AccessTier::Parked));
    }

    // ── Tier-aware admit truth table ─────────────────────────────────────────

    fn make_tier_entry(tier: AccessTier) -> TierEntry {
        TierEntry { tier, strikes: 0 }
    }

    fn learned_entry_for(reads: &[u32], writes: &[u32]) -> TierEntry {
        let mut r = IntSet::default();
        let mut w = IntSet::default();
        for &t in reads {
            r.insert(TableId(t));
        }
        for &t in writes {
            w.insert(TableId(t));
        }
        make_tier_entry(AccessTier::Learned(LearnedSet { reads: r, writes: w }))
    }

    fn admit_with_tiers(
        access: &ReducerAccessInfo,
        resolved: &ResolvedAccess,
        tiers: &[TierEntry],
        candidate: usize,
        admitted: &[usize],
    ) -> bool {
        let admitted_ids: Vec<ReducerId> = admitted.iter().map(|&a| ReducerId(a as u32)).collect();
        admit(access, resolved, tiers, ReducerId(candidate as u32), &admitted_ids)
    }

    #[test]
    fn tier_admit_static_static_unchanged() {
        // Static×Static should behave identically to the all-static baseline.
        let (access, resolved) = make_fixture(&[&[], &[]], &[&["a"], &["b"]], &[false, false], &[false, false]);
        let tiers = all_static_tiers(2);
        assert!(admit_with_tiers(&access, &resolved, &tiers, 0, &[]));
        assert!(admit_with_tiers(&access, &resolved, &tiers, 1, &[0]));

        let (access2, resolved2) = make_fixture(&[&[], &[]], &[&["a"], &["a"]], &[false, false], &[false, false]);
        let tiers2 = all_static_tiers(2);
        assert!(admit_with_tiers(&access2, &resolved2, &tiers2, 0, &[]));
        assert!(!admit_with_tiers(&access2, &resolved2, &tiers2, 1, &[0]));
    }

    #[test]
    fn tier_admit_unknown_and_parked_always_rejected() {
        let (access, resolved) = make_fixture(&[&[], &[]], &[&["a"], &["b"]], &[false, false], &[false, false]);
        let tiers_unknown = vec![
            make_tier_entry(AccessTier::Static),
            make_tier_entry(AccessTier::Unknown(ObsState::default())),
        ];
        let tiers_parked = vec![make_tier_entry(AccessTier::Static), make_tier_entry(AccessTier::Parked)];
        assert!(!admit_with_tiers(&access, &resolved, &tiers_unknown, 1, &[0]));
        assert!(!admit_with_tiers(&access, &resolved, &tiers_parked, 1, &[0]));
    }

    #[test]
    fn tier_admit_learned_static_conflict_write_vs_read() {
        // Candidate Learned(writes={1}), admitted Static(reads={1}) → conflict.
        let (access, resolved) = make_fixture(&[&["t1"], &[]], &[&[], &["t1"]], &[false, false], &[false, false]);
        let tiers = vec![
            make_tier_entry(AccessTier::Static),
            learned_entry_for(&[], &[0]), // table id 0 = "t1"
        ];
        // candidate=1 (Learned writes {0}), admitted=[0] (Static reads {0}): conflict.
        assert!(!admit_with_tiers(&access, &resolved, &tiers, 1, &[0]));
    }

    #[test]
    fn tier_admit_static_learned_conflict_write_vs_read() {
        // Candidate Static(writes={t1}), admitted Learned(reads={t1}) → conflict.
        let (access, resolved) = make_fixture(&[&[], &[]], &[&["t1"], &[]], &[false, false], &[false, false]);
        let tiers = vec![
            make_tier_entry(AccessTier::Static),
            learned_entry_for(&[0], &[]), // Learned reads table 0 = "t1"
        ];
        // candidate=0 (Static writes {0}), admitted=[1] (Learned reads {0}): conflict.
        assert!(!admit_with_tiers(&access, &resolved, &tiers, 0, &[1]));
    }

    #[test]
    fn tier_admit_learned_static_disjoint_admits() {
        // Candidate Learned(writes={99}), admitted Static(reads/writes={0..10}): disjoint.
        let (access, resolved) = make_fixture(&[&["t1"], &[]], &[&["t2"], &[]], &[false, false], &[false, false]);
        let tiers = vec![make_tier_entry(AccessTier::Static), learned_entry_for(&[], &[99])];
        assert!(admit_with_tiers(&access, &resolved, &tiers, 1, &[0]));
    }

    #[test]
    fn tier_admit_learned_vs_unresolved_static_rejected() {
        // Admitted side is Static with a name-resolution wildcard (analyzer OK, resolution
        // failed): its resolved slices are empty, but its true access is unknown — the
        // Learned candidate must be rejected, never set-intersected against empty slices.
        let (access, resolved) = make_fixture(&[&["t1"], &[]], &[&["t2"], &[]], &[false, false], &[true, false]);
        let tiers = vec![make_tier_entry(AccessTier::Static), learned_entry_for(&[], &[99])];
        assert!(!admit_with_tiers(&access, &resolved, &tiers, 1, &[0]));
    }

    #[test]
    fn tier_admit_learned_learned_write_write_conflict() {
        // Both Learned write table 42 → conflict.
        let (access, resolved) = make_fixture(&[&[], &[]], &[&[], &[]], &[false, false], &[false, false]);
        let tiers = vec![learned_entry_for(&[], &[42]), learned_entry_for(&[], &[42])];
        assert!(!admit_with_tiers(&access, &resolved, &tiers, 1, &[0]));
    }

    #[test]
    fn tier_admit_learned_learned_disjoint_admits() {
        let (access, resolved) = make_fixture(&[&[], &[]], &[&[], &[]], &[false, false], &[false, false]);
        let tiers = vec![learned_entry_for(&[], &[1]), learned_entry_for(&[], &[2])];
        assert!(admit_with_tiers(&access, &resolved, &tiers, 1, &[0]));
    }

    // ── fork_eligible tier gating ────────────────────────────────────────────

    #[test]
    fn tier_fork_eligible_gating() {
        let static_entry = make_tier_entry(AccessTier::Static);
        let learned_entry = learned_entry_for(&[], &[1]);
        let unknown_entry = make_tier_entry(AccessTier::Unknown(ObsState::default()));
        let parked_entry = make_tier_entry(AccessTier::Parked);

        assert!(tier_fork_eligible(&static_entry), "Static must be fork_eligible");
        assert!(tier_fork_eligible(&learned_entry), "Learned must be fork_eligible");
        assert!(!tier_fork_eligible(&unknown_entry), "Unknown must not be fork_eligible");
        assert!(!tier_fork_eligible(&parked_entry), "Parked must not be fork_eligible");
    }

    // ── Early-cut trap helpers (U5) ──────────────────────────────────────────

    fn learned(reads: &[u32], writes: &[u32]) -> LearnedSet {
        let mut r = IntSet::default();
        let mut w = IntSet::default();
        for &t in reads {
            r.insert(TableId(t));
        }
        for &t in writes {
            w.insert(TableId(t));
        }
        LearnedSet { reads: r, writes: w }
    }

    fn write_union(tables: &[u32]) -> IntSet<TableId> {
        tables.iter().map(|&t| TableId(t)).collect()
    }

    #[test]
    fn escape_check_truth_table() {
        let ls = learned(&[1, 2], &[10, 11]);
        // Subset of the learned set → no escape.
        assert!(!escape_check(&make_observed(&[1], &[10]), &ls), "subset → false");
        assert!(!escape_check(&make_observed(&[1, 2], &[10, 11]), &ls), "exact → false");
        // A new read not in learned.reads → escape.
        assert!(escape_check(&make_observed(&[3], &[10]), &ls), "new read → true");
        // A new write not in learned.writes → escape.
        assert!(escape_check(&make_observed(&[1], &[99]), &ls), "new write → true");
        // Empty observed → no escape.
        assert!(!escape_check(&make_observed(&[], &[]), &ls), "empty observed → false");
    }

    #[test]
    fn hazard_check_read_write_disjoint() {
        let earlier = write_union(&[10, 11]);
        // Read hits an earlier write.
        assert!(hazard_check(&make_observed(&[10], &[]), &earlier), "read-hit → true");
        // Write hits an earlier write.
        assert!(hazard_check(&make_observed(&[], &[11]), &earlier), "write-hit → true");
        // Disjoint reads and writes.
        assert!(
            !hazard_check(&make_observed(&[1, 2], &[3, 4]), &earlier),
            "disjoint → false"
        );
        // Empty observed.
        assert!(!hazard_check(&make_observed(&[], &[]), &earlier), "empty → false");
    }

    #[test]
    fn member_trap_decision_precedence() {
        use MemberTrapDecision::*;
        // View trap dominates regardless of escape/conflict.
        assert_eq!(member_trap_decision(true, true, true), RequeueViewTrap);
        assert_eq!(member_trap_decision(true, false, false), RequeueViewTrap);
        // Conflicting escape (no view trap).
        assert_eq!(member_trap_decision(false, true, true), RequeueConflict);
        // Harmless escape: escaped but disjoint → keep but cut.
        assert_eq!(member_trap_decision(false, true, false), KeepHarmlessEscape);
        // No trap.
        assert_eq!(member_trap_decision(false, false, false), Keep);
        // conflicting can't be true without escaped in production, but the fn is total.
        assert_eq!(member_trap_decision(false, false, true), Keep);
    }

    #[test]
    fn record_escape_demotes_then_parks() {
        let mut entry = unknown_entry();
        promote_entry(&mut entry);
        let esc = make_observed(&[], &[7]);
        // First escape demotes.
        assert_eq!(
            record_escape(&mut entry, &esc),
            EscapeOutcome::Demoted,
            "first escape → Demoted"
        );
        assert!(matches!(entry.tier, AccessTier::Unknown(_)));
        // Re-promote then escape again → Parked.
        for _ in 0..100 {
            if matches!(entry.tier, AccessTier::Learned(_)) {
                break;
            }
            record_observation(&mut entry, &esc);
        }
        assert!(matches!(entry.tier, AccessTier::Learned(_)), "re-promote failed");
        assert_eq!(
            record_escape(&mut entry, &esc),
            EscapeOutcome::Parked,
            "second escape → Parked"
        );
        assert!(matches!(entry.tier, AccessTier::Parked));
    }

    // head_cut_index: build per-member (reads, writes) TableSet pairs from owned IntSets.
    fn member_sets(shapes: &[(Vec<u32>, Vec<u32>)]) -> (Vec<IntSet<TableId>>, Vec<IntSet<TableId>>) {
        let reads = shapes
            .iter()
            .map(|(r, _)| r.iter().map(|&t| TableId(t)).collect())
            .collect();
        let writes = shapes
            .iter()
            .map(|(_, w)| w.iter().map(|&t| TableId(t)).collect())
            .collect();
        (reads, writes)
    }

    fn pairs<'a>(reads: &'a [IntSet<TableId>], writes: &'a [IntSet<TableId>]) -> Vec<(TableSet<'a>, TableSet<'a>)> {
        reads
            .iter()
            .zip(writes)
            .map(|(r, w)| (TableSet::Set(r), TableSet::Set(w)))
            .collect()
    }

    #[test]
    fn head_cut_index_no_conflict() {
        let head = write_union(&[100]);
        let (r, w) = member_sets(&[(vec![1], vec![2]), (vec![3], vec![4])]);
        assert_eq!(
            head_cut_index(&head, pairs(&r, &w).into_iter()),
            None,
            "no conflicts → None"
        );
    }

    #[test]
    fn head_cut_index_first_conflict_via_write() {
        // Head wrote {5}; member 1 writes 5 → cut at 1.
        let head = write_union(&[5]);
        let (r, w) = member_sets(&[(vec![1], vec![2]), (vec![3], vec![5]), (vec![6], vec![7])]);
        assert_eq!(
            head_cut_index(&head, pairs(&r, &w).into_iter()),
            Some(1),
            "first write conflict at 1"
        );
    }

    #[test]
    fn head_cut_index_conflict_via_read() {
        // Head wrote {9}; member 0 reads 9 (write-vs-read hazard) → cut at 0.
        let head = write_union(&[9]);
        let (r, w) = member_sets(&[(vec![9], vec![2]), (vec![3], vec![4])]);
        assert_eq!(
            head_cut_index(&head, pairs(&r, &w).into_iter()),
            Some(0),
            "read conflict at 0"
        );
    }

    #[test]
    fn head_cut_index_mixed_shapes() {
        // Head wrote {20}; member 2 reads 20 → cut at 2, prefix [0,1) clean.
        let head = write_union(&[20]);
        let (r, w) = member_sets(&[(vec![1], vec![2]), (vec![3], vec![4]), (vec![20], vec![5])]);
        assert_eq!(
            head_cut_index(&head, pairs(&r, &w).into_iter()),
            Some(2),
            "first conflict at 2"
        );
    }

    #[test]
    fn push_front_in_order_preserves_order() {
        // pending already holds [9]; requeue [1,2,3] at front → [1,2,3,9].
        let mut pending: VecDeque<usize> = VecDeque::from([9]);
        push_front_in_order(&mut pending, vec![1, 2, 3]);
        assert_eq!(
            pending.into_iter().collect::<Vec<_>>(),
            vec![1, 2, 3, 9],
            "head first, then members in order"
        );
    }

    #[test]
    fn push_front_in_order_head_then_members() {
        // Models the whole-batch view abort requeue: [head, m1, m2] lands front-most in order.
        let mut pending: VecDeque<usize> = VecDeque::new();
        let jobs = vec![0 /*head*/, 1, 2];
        push_front_in_order(&mut pending, jobs);
        assert_eq!(
            pending.into_iter().collect::<Vec<_>>(),
            vec![0, 1, 2],
            "FIFO serial order restored"
        );
    }
}
