//! Shadow-mode access-prediction harness (measurement only).
//!
//! When `STDB_SHADOW_ACCESS` is set, the host statically analyzes each reducer's
//! table access set at publish time, then compares those predictions against the
//! tables actually observed at runtime and simulates the batch widths a
//! disjointness-based scheduler *would* have produced. It never changes
//! scheduling or behavior; absent the env var everything here is inert.

use std::collections::{HashMap, HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use spacetimedb_access_analysis::matrix::ConflictMatrix;
use spacetimedb_access_analysis::AccessSet;
use spacetimedb_primitives::ReducerId;

/// Names of tables a reducer touched, resolved by the caller from `TableId`s.
pub struct ObservedNames {
    pub reads: Vec<Box<str>>,
    pub writes: Vec<Box<str>>,
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
}

/// Whether shadow access mode is enabled. Read once.
pub fn shadow_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| env_flag("STDB_SHADOW_ACCESS"))
}

fn report_dir() -> Option<PathBuf> {
    static DIR: OnceLock<Option<PathBuf>> = OnceLock::new();
    DIR.get_or_init(|| std::env::var_os("STDB_SHADOW_ACCESS_REPORT").map(PathBuf::from))
        .clone()
}

fn report_interval() -> Duration {
    static SECS: OnceLock<u64> = OnceLock::new();
    let secs = SECS.get_or_init(|| {
        std::env::var("STDB_SHADOW_ACCESS_REPORT_INTERVAL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(60)
    });
    Duration::from_secs(*secs)
}

const MAX_UNDER_DETAILS_PER_REDUCER: usize = 100;
const MAX_BATCH_RECORDS: usize = 10_000;
const RUNTIME_BUCKETS: usize = 24;

/// log2-bucketed runtime histogram, ~1µs..16s across 24 buckets.
#[derive(Default, serde::Serialize)]
struct RuntimeHistogram {
    count: u64,
    sum_ns: u64,
    max_ns: u64,
    buckets: [u64; RUNTIME_BUCKETS],
}

impl RuntimeHistogram {
    fn record(&mut self, ns: u64) {
        self.count += 1;
        self.sum_ns = self.sum_ns.saturating_add(ns);
        self.max_ns = self.max_ns.max(ns);
        // Bucket 0 holds <2µs; each later bucket doubles, last bucket is saturating.
        let bucket = if ns < 1000 {
            0
        } else {
            let log = 63 - (ns / 1000).leading_zeros() as usize;
            (log + 1).min(RUNTIME_BUCKETS - 1)
        };
        self.buckets[bucket] += 1;
    }
}

#[derive(serde::Serialize)]
struct UnderApproxDetail {
    table: String,
    kind: &'static str,
}

#[derive(Default)]
struct ReducerStats {
    calls: u64,
    under_approx_count: u64,
    under_approx_details: Vec<UnderApproxDetail>,
    over_approx_tables: HashMap<String, u64>,
    runtime: RuntimeHistogram,
}

struct ShadowInner {
    reducers: Vec<ReducerStats>,
    sim: PrefixBatchSim,
    last_report: Instant,
}

pub struct ShadowAccess {
    sets: Vec<AccessSet>,
    matrix: ConflictMatrix,
    reducer_names: Vec<String>,
    database_identity: String,
    /// Lower bound on fork/handoff cost: a bare thread round-trip, with no cold
    /// wasm instantiation, so true fork cost is strictly larger.
    handoff_proxy_ns_lower_bound: u64,
    report_dir: Option<PathBuf>,
    seq: AtomicU64,
    inner: Mutex<ShadowInner>,
}

impl ShadowAccess {
    pub fn new(sets: Vec<AccessSet>, reducer_names: Vec<String>, database_identity: String) -> Self {
        let matrix = ConflictMatrix::build(&sets);
        let handoff_proxy_ns_lower_bound = measure_handoff_proxy();
        let n = reducer_names.len();
        let inner = ShadowInner {
            reducers: (0..n).map(|_| ReducerStats::default()).collect(),
            sim: PrefixBatchSim::default(),
            last_report: Instant::now(),
        };
        ShadowAccess {
            sets,
            matrix,
            reducer_names,
            database_identity,
            handoff_proxy_ns_lower_bound,
            report_dir: report_dir(),
            seq: AtomicU64::new(0),
            inner: Mutex::new(inner),
        }
    }

    /// Register an enqueued wasm reducer; returns its mirror sequence number.
    /// Called from any thread (the enqueue producer lanes).
    pub fn register_enqueue(&self, reducer_id: ReducerId) -> u64 {
        let mut inner = self.inner.lock().unwrap();
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        inner.sim.mirror.push_back((seq, reducer_id, Instant::now()));
        seq
    }

    /// Process an executed reducer. Called ONLY from the single wasm executor thread.
    pub fn on_executed(
        &self,
        seq: Option<u64>,
        reducer_id: ReducerId,
        duration: Duration,
        observed: Option<ObservedNames>,
    ) {
        let idx = reducer_id.idx();
        let mut inner = self.inner.lock().unwrap();

        if let (Some(observed), Some(set)) = (&observed, self.sets.get(idx)) {
            if !set.wildcard {
                self.diff_prediction(&mut inner, idx, set, observed);
            }
        }

        let runtime_ns = duration.as_nanos().min(u64::MAX as u128) as u64;
        inner.sim.on_execute(seq, reducer_id, runtime_ns, &self.matrix);

        if let Some(stats) = inner.reducers.get_mut(idx) {
            stats.calls += 1;
            stats.runtime.record(runtime_ns);
        }

        if inner.last_report.elapsed() >= report_interval() {
            inner.last_report = Instant::now();
            drop(inner);
            self.write_report();
        }
    }

    fn diff_prediction(&self, inner: &mut ShadowInner, idx: usize, set: &AccessSet, observed: &ObservedNames) {
        // Compare against the observed name as a string, NOT a parsed Identifier:
        // system table names may not be valid Identifiers and must still compare
        // (and never panic) rather than being silently dropped.
        let predicted_has = |coll: &std::collections::BTreeSet<spacetimedb_schema::identifier::Identifier>,
                             name: &str| coll.iter().any(|i| &**i == name);

        let mut record_under = |table: &str, kind: &'static str| {
            log::error!(
                "shadow access UNDER-APPROXIMATION: db={} reducer={} table={} kind={}",
                self.database_identity,
                self.reducer_names.get(idx).map(String::as_str).unwrap_or("?"),
                table,
                kind,
            );
            if let Some(stats) = inner.reducers.get_mut(idx) {
                stats.under_approx_count += 1;
                if stats.under_approx_details.len() < MAX_UNDER_DETAILS_PER_REDUCER {
                    stats.under_approx_details.push(UnderApproxDetail {
                        table: table.to_owned(),
                        kind,
                    });
                }
            }
        };

        for name in &observed.reads {
            if !predicted_has(&set.reads, name) && !predicted_has(&set.writes, name) {
                record_under(name, "read");
            }
        }
        for name in &observed.writes {
            if !predicted_has(&set.writes, name) {
                record_under(name, "write");
            }
        }

        // Over-approximation: predicted tables never observed this invocation.
        if let Some(stats) = inner.reducers.get_mut(idx) {
            let observed_any = |name: &str| {
                observed.reads.iter().any(|n| &**n == name) || observed.writes.iter().any(|n| &**n == name)
            };
            let mut counted = HashSet::new();
            for table in set.reads.iter().chain(set.writes.iter()) {
                if !observed_any(table) && counted.insert(table) {
                    *stats.over_approx_tables.entry(table.to_string()).or_insert(0) += 1;
                }
            }
        }
    }

    /// Write the report to disk (if a report dir is configured) and log a summary.
    pub fn write_report(&self) {
        let inner = self.inner.lock().unwrap();

        let total_under: u64 = inner.reducers.iter().map(|r| r.under_approx_count).sum();
        log::info!(
            "shadow access summary: db={} total_under_approx={} batch_width_histogram={:?} barriers={}",
            self.database_identity,
            total_under,
            inner.sim.width_histogram,
            inner.sim.barriers,
        );

        let Some(dir) = &self.report_dir else {
            return;
        };

        let report = Report {
            database: &self.database_identity,
            handoff_proxy_ns_lower_bound: self.handoff_proxy_ns_lower_bound,
            reducers: inner
                .reducers
                .iter()
                .enumerate()
                .map(|(i, stats)| {
                    let set = self.sets.get(i);
                    ReducerReport {
                        id: i as u32,
                        name: self.reducer_names.get(i).map(String::as_str).unwrap_or(""),
                        wildcard: set.map(|s| s.wildcard).unwrap_or(false),
                        predicted_reads: set
                            .map(|s| s.reads.iter().map(|i| i.to_string()).collect())
                            .unwrap_or_default(),
                        predicted_writes: set
                            .map(|s| s.writes.iter().map(|i| i.to_string()).collect())
                            .unwrap_or_default(),
                        calls: stats.calls,
                        under_approx_count: stats.under_approx_count,
                        under_approx_details: &stats.under_approx_details,
                        over_approx_tables: &stats.over_approx_tables,
                        runtime: &stats.runtime,
                    }
                })
                .collect(),
            batches: BatchReport {
                width_histogram: &inner.sim.width_histogram,
                records: &inner.sim.records,
                dropped_records: inner.sim.dropped_records,
            },
            barriers: inner.sim.barriers,
        };

        if let Err(e) = write_report_atomic(dir, &self.database_identity, &report) {
            log::warn!("shadow access: failed to write report: {e}");
        }
    }
}

impl Drop for ShadowAccess {
    fn drop(&mut self) {
        self.write_report();
    }
}

fn write_report_atomic(dir: &std::path::Path, database: &str, report: &Report) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let final_path = dir.join(format!("{database}.json"));
    let tmp_path = dir.join(format!("{database}.json.tmp"));
    {
        let file = std::fs::File::create(&tmp_path)?;
        let mut writer = std::io::BufWriter::new(file);
        serde_json::to_writer(&mut writer, report).map_err(std::io::Error::other)?;
        use std::io::Write as _;
        writer.flush()?;
    }
    std::fs::rename(&tmp_path, &final_path)
}

/// Measure a lower bound on cross-thread handoff cost: the median round-trip of
/// shipping a boxed no-op closure to a worker thread and getting an ack back.
/// No cold wasm instance is involved, so true fork cost is strictly larger.
fn measure_handoff_proxy() -> u64 {
    use std::sync::mpsc;

    let (job_tx, job_rx) = mpsc::channel::<Box<dyn FnOnce() + Send>>();
    let (ack_tx, ack_rx) = mpsc::channel::<()>();

    let worker = std::thread::spawn(move || {
        while let Ok(job) = job_rx.recv() {
            job();
            if ack_tx.send(()).is_err() {
                break;
            }
        }
    });

    let round_trip = |job_tx: &mpsc::Sender<Box<dyn FnOnce() + Send>>, ack_rx: &mpsc::Receiver<()>| {
        let _ = job_tx.send(Box::new(|| {}));
        let _ = ack_rx.recv();
    };

    for _ in 0..100 {
        round_trip(&job_tx, &ack_rx);
    }

    let mut samples = Vec::with_capacity(1000);
    for _ in 0..1000 {
        let start = Instant::now();
        round_trip(&job_tx, &ack_rx);
        samples.push(start.elapsed().as_nanos().min(u64::MAX as u128) as u64);
    }

    drop(job_tx);
    let _ = worker.join();

    samples.sort_unstable();
    samples.get(samples.len() / 2).copied().unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Batch-width simulation (pure: std + ReducerId + &ConflictMatrix, no I/O).
// ---------------------------------------------------------------------------

#[derive(serde::Serialize)]
struct BatchRecord {
    members: Vec<u32>,
    runtimes_ns: Vec<u64>,
}

struct CurBatch {
    member_seqs: HashSet<u64>,
    executed: Vec<(ReducerId, u64)>,
    width: usize,
}

#[derive(Default)]
struct PrefixBatchSim {
    mirror: VecDeque<(u64, ReducerId, Instant)>,
    current_batch: Option<CurBatch>,
    width_histogram: HashMap<usize, u64>,
    records: Vec<BatchRecord>,
    barriers: u64,
    dropped_records: u64,
}

impl PrefixBatchSim {
    fn on_execute(&mut self, seq: Option<u64>, reducer_id: ReducerId, runtime_ns: u64, matrix: &ConflictMatrix) {
        let Some(s) = seq else {
            // seq=None: scheduler/init/lifecycle work bypassed mirror registration.
            // Treat it as a barrier so unknown work never inflates simulated widths.
            self.close_current_batch();
            self.barriers += 1;
            return;
        };

        let pos = self.mirror.iter().position(|(ms, _, _)| *ms == s);
        let Some(pos) = pos else {
            // Desync guard: a seq we never registered behaves like the None barrier.
            self.close_current_batch();
            self.barriers += 1;
            return;
        };
        self.mirror.remove(pos);

        if let Some(batch) = &mut self.current_batch {
            if batch.member_seqs.contains(&s) {
                batch.executed.push((reducer_id, runtime_ns));
                if batch.executed.len() == batch.width {
                    self.close_current_batch();
                }
                return;
            }
        }

        self.close_current_batch();

        let head_idx = reducer_id.idx();
        let mut member_seqs = HashSet::new();
        member_seqs.insert(s);
        let mut members = vec![head_idx];

        // Strict contiguous prefix: walk the mirror in seq (FIFO) order, admitting
        // entries that conflict with NO current member; STOP at the first conflict.
        // This is strict-prefix batching by design, not max-independent-set.
        for (ms, mid, _) in self.mirror.iter() {
            let cand = mid.idx();
            let conflicts = members.iter().any(|&m| matrix.conflicts(m, cand));
            if conflicts {
                break;
            }
            member_seqs.insert(*ms);
            members.push(cand);
        }

        let width = members.len();
        self.current_batch = Some(CurBatch {
            member_seqs,
            executed: vec![(reducer_id, runtime_ns)],
            width,
        });

        if width == 1 {
            self.close_current_batch();
        }
    }

    fn close_current_batch(&mut self) {
        let Some(batch) = self.current_batch.take() else {
            return;
        };
        let width = batch.width;
        *self.width_histogram.entry(width).or_insert(0) += 1;
        if self.records.len() < MAX_BATCH_RECORDS {
            self.records.push(BatchRecord {
                members: batch.executed.iter().map(|(r, _)| r.0).collect(),
                runtimes_ns: batch.executed.iter().map(|(_, ns)| *ns).collect(),
            });
        } else {
            self.dropped_records += 1;
        }
    }
}

// ---------------------------------------------------------------------------
// Serialize structs for the report.
// ---------------------------------------------------------------------------

#[derive(serde::Serialize)]
struct Report<'a> {
    database: &'a str,
    handoff_proxy_ns_lower_bound: u64,
    reducers: Vec<ReducerReport<'a>>,
    batches: BatchReport<'a>,
    barriers: u64,
}

#[derive(serde::Serialize)]
struct ReducerReport<'a> {
    id: u32,
    name: &'a str,
    wildcard: bool,
    predicted_reads: Vec<String>,
    predicted_writes: Vec<String>,
    calls: u64,
    under_approx_count: u64,
    under_approx_details: &'a [UnderApproxDetail],
    over_approx_tables: &'a HashMap<String, u64>,
    runtime: &'a RuntimeHistogram,
}

#[derive(serde::Serialize)]
struct BatchReport<'a> {
    width_histogram: &'a HashMap<usize, u64>,
    records: &'a [BatchRecord],
    dropped_records: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use spacetimedb_schema::identifier::Identifier;
    use std::collections::BTreeSet;

    fn id(name: &str) -> Identifier {
        Identifier::for_test(name)
    }

    fn reads(tables: &[&str]) -> AccessSet {
        AccessSet {
            reads: tables.iter().map(|s| id(s)).collect::<BTreeSet<_>>(),
            writes: BTreeSet::new(),
            wildcard: false,
        }
    }

    fn writes(tables: &[&str]) -> AccessSet {
        AccessSet {
            reads: BTreeSet::new(),
            writes: tables.iter().map(|s| id(s)).collect::<BTreeSet<_>>(),
            wildcard: false,
        }
    }

    fn rid(i: u32) -> ReducerId {
        ReducerId(i)
    }

    /// Enqueue mirror entries for the given reducer ids, returning their seqs.
    fn enqueue(sim: &mut PrefixBatchSim, ids: &[u32]) -> Vec<u64> {
        let mut seqs = Vec::new();
        for (i, &id) in ids.iter().enumerate() {
            let seq = i as u64;
            sim.mirror.push_back((seq, rid(id), Instant::now()));
            seqs.push(seq);
        }
        seqs
    }

    fn width_of_only_record(sim: &PrefixBatchSim) -> Option<usize> {
        sim.width_histogram.iter().find(|&(_, &c)| c > 0).map(|(&w, _)| w)
    }

    #[test]
    fn burst_of_disjoint_reducers_forms_full_width_batch() {
        // Three reducers writing disjoint tables: all admitted into one batch.
        let sets = [writes(&["a"]), writes(&["b"]), writes(&["c"])];
        let matrix = ConflictMatrix::build(&sets);
        let mut sim = PrefixBatchSim::default();

        let seqs = enqueue(&mut sim, &[0, 1, 2]);
        // Head executes, admitting the rest as a contiguous disjoint prefix.
        sim.on_execute(Some(seqs[0]), rid(0), 10, &matrix);
        sim.on_execute(Some(seqs[1]), rid(1), 10, &matrix);
        sim.on_execute(Some(seqs[2]), rid(2), 10, &matrix);

        assert_eq!(*sim.width_histogram.get(&3).unwrap(), 1);
        assert_eq!(sim.records.len(), 1);
        assert_eq!(sim.records[0].members, vec![0, 1, 2]);
    }

    #[test]
    fn prefix_stops_at_first_conflict() {
        // a, a (conflict), b: strict prefix stops at the conflicting second entry.
        let sets = [writes(&["a"]), writes(&["b"])];
        let matrix = ConflictMatrix::build(&sets);
        let mut sim = PrefixBatchSim::default();

        // reducers: 0 (writes a), 0 (writes a, self-conflict), 1 (writes b)
        let seqs = enqueue(&mut sim, &[0, 0, 1]);
        sim.on_execute(Some(seqs[0]), rid(0), 10, &matrix);
        // Head reducer 0 conflicts with the next reducer 0 -> width 1.
        assert_eq!(*sim.width_histogram.get(&1).unwrap(), 1);

        // Next execution forms a new batch starting at the second entry (0),
        // which conflicts with nothing ahead except itself; b follows disjoint.
        sim.on_execute(Some(seqs[1]), rid(0), 10, &matrix);
        sim.on_execute(Some(seqs[2]), rid(1), 10, &matrix);
        // Second batch: head 0 vs b -> disjoint -> width 2.
        assert_eq!(*sim.width_histogram.get(&2).unwrap(), 1);
    }

    #[test]
    fn none_seq_closes_batch_and_counts_barrier() {
        let sets = [writes(&["a"]), writes(&["b"])];
        let matrix = ConflictMatrix::build(&sets);
        let mut sim = PrefixBatchSim::default();

        let seqs = enqueue(&mut sim, &[0, 1]);
        // Start a batch of width 2 (disjoint a, b).
        sim.on_execute(Some(seqs[0]), rid(0), 10, &matrix);
        // A None-seq execution arrives before the admitted member ran.
        sim.on_execute(None, rid(0), 10, &matrix);
        assert_eq!(sim.barriers, 1);
        // The open batch was closed and emitted, recording only the executed head.
        assert_eq!(*sim.width_histogram.get(&2).unwrap(), 1);
        assert_eq!(sim.records[0].members, vec![0]);
    }

    #[test]
    fn empty_mirror_yields_width_one_batches() {
        let sets = [writes(&["a"])];
        let matrix = ConflictMatrix::build(&sets);
        let mut sim = PrefixBatchSim::default();

        // Execute a registered reducer with nothing else queued behind it.
        let seqs = enqueue(&mut sim, &[0]);
        sim.on_execute(Some(seqs[0]), rid(0), 10, &matrix);
        assert_eq!(width_of_only_record(&sim), Some(1));
    }

    #[test]
    fn same_writer_self_conflicts_to_width_one() {
        // A writer self-conflicts (writes∩writes), so a burst of the same writer
        // never batches.
        let sets = [writes(&["a"])];
        let matrix = ConflictMatrix::build(&sets);
        let mut sim = PrefixBatchSim::default();

        let seqs = enqueue(&mut sim, &[0, 0, 0]);
        sim.on_execute(Some(seqs[0]), rid(0), 10, &matrix);
        sim.on_execute(Some(seqs[1]), rid(0), 10, &matrix);
        sim.on_execute(Some(seqs[2]), rid(0), 10, &matrix);
        assert_eq!(*sim.width_histogram.get(&1).unwrap(), 3);
        assert!(sim.width_histogram.get(&2).is_none());
    }

    #[test]
    fn member_executions_consume_batch_without_new_batches() {
        // A 3-wide disjoint batch is consumed by exactly 3 executions, emitting one record.
        let sets = [reads(&["a"]), reads(&["b"]), reads(&["c"])];
        let matrix = ConflictMatrix::build(&sets);
        let mut sim = PrefixBatchSim::default();

        let seqs = enqueue(&mut sim, &[0, 1, 2]);
        sim.on_execute(Some(seqs[0]), rid(0), 1, &matrix);
        // After head, mirror holds only the two admitted members.
        assert!(sim.current_batch.is_some());
        sim.on_execute(Some(seqs[1]), rid(1), 2, &matrix);
        assert!(sim.current_batch.is_some());
        sim.on_execute(Some(seqs[2]), rid(2), 3, &matrix);
        // Final member closes the batch; exactly one record, no extra batches.
        assert!(sim.current_batch.is_none());
        assert_eq!(sim.records.len(), 1);
        assert_eq!(sim.records[0].runtimes_ns, vec![1, 2, 3]);
        assert_eq!(sim.width_histogram.values().sum::<u64>(), 1);
    }
}
