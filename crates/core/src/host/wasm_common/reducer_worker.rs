use crate::db::relational_db::RelationalDB;
use crate::host::module_host::CallReducerParams;
use crate::host::wasm_common::module_host_actor::{BatchBodyOutcome, CaptureSpec, WasmInstance, WasmModuleInstance};
use spacetimedb_datastore::execution_context::Workload;
use spacetimedb_datastore::locking_tx_datastore::batch_tx::BatchTxState;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

// ─── Public handle types ──────────────────────────────────────────────────────

pub(crate) struct ReducerWorker {
    job_tx: Sender<WorkerJob>,
    ready: Arc<AtomicBool>,
    join: Option<thread::JoinHandle<()>>,
}

pub(crate) struct BatchRunJob {
    pub params: CallReducerParams,
    pub tx: BatchTxState,
    pub capture: CaptureSpec,
}

pub(crate) struct BatchRunReply {
    pub outcome: BatchBodyOutcome,
}

pub(crate) enum WorkerGone {
    Exited,
}

// ─── Internal channel message ─────────────────────────────────────────────────

enum WorkerJob {
    Calibrate {
        reply: Sender<()>,
    },
    Run {
        job: Box<BatchRunJob>,
        reply: Sender<BatchRunReply>,
    },
}

// ─── Thin internal trait for testability ─────────────────────────────────────

/// Not `Send` — values live entirely on the worker thread (created inside `make_instance`).
#[allow(dead_code)] // production worker calls `call_reducer_body_batch` directly; trait is test-only.
trait RunReducerBody: 'static {
    fn run_body(&mut self, tx: BatchTxState, params: CallReducerParams, capture: CaptureSpec) -> BatchBodyOutcome;
    fn needs_replacement(&self) -> bool;
}

impl<I: WasmInstance + 'static> RunReducerBody for WasmModuleInstance<I> {
    fn run_body(&mut self, tx: BatchTxState, params: CallReducerParams, capture: CaptureSpec) -> BatchBodyOutcome {
        self.call_reducer_body_batch(tx, params, capture)
    }

    fn needs_replacement(&self) -> bool {
        self.trapped()
    }
}

// ─── ReducerWorker impl ───────────────────────────────────────────────────────

impl ReducerWorker {
    /// `make_instance` runs on the worker thread at startup and after a trap (self-heal).
    /// Generic only here; the handle is non-generic because channels carry concrete payloads.
    pub fn spawn<I, F>(make_instance: F, relational_db: Arc<RelationalDB>, thread_name: String) -> Self
    where
        I: WasmInstance + 'static,
        F: Fn() -> WasmModuleInstance<I> + Send + 'static,
    {
        let (job_tx, job_rx) = mpsc::channel::<WorkerJob>();
        let ready = Arc::new(AtomicBool::new(false));
        let ready_clone = Arc::clone(&ready);

        let join = thread::Builder::new()
            .name(thread_name)
            .spawn(move || {
                run_worker_loop(make_instance, relational_db, job_rx, ready_clone);
            })
            .expect("failed to spawn reducer worker thread");

        ReducerWorker {
            job_tx,
            ready,
            join: Some(join),
        }
    }

    /// Non-blocking poll: true once the worker is ready to accept jobs.
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }

    /// Measures one send→reply round-trip (includes channel latency + thread wake),
    /// which is the true cost proxy for deciding whether to fork.
    /// `FinishedBatchTx` is dropped without committing — calibration never advances the tx offset.
    pub fn calibrate(&self) -> Result<Duration, WorkerGone> {
        let (reply_tx, reply_rx) = mpsc::channel();
        let start = Instant::now();
        self.job_tx
            .send(WorkerJob::Calibrate { reply: reply_tx })
            .map_err(|_| WorkerGone::Exited)?;
        reply_rx.recv().map_err(|_| WorkerGone::Exited)?;
        Ok(start.elapsed())
    }

    /// Returns the reply receiver; caller should block on it after running home-thread work.
    pub fn dispatch(&self, job: BatchRunJob) -> Result<Receiver<BatchRunReply>, WorkerGone> {
        let (reply_tx, reply_rx) = mpsc::channel();
        self.job_tx
            .send(WorkerJob::Run {
                job: Box::new(job),
                reply: reply_tx,
            })
            .map_err(|_| WorkerGone::Exited)?;
        Ok(reply_rx)
    }

    pub fn recv_reply(rx: Receiver<BatchRunReply>) -> Result<BatchRunReply, WorkerGone> {
        rx.recv().map_err(|_| WorkerGone::Exited)
    }
}

impl Drop for ReducerWorker {
    fn drop(&mut self) {
        // The sender field drops here, closing the channel.  The worker's recv
        // loop exits when the channel is empty and closed.  We join with a
        // 5-second timeout for hygiene; log a warning if it exceeds that.
        if let Some(handle) = self.join.take() {
            // std::thread::JoinHandle has no timed join; spin on is_finished().
            let deadline = Instant::now() + Duration::from_secs(5);
            loop {
                if handle.is_finished() {
                    let _ = handle.join();
                    return;
                }
                if Instant::now() >= deadline {
                    log::warn!("reducer worker thread did not exit within 5 s; detaching");
                    return;
                }
                thread::sleep(Duration::from_millis(5));
            }
        }
    }
}

// ─── Worker thread body ───────────────────────────────────────────────────────

fn run_worker_loop<I, F>(
    make_instance: F,
    relational_db: Arc<RelationalDB>,
    job_rx: Receiver<WorkerJob>,
    ready: Arc<AtomicBool>,
) where
    I: WasmInstance + 'static,
    F: Fn() -> WasmModuleInstance<I>,
{
    let mut instance = make_instance();
    ready.store(true, Ordering::Release);

    for job in &job_rx {
        match job {
            WorkerJob::Calibrate { reply } => {
                // FinishedBatchTx dropped without committing — calibration must never advance the tx offset.
                let tx = relational_db.begin_batch_tx(Workload::Internal);
                let _finished = tx.finish();
                let _ = reply.send(());
            }

            WorkerJob::Run { job, reply } => {
                let BatchRunJob { params, tx, capture } = *job;
                let outcome = instance.call_reducer_body_batch(tx, params, capture);
                let trapped = outcome.trapped;

                let _ = reply.send(BatchRunReply { outcome });

                // Rebuild after a trap so the home thread never observes a dead instance.
                if trapped {
                    instance = make_instance();
                }
            }
        }
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::module_host::{CallReducerParams, DatabaseUpdate, EventStatus, ModuleEvent, ModuleFunctionCall};
    use spacetimedb_client_api_messages::energy::FunctionBudget;
    use spacetimedb_datastore::execution_context::Workload;
    use spacetimedb_datastore::locking_tx_datastore::batch_tx::BatchTxState;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    // ── Fake instance ──────────────────────────────────────────────────────────
    //
    // Faking `WasmInstance` directly is impractical (it requires async methods,
    // ReplicaContext, etc.).  Instead we drive the worker loop through a generic
    // helper that accepts any `RunReducerBody`, and use this cheap stub.

    struct FakeInstance {
        call_log: Arc<Mutex<Vec<u32>>>,
        /// Records the `capture_access` flag of each dispatched job, in order.
        capture_log: Arc<Mutex<Vec<bool>>>,
    }

    impl RunReducerBody for FakeInstance {
        fn run_body(&mut self, tx: BatchTxState, params: CallReducerParams, capture: CaptureSpec) -> BatchBodyOutcome {
            // finish() drops the read guard so the home thread can later commit.
            let _finished = tx.finish();
            self.call_log.lock().unwrap().push(u32::from(params.reducer_id));
            self.capture_log.lock().unwrap().push(capture.capture_access);
            BatchBodyOutcome {
                finished: None,
                event: stub_event(),
                execution_budget_used: FunctionBudget::ZERO,
                host_execution_duration: Duration::ZERO,
                trapped: false,
                observed: None,
                view_refresh_needed: false,
            }
        }

        fn needs_replacement(&self) -> bool {
            false
        }
    }

    fn stub_event() -> ModuleEvent {
        use spacetimedb_lib::{Identity, Timestamp};
        ModuleEvent {
            timestamp: Timestamp::now(),
            caller_identity: Identity::ZERO,
            caller_connection_id: None,
            function_call: ModuleFunctionCall {
                reducer: None,
                reducer_id: Default::default(),
                args: Default::default(),
            },
            status: EventStatus::Committed(DatabaseUpdate::default()),
            reducer_return_value: None,
            execution_budget_used: FunctionBudget::ZERO,
            host_execution_duration: Duration::ZERO,
            request_id: None,
            timer: None,
        }
    }

    // ── Generic test harness ───────────────────────────────────────────────────

    fn spawn_fake<B>(
        db: Arc<RelationalDB>,
        make: impl Fn() -> B + Send + 'static,
    ) -> (Sender<WorkerJob>, Arc<AtomicBool>, thread::JoinHandle<()>)
    where
        B: RunReducerBody + Send,
    {
        let (job_tx, job_rx) = mpsc::channel::<WorkerJob>();
        let ready = Arc::new(AtomicBool::new(false));
        let ready_clone = Arc::clone(&ready);

        let join = thread::spawn(move || {
            let mut instance = make();
            ready_clone.store(true, Ordering::Release);
            for job in &job_rx {
                match job {
                    WorkerJob::Calibrate { reply } => {
                        let tx = db.begin_batch_tx(Workload::Internal);
                        let _finished = tx.finish();
                        let _ = reply.send(());
                    }
                    WorkerJob::Run { job, reply } => {
                        let BatchRunJob { params, tx, capture } = *job;
                        let outcome = instance.run_body(tx, params, capture);
                        let trapped = outcome.trapped;
                        let _ = reply.send(BatchRunReply { outcome });
                        if trapped {
                            instance = make();
                        }
                    }
                }
            }
        });

        (job_tx, ready, join)
    }

    fn wait_ready(ready: &Arc<AtomicBool>) {
        let deadline = Instant::now() + Duration::from_secs(5);
        while !ready.load(Ordering::Acquire) {
            assert!(Instant::now() < deadline, "worker never became ready");
            thread::sleep(Duration::from_millis(1));
        }
    }

    fn make_test_db() -> Arc<RelationalDB> {
        let test_db = crate::db::relational_db::tests_utils::TestDB::in_memory().expect("TestDB::in_memory failed");
        Arc::clone(&test_db.db)
    }

    fn stub_params(seq: usize) -> CallReducerParams {
        use crate::host::ArgsTuple;
        use spacetimedb_lib::{ConnectionId, Identity, Timestamp};
        use spacetimedb_primitives::ReducerId;
        CallReducerParams {
            timestamp: Timestamp::now(),
            caller_identity: Identity::ZERO,
            caller_connection_id: ConnectionId::ZERO,
            client: None,
            request_id: None,
            reducer_id: ReducerId::from(seq as u32),
            args: ArgsTuple::nullary(),
            timer: None,
            capture_access: false,
        }
    }

    // ── Test cases ─────────────────────────────────────────────────────────────

    type CallLog = Arc<Mutex<Vec<u32>>>;
    type CaptureLog = Arc<Mutex<Vec<bool>>>;

    fn new_logs() -> (CallLog, CaptureLog) {
        (Arc::new(Mutex::new(Vec::new())), Arc::new(Mutex::new(Vec::new())))
    }

    #[test]
    fn spawn_becomes_ready() {
        let db = make_test_db();
        let (calls, captures) = new_logs();
        let calls_c = Arc::clone(&calls);
        let captures_c = Arc::clone(&captures);
        let (_, ready, join) = spawn_fake(Arc::clone(&db), move || FakeInstance {
            call_log: Arc::clone(&calls_c),
            capture_log: Arc::clone(&captures_c),
        });
        wait_ready(&ready);
        assert!(ready.load(Ordering::Acquire));
        drop(join); // detach; clean shutdown tested in drop_exits_thread
    }

    #[test]
    fn calibrate_returns_a_duration() {
        let db = make_test_db();
        let (calls, captures) = new_logs();
        let calls_c = Arc::clone(&calls);
        let captures_c = Arc::clone(&captures);
        let (job_tx, ready, _join) = spawn_fake(Arc::clone(&db), move || FakeInstance {
            call_log: Arc::clone(&calls_c),
            capture_log: Arc::clone(&captures_c),
        });
        wait_ready(&ready);

        let (reply_tx, reply_rx) = mpsc::channel();
        let start = Instant::now();
        job_tx.send(WorkerJob::Calibrate { reply: reply_tx }).unwrap();
        reply_rx.recv().expect("calibrate reply");
        let elapsed = start.elapsed();

        assert!(elapsed < Duration::from_secs(5), "calibrate blocked: {elapsed:?}");
    }

    #[test]
    fn dispatch_recv_returns_outcome() {
        let db = make_test_db();
        let (calls, captures) = new_logs();
        let calls_c = Arc::clone(&calls);
        let captures_c = Arc::clone(&captures);
        let (job_tx, ready, _join) = spawn_fake(Arc::clone(&db), move || FakeInstance {
            call_log: Arc::clone(&calls_c),
            capture_log: Arc::clone(&captures_c),
        });
        wait_ready(&ready);

        let tx = db.begin_batch_tx(Workload::Internal);
        let (reply_tx, reply_rx) = mpsc::channel();
        job_tx
            .send(WorkerJob::Run {
                job: Box::new(BatchRunJob {
                    params: stub_params(77),
                    tx,
                    capture: CaptureSpec::default(),
                }),
                reply: reply_tx,
            })
            .unwrap();
        let reply = reply_rx.recv().expect("run reply");
        assert!(!reply.outcome.trapped);
    }

    /// A dispatched job's `capture` flag must reach `run_body` unchanged.
    #[test]
    fn capture_flag_reaches_run_body() {
        let db = make_test_db();
        let (calls, captures) = new_logs();
        let calls_c = Arc::clone(&calls);
        let captures_c = Arc::clone(&captures);
        let (job_tx, ready, _join) = spawn_fake(Arc::clone(&db), move || FakeInstance {
            call_log: Arc::clone(&calls_c),
            capture_log: Arc::clone(&captures_c),
        });
        wait_ready(&ready);

        // Dispatch one job with capture on, one with it off.
        for capture_access in [true, false] {
            let tx = db.begin_batch_tx(Workload::Internal);
            let (reply_tx, reply_rx) = mpsc::channel();
            job_tx
                .send(WorkerJob::Run {
                    job: Box::new(BatchRunJob {
                        params: stub_params(1),
                        tx,
                        capture: CaptureSpec {
                            capture_access,
                            check_views: false,
                        },
                    }),
                    reply: reply_tx,
                })
                .unwrap();
            reply_rx.recv().expect("run reply");
        }

        assert_eq!(*captures.lock().unwrap(), vec![true, false]);
    }

    #[test]
    fn drop_exits_thread() {
        let db = make_test_db();
        let (calls, captures) = new_logs();
        let calls_c = Arc::clone(&calls);
        let captures_c = Arc::clone(&captures);
        let (job_tx, ready, join) = spawn_fake(Arc::clone(&db), move || FakeInstance {
            call_log: Arc::clone(&calls_c),
            capture_log: Arc::clone(&captures_c),
        });
        wait_ready(&ready);

        drop(job_tx);

        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if join.is_finished() {
                let _ = join.join();
                return;
            }
            assert!(Instant::now() < deadline, "worker did not exit after channel close");
            thread::sleep(Duration::from_millis(5));
        }
    }
}
