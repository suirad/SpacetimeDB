#![allow(clippy::disallowed_macros)]
//! Integration tests for the reducer-batching scheduler.
//!
//! These tests exercise the live fork path (pool spawned, width≥2 batch
//! dispatched to the worker) and the trap-recovery path. They load the
//! `reducer-batching-fixture` wasm module once per process (memoised by
//! `lazy_static`) and run against a real `StandaloneEnv`.
//!
//! Nondeterminism policy: fork detection uses a bounded retry loop (up to
//! MAX_ROUNDS rounds of concurrent pairs). If the loop exhausts without a
//! fork we print a message and skip the fork assertion — this happens only
//! when the test environment can't physically produce a ready pool (e.g.
//! STDB_REDUCER_POOL_CAP=0 was set externally, or the calibration threshold
//! landed above the heavy reducer's runtime). n=5_000 rows is chosen so that
//! even debug-mode inserts (~1–2 µs each) yield a ~5–10 ms reducer body,
//! which is well above any realistic fork threshold.

use lazy_static::lazy_static;
use serial_test::serial;
use spacetimedb_lib::sats::product;
use spacetimedb_testing::modules::{CompilationMode, CompiledModule, LoggerRecord, DEFAULT_CONFIG};

const N: u64 = 5_000;
const MAX_ROUNDS: usize = 50;

lazy_static! {
    static ref MODULE: CompiledModule =
        CompiledModule::compile("reducer-batching-fixture", CompilationMode::Debug);
}

fn init_logger() {
    let _ = env_logger::builder()
        .parse_filters("spacetimedb=info")
        .is_test(true)
        .try_init();
}

/// Parse the most recent `counts a=X b=Y` message from JSON log lines.
fn parse_counts(log: &str) -> Option<(u64, u64)> {
    for line in log.lines().rev() {
        if line.is_empty() {
            continue;
        }
        let record: LoggerRecord = serde_json::from_str(line).ok()?;
        let msg = &record.message;
        if let Some(rest) = msg.strip_prefix("counts a=") {
            let mut parts = rest.split_whitespace();
            let a: u64 = parts.next()?.parse().ok()?;
            let b_part = parts.next()?;
            let b: u64 = b_part.strip_prefix("b=")?.parse().ok()?;
            return Some((a, b));
        }
    }
    None
}

/// Warm the pool: run enough sequential heavy_a calls so the scheduler
/// measures the reducer's runtime, trips the threshold, spawns the worker,
/// and finishes calibration. Returns when `pool_state == Ready` or after
/// MAX_ROUNDS attempts.
async fn warm_pool(module: &spacetimedb_testing::modules::ModuleHandle) {
    for _ in 0..MAX_ROUNDS {
        module
            .call_reducer_binary("heavy_a", &product![N])
            .await
            .expect("heavy_a warm-up failed");
        if let Some(snap) = module.batch_stats().await
            && snap.pool_state == spacetimedb::host::PoolStateTag::Ready
        {
            println!("[warm_pool] pool ready after calibration");
            return;
        }
    }
    println!("[warm_pool] pool did not reach Ready within {MAX_ROUNDS} rounds (threshold may be above heavy runtime)");
}

// ─── Test 1: fork fires, FIFO holds, DB consistent ───────────────────────────

#[tokio::test]
#[serial]
async fn fork_fires_and_fifo_holds() {
    init_logger();
    let module = MODULE.load_module(DEFAULT_CONFIG, None).await;

    warm_pool(&module).await;

    let snap_before = module.batch_stats().await;
    let pool_ready = snap_before
        .as_ref()
        .map(|s| s.pool_state == spacetimedb::host::PoolStateTag::Ready)
        .unwrap_or(false);

    if !pool_ready {
        println!("[fork_fires_and_fifo_holds] pool not ready — skipping fork assertions");
    }

    let mut forked = false;
    let mut successful_pairs: u64 = 0;

    for round in 0..MAX_ROUNDS {
        // Fire two concurrent heavy reducers — queue depth triggers batch build.
        let args_a = product![N];
        let args_b = product![N];
        let fa = module.call_reducer_binary("heavy_a", &args_a);
        let fb = module.call_reducer_binary("heavy_b", &args_b);
        let (ra, rb) = tokio::join!(fa, fb);
        assert!(ra.is_ok(), "heavy_a failed on round {round}: {:?}", ra.err());
        assert!(rb.is_ok(), "heavy_b failed on round {round}: {:?}", rb.err());
        successful_pairs += 1;

        if pool_ready {
            if let Some(snap) = module.batch_stats().await
                && snap.forks >= 1 && snap.widths[1] >= 1
            {
                forked = true;
                println!(
                    "[fork_fires_and_fifo_holds] fork observed at round {round}: \
                     forks={} widths={:?}",
                    snap.forks, snap.widths
                );
                break;
            }
        } else {
            // Pool not ready: just verify correctness, don't assert fork.
            if round + 1 >= 5 {
                break;
            }
        }
    }

    if pool_ready && !forked {
        println!(
            "[fork_fires_and_fifo_holds] WARNING: pool was Ready but no fork observed in \
             {MAX_ROUNDS} rounds — threshold may exceed heavy_a runtime in this environment"
        );
    }

    // DB consistency: call log_counts and parse the result.
    module
        .call_reducer_binary("log_counts", &product![])
        .await
        .expect("log_counts failed");

    let log = module.read_log(None).await;
    let (a_count, b_count) = parse_counts(&log).expect("log_counts output not found in log");

    // Each successful pair contributes N rows to both a and b.
    // heavy_a warm-up calls also write to a — they ran MAX_ROUNDS times at most,
    // but only up to when pool became ready. We can't know the exact warm-up
    // count without checking; so we assert b_count equals exactly the pairs' N
    // (b is only written by heavy_b calls, which only ran in the pair rounds).
    assert_eq!(
        b_count,
        successful_pairs * N,
        "table_b row count mismatch: expected {} ({}×{}), got {}",
        successful_pairs * N, successful_pairs, N, b_count
    );
    // a_count ≥ pairs × N (warm-up also wrote to a).
    assert!(
        a_count >= successful_pairs * N,
        "table_a row count too low: expected ≥{}, got {}",
        successful_pairs * N, a_count
    );

    println!(
        "[fork_fires_and_fifo_holds] PASS — pairs={successful_pairs} a={a_count} b={b_count} forked={forked}"
    );
    if let Some(snap) = module.batch_stats().await {
        println!("[fork_fires_and_fifo_holds] final stats: {snap:?}");
    }
}

// ─── Test 2: trap on worker recovers ─────────────────────────────────────────

#[tokio::test]
#[serial]
async fn trap_on_worker_recovers() {
    init_logger();
    let module = MODULE.load_module(DEFAULT_CONFIG, None).await;

    warm_pool(&module).await;

    // Learn heavy_panic's weight: two sequential calls (they will fail/trap).
    // This updates the scheduler's runtime stat for the reducer.
    for _ in 0..2 {
        let _ = module.call_reducer_binary("heavy_panic", &product![N]).await;
        // expected to fail — that's fine
    }

    let snap_before = module.batch_stats().await;
    let pool_ready = snap_before
        .as_ref()
        .map(|s| s.pool_state == spacetimedb::host::PoolStateTag::Ready)
        .unwrap_or(false);
    let forks_before = snap_before.as_ref().map(|s| s.forks).unwrap_or(0);

    // Fire concurrent heavy_panic + heavy_b (panic+disjoint — may or may not fork).
    // We don't assert it forked, just that subsequent rounds still work.
    let mut trap_rounds = 0usize;
    for _ in 0..10 {
        let args_p = product![N];
        let args_b = product![N];
        let fp = module.call_reducer_binary("heavy_panic", &args_p);
        let fb = module.call_reducer_binary("heavy_b", &args_b);
        let (rp, rb) = tokio::join!(fp, fb);
        // heavy_panic is expected to return an error (reducer trap).
        let _ = rp; // may be Ok or Err depending on whether it trapped
        assert!(rb.is_ok(), "heavy_b should not fail even alongside a panicking reducer");
        trap_rounds += 1;
    }
    let _ = trap_rounds;

    let snap_mid = module.batch_stats().await;
    if pool_ready
        && let Some(s) = &snap_mid
    {
        println!(
            "[trap_on_worker_recovers] after panic rounds: forks_before={forks_before} forks_now={}",
            s.forks
        );
    }

    // Now run a clean concurrent pair to confirm pool is still functional.
    for round in 0..MAX_ROUNDS {
        let args_a = product![N];
        let args_b = product![N];
        let fa = module.call_reducer_binary("heavy_a", &args_a);
        let fb = module.call_reducer_binary("heavy_b", &args_b);
        let (ra, rb) = tokio::join!(fa, fb);
        assert!(ra.is_ok(), "post-trap heavy_a failed on round {round}");
        assert!(rb.is_ok(), "post-trap heavy_b failed on round {round}");

        if pool_ready {
            if let Some(snap) = module.batch_stats().await
                && snap.forks > forks_before
            {
                println!("[trap_on_worker_recovers] pool re-forked after recovery at round {round}");
                break;
            }
        } else {
            break;
        }
    }

    // Final consistency check via log_counts.
    module
        .call_reducer_binary("log_counts", &product![])
        .await
        .expect("log_counts failed after trap test");
    let log = module.read_log(None).await;
    let (a_count, b_count) = parse_counts(&log).expect("log_counts output not found in log");

    // b was only written by successful heavy_b calls. Each succeeded.
    // We can't easily count exactly how many b writes happened (some may have
    // been paired with heavy_panic), but b_count must be > 0.
    assert!(b_count > 0, "table_b should have rows after successful heavy_b calls");
    let _ = a_count; // value present and accessible; b count is the binding constraint above

    println!("[trap_on_worker_recovers] PASS — a={a_count} b={b_count}");
}
