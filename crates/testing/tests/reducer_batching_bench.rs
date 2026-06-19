#![allow(clippy::disallowed_macros)]
//! Measurement harness for the reducer-batching scheduler.
//!
//! Run explicitly (skipped in normal CI):
//!   cargo test -p spacetimedb-testing --test reducer_batching_bench -- --ignored --nocapture
//!
//! Set STDB_REDUCER_BATCHING externally before running to label the output;
//! default label is "1" (one-worker pool). This test never sets that var itself.

use futures::future::join_all;
use lazy_static::lazy_static;
use spacetimedb_lib::sats::product;
use spacetimedb_testing::modules::{CompilationMode, CompiledModule, DEFAULT_CONFIG};

// N large enough that even debug-mode wasm inserts land well above the fork
// threshold, giving the scheduler something meaningful to measure.
const N: u64 = 20_000;
// Warm-up cap: we stop early when the pool reports Ready.
const WARM_MAX: usize = 30;
// Heavy phase: number of concurrent (heavy_a, heavy_b) pairs.
const HEAVY_ROUNDS: usize = 40;
// Mixed phase: 2 heavy + 10 cheap per round, exercises prefix-truncation when
// same-table writers (cheap_c..cheap_g appear twice each) self-conflict.
const M_MIXED: usize = 40;
// Cheap phase: sequential cheap_a calls for fast-path overhead comparison.
const CHEAP_CALLS: usize = 2000;

lazy_static! {
    static ref MODULE: CompiledModule = CompiledModule::compile("reducer-batching-fixture", CompilationMode::Debug);
}

/// Parse all seven counts from the most recent `counts a=X b=Y c=Z …` log line.
fn parse_counts(log: &str) -> Option<(u64, u64, u64, u64, u64, u64, u64)> {
    use spacetimedb_testing::modules::LoggerRecord;
    for line in log.lines().rev() {
        if line.is_empty() {
            continue;
        }
        let record: LoggerRecord = serde_json::from_str(line).ok()?;
        let msg = &record.message;
        if let Some(rest) = msg.strip_prefix("counts a=") {
            let mut parts = rest.split_whitespace();
            let a: u64 = parts.next()?.parse().ok()?;
            let b: u64 = parts.next()?.strip_prefix("b=")?.parse().ok()?;
            let c: u64 = parts.next()?.strip_prefix("c=")?.parse().ok()?;
            let d: u64 = parts.next()?.strip_prefix("d=")?.parse().ok()?;
            let e: u64 = parts.next()?.strip_prefix("e=")?.parse().ok()?;
            let f: u64 = parts.next()?.strip_prefix("f=")?.parse().ok()?;
            let g: u64 = parts.next()?.strip_prefix("g=")?.parse().ok()?;
            return Some((a, b, c, d, e, f, g));
        }
    }
    None
}

#[tokio::test]
#[ignore]
async fn bench() {
    // Read cap label BEFORE touching the module so we reflect the env the
    // caller configured, not a value we injected ourselves.
    let cap_label = std::env::var("STDB_REDUCER_BATCHING").unwrap_or_else(|_| "1".to_string());

    let module = MODULE.load_module(DEFAULT_CONFIG, None).await;

    // ── Warm-up phase ────────────────────────────────────────────────────────
    // Drive sequential heavy_a calls until the pool reaches Ready (calibration
    // complete and worker spawned) or WARM_MAX is exhausted. With cap=0 the
    // pool is Off and will never become Ready — the bound still holds.
    println!("[bench] warm-up start (cap={cap_label}, N={N}, max={WARM_MAX})");
    for i in 0..WARM_MAX {
        module
            .call_reducer_binary("heavy_a", &product![N])
            .await
            .expect("heavy_a warm-up failed");
        if let Some(snap) = module.batch_stats().await {
            if snap.pool_state == spacetimedb::host::PoolStateTag::Ready {
                println!(
                    "[bench] pool Ready after {} warm-up calls; calibrated_ns={:?}",
                    i + 1,
                    snap.calibrated_ns
                );
                break;
            }
            if i + 1 == WARM_MAX {
                println!(
                    "[bench] pool did not reach Ready in {WARM_MAX} warm-up calls; \
                     pool_state={:?} calibrated_ns={:?}",
                    snap.pool_state, snap.calibrated_ns
                );
            }
        }
    }

    // ── Heavy phase ──────────────────────────────────────────────────────────
    // M concurrent (heavy_a, heavy_b) pairs; both must succeed each round.
    println!("[bench] heavy phase start ({HEAVY_ROUNDS} rounds)");
    let heavy_start = std::time::Instant::now();
    for round in 0..HEAVY_ROUNDS {
        let args_a = product![N];
        let args_b = product![N];
        let fa = module.call_reducer_binary("heavy_a", &args_a);
        let fb = module.call_reducer_binary("heavy_b", &args_b);
        let (ra, rb) = tokio::join!(fa, fb);
        assert!(ra.is_ok(), "heavy_a failed on round {round}: {:?}", ra.err());
        assert!(rb.is_ok(), "heavy_b failed on round {round}: {:?}", rb.err());
    }
    let heavy_total_ms = heavy_start.elapsed().as_millis();
    let heavy_mean_ms = heavy_total_ms as f64 / HEAVY_ROUNDS as f64;
    println!(
        "[bench] heavy phase done: total={}ms mean={:.1}ms/round",
        heavy_total_ms, heavy_mean_ms
    );

    // ── Mixed phase ──────────────────────────────────────────────────────────
    // Each round fires 12 concurrent calls in a fixed order:
    //   heavy_a, cheap_c, cheap_d, cheap_e, heavy_b, cheap_f, cheap_g,
    //   cheap_c, cheap_d, cheap_e, cheap_f, cheap_g
    // The deliberate repetition of cheap_c..g (each table hit twice per round)
    // means same-table writers self-conflict, exercising prefix truncation in
    // the batch admission predicate.
    let snap_before_mixed = module.batch_stats().await;
    println!("[bench] mixed phase start ({M_MIXED} rounds)");
    let mixed_start = std::time::Instant::now();
    for round in 0..M_MIXED {
        // Hoist product! temporaries so the borrows outlive the futures vec.
        let args_heavy = product![N];
        let args_heavy2 = product![N];
        let e0 = product![];
        let e1 = product![];
        let e2 = product![];
        let e3 = product![];
        let e4 = product![];
        let e5 = product![];
        let e6 = product![];
        let e7 = product![];
        let e8 = product![];
        let e9 = product![];
        let futures: Vec<_> = vec![
            module.call_reducer_binary("heavy_a", &args_heavy),
            module.call_reducer_binary("cheap_c", &e0),
            module.call_reducer_binary("cheap_d", &e1),
            module.call_reducer_binary("cheap_e", &e2),
            module.call_reducer_binary("heavy_b", &args_heavy2),
            module.call_reducer_binary("cheap_f", &e3),
            module.call_reducer_binary("cheap_g", &e4),
            module.call_reducer_binary("cheap_c", &e5),
            module.call_reducer_binary("cheap_d", &e6),
            module.call_reducer_binary("cheap_e", &e7),
            module.call_reducer_binary("cheap_f", &e8),
            module.call_reducer_binary("cheap_g", &e9),
        ];
        let results = join_all(futures).await;
        for (idx, r) in results.iter().enumerate() {
            assert!(
                r.is_ok(),
                "mixed round {round} call[{idx}] failed: {:?}",
                r.as_ref().err()
            );
        }
    }
    let mixed_total_ms = mixed_start.elapsed().as_millis();
    let mixed_mean_ms = mixed_total_ms as f64 / M_MIXED as f64;
    println!(
        "[bench] mixed phase done: total={}ms mean={:.1}ms/round",
        mixed_total_ms, mixed_mean_ms
    );

    let snap_after_mixed = module.batch_stats().await;
    println!("[bench] mixed batch_stats snapshot: {snap_after_mixed:?}");

    // Per-phase deltas relative to the snapshot taken before this phase.
    let (mixed_forks_delta, mixed_batches_delta, mixed_widths_delta_str) = match (&snap_before_mixed, &snap_after_mixed)
    {
        (Some(before), Some(after)) => {
            let forks_d = after.forks.saturating_sub(before.forks);
            let batches_d = after.batches.saturating_sub(before.batches);
            // Element-wise delta for the widths histogram.
            let widths_d: Vec<u64> = after
                .widths
                .iter()
                .zip(before.widths.iter())
                .map(|(a, b)| a.saturating_sub(*b))
                .collect();
            (forks_d, batches_d, format!("{widths_d:?}"))
        }
        (None, Some(after)) => (after.forks, after.batches, format!("{:?}", after.widths)),
        _ => (0, 0, "N/A".to_string()),
    };

    println!(
        "BENCH-MIXED cap={} mixed_total_ms={} mixed_mean_ms={:.1} forks={} batches={} widths={}",
        cap_label, mixed_total_ms, mixed_mean_ms, mixed_forks_delta, mixed_batches_delta, mixed_widths_delta_str,
    );

    // ── Cheap phase ──────────────────────────────────────────────────────────
    // Sequential cheap_a calls to measure fast-path overhead.
    println!("[bench] cheap phase start ({CHEAP_CALLS} calls)");
    let cheap_start = std::time::Instant::now();
    for _ in 0..CHEAP_CALLS {
        module
            .call_reducer_binary("cheap_a", &product![])
            .await
            .expect("cheap_a failed");
    }
    let cheap_total_ms = cheap_start.elapsed().as_millis();
    let cheap_mean_us = (cheap_start.elapsed().as_micros() as f64) / CHEAP_CALLS as f64;
    println!(
        "[bench] cheap phase done: total={}ms mean={:.1}µs/call",
        cheap_total_ms, cheap_mean_us
    );

    // ── Final snapshot ───────────────────────────────────────────────────────
    let snap = module.batch_stats().await;
    println!("[bench] final batch_stats: {snap:?}");

    let (forks, batches, widths_str) = match &snap {
        Some(s) => (s.forks, s.batches, format!("{:?}", s.widths)),
        None => (0, 0, "N/A".to_string()),
    };

    // Single machine-greppable summary line.
    println!(
        "BENCH cap={} heavy_total_ms={} heavy_mean_ms={:.1} cheap_total_ms={} cheap_mean_us={:.1} forks={} batches={} widths={}",
        cap_label,
        heavy_total_ms,
        heavy_mean_ms,
        cheap_total_ms,
        cheap_mean_us,
        forks,
        batches,
        widths_str,
    );

    // ── Correctness assertions ────────────────────────────────────────────────
    // Call log_counts and parse all seven table sizes.
    module
        .call_reducer_binary("log_counts", &product![])
        .await
        .expect("log_counts failed");
    let log = module.read_log(None).await;
    let (a_count, b_count, c_count, d_count, e_count, f_count, g_count) =
        parse_counts(&log).expect("log_counts output not found in log");

    // c–g are only written by the mixed phase (2 calls per table per round).
    assert_eq!(
        c_count,
        2 * M_MIXED as u64,
        "table_c: expected {} got {}",
        2 * M_MIXED,
        c_count
    );
    assert_eq!(
        d_count,
        2 * M_MIXED as u64,
        "table_d: expected {} got {}",
        2 * M_MIXED,
        d_count
    );
    assert_eq!(
        e_count,
        2 * M_MIXED as u64,
        "table_e: expected {} got {}",
        2 * M_MIXED,
        e_count
    );
    assert_eq!(
        f_count,
        2 * M_MIXED as u64,
        "table_f: expected {} got {}",
        2 * M_MIXED,
        f_count
    );
    assert_eq!(
        g_count,
        2 * M_MIXED as u64,
        "table_g: expected {} got {}",
        2 * M_MIXED,
        g_count
    );

    // heavy_b runs in both the heavy phase (HEAVY_ROUNDS × N) and the mixed
    // phase (M_MIXED × N), so b == (HEAVY_ROUNDS + M_MIXED) × N.
    let expected_b = (HEAVY_ROUNDS as u64 + M_MIXED as u64) * N;
    assert_eq!(
        b_count, expected_b,
        "table_b: expected {} (heavy {} + mixed {}) × N={}, got {}",
        expected_b, HEAVY_ROUNDS, M_MIXED, N, b_count
    );

    // a_count ≥ (HEAVY_ROUNDS + M_MIXED) × N because warm-up also writes to a.
    let min_a = (HEAVY_ROUNDS as u64 + M_MIXED as u64) * N;
    assert!(a_count >= min_a, "table_a too low: expected ≥{} got {}", min_a, a_count);

    println!("[bench] PASS — a={a_count} b={b_count} c={c_count} d={d_count} e={e_count} f={f_count} g={g_count}");
}
