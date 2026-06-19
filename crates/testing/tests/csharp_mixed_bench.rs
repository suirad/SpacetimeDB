#![allow(clippy::disallowed_macros)]
//! C# analogue of the synthetic mixed-burst bench; base (cap=0) vs v2 (cap=1, learned);
//! C# reducers are all wildcard so the bench promotes them first; run under both caps.
//!
//! Unlike the Rust fixture (where the static analyzer resolves reducer tables so batching
//! works immediately), the C# module is Mono/wasm — the static analyzer returns wildcard
//! for every C# reducer, meaning they all start `Unknown` tier and cannot fork until they
//! have been LEARNED via inline capture. This bench therefore promotes every burst reducer
//! (heavy_a, heavy_b, cheap_c..cheap_g) before the mixed phase.
//!
//! Run explicitly (skipped in normal CI):
//!   cargo test -p spacetimedb-testing --test csharp_mixed_bench -- --ignored --nocapture
//!
//! Set STDB_REDUCER_BATCHING externally before running to label the output;
//! default label is "1". This test never sets that variable itself.
//!   STDB_REDUCER_BATCHING=0  → base lane (no pool, no forks, Unknown stays Unknown)
//!   STDB_REDUCER_BATCHING=1  → v2 lane  (learned fork path)

use futures::future::join_all;
use lazy_static::lazy_static;
use spacetimedb_lib::sats::product;
use spacetimedb_testing::modules::{CompilationMode, CompiledModule, ModuleHandle, DEFAULT_CONFIG};

/// Heavy reducer insert count. C# Debug wasm (Mono) is far slower per insert than Rust,
/// so N=2000 should comfortably clear the ~250µs fork threshold while keeping runs tractable.
const N: u64 = 2000;
/// Number of mixed-burst rounds.
const M_MIXED: usize = 20;
/// Sequential runs per reducer for promotion (≥ LEARN_MIN_RUNS=8; 12 for safety).
const LEARN_RUNS: usize = 12;
/// Warm-up cap: stop early when pool reaches Ready, or give up after this many calls.
const WARM_MAX: usize = 30;

lazy_static! {
    static ref MODULE: CompiledModule = CompiledModule::compile("reducer-batching-fixture-cs", CompilationMode::Debug);
}

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn phase_deltas(
    before: &Option<spacetimedb::host::BatchStatsSnapshot>,
    after: &Option<spacetimedb::host::BatchStatsSnapshot>,
) -> (u64, u64, String) {
    match (before, after) {
        (Some(b), Some(a)) => {
            let forks_d = a.forks.saturating_sub(b.forks);
            let batches_d = a.batches.saturating_sub(b.batches);
            let widths_d: Vec<u64> = a
                .widths
                .iter()
                .zip(b.widths.iter())
                .map(|(av, bv)| av.saturating_sub(*bv))
                .collect();
            (forks_d, batches_d, format!("{widths_d:?}"))
        }
        (None, Some(a)) => (a.forks, a.batches, format!("{:?}", a.widths)),
        _ => (0, 0, "N/A".to_string()),
    }
}

/// Run `LEARN_RUNS` sequential inline calls for one reducer to accumulate a stable access
/// set and trigger promotion (Unknown → Learned). Returns the run index (1-based) at which
/// the promotion was first observed, or `None` if it was not observed within `LEARN_RUNS`.
///
/// `promotions_before` is the global promotions counter captured *just before* this
/// reducer's loop starts — so we detect only ticks attributable to this reducer.
async fn promote(module: &ModuleHandle, name: &str, arg: Option<u64>, promotions_before: u64) -> Option<usize> {
    let mut observed_at: Option<usize> = None;
    for i in 0..LEARN_RUNS {
        let args_buf;
        let args: &spacetimedb_lib::ProductValue = match arg {
            Some(n) => {
                args_buf = product![n];
                &args_buf
            }
            None => {
                args_buf = product![];
                &args_buf
            }
        };
        module
            .call_reducer_binary(name, args)
            .await
            .unwrap_or_else(|e| panic!("promote: reducer `{name}` errored at run {i}: {e:?}"));

        if observed_at.is_none() {
            if let Some(snap) = module.batch_stats().await {
                if snap.promotions > promotions_before {
                    observed_at = Some(i + 1);
                    println!(
                        "[csmixed] `{name}` promotion observed after run {} (promotions={})",
                        i + 1,
                        snap.promotions
                    );
                }
            }
        }
    }
    observed_at
}

// ─── Test ─────────────────────────────────────────────────────────────────────

#[tokio::test]
#[ignore]
async fn bench_csharp_mixed() {
    // Read cap label BEFORE touching the module so we reflect what the caller configured.
    let cap_label = std::env::var("STDB_REDUCER_BATCHING").unwrap_or_else(|_| "1".to_string());

    // ── 1. Fresh module instance ─────────────────────────────────────────────
    let module = MODULE.load_module(DEFAULT_CONFIG, None).await;
    println!("[csmixed] module loaded (cap={cap_label}, N={N}, M_MIXED={M_MIXED}, LEARN_RUNS={LEARN_RUNS})");

    // ── 2. Promote every burst reducer ───────────────────────────────────────
    // C# reducers start Unknown. They cannot fork until Learned.
    // Run LEARN_RUNS sequential inline calls for each so the scheduler accumulates
    // a stable access set and promotes Unknown→Learned before the mixed phase.
    println!("[csmixed] promotion phase start");

    let snap_before_all = module.batch_stats().await;
    let promotions_start = snap_before_all.as_ref().map(|s| s.promotions).unwrap_or(0);

    // heavy_a
    let before_heavy_a = module.batch_stats().await.map(|s| s.promotions).unwrap_or(0);
    let _heavy_a_at = promote(&module, "heavy_a", Some(N), before_heavy_a).await;

    // heavy_b
    let before_heavy_b = module.batch_stats().await.map(|s| s.promotions).unwrap_or(0);
    let _heavy_b_at = promote(&module, "heavy_b", Some(N), before_heavy_b).await;

    // cheap_c
    let before_cheap_c = module.batch_stats().await.map(|s| s.promotions).unwrap_or(0);
    let _cheap_c_at = promote(&module, "cheap_c", None, before_cheap_c).await;

    // cheap_d
    let before_cheap_d = module.batch_stats().await.map(|s| s.promotions).unwrap_or(0);
    let _cheap_d_at = promote(&module, "cheap_d", None, before_cheap_d).await;

    // cheap_e
    let before_cheap_e = module.batch_stats().await.map(|s| s.promotions).unwrap_or(0);
    let _cheap_e_at = promote(&module, "cheap_e", None, before_cheap_e).await;

    // cheap_f
    let before_cheap_f = module.batch_stats().await.map(|s| s.promotions).unwrap_or(0);
    let _cheap_f_at = promote(&module, "cheap_f", None, before_cheap_f).await;

    // cheap_g
    let before_cheap_g = module.batch_stats().await.map(|s| s.promotions).unwrap_or(0);
    let _cheap_g_at = promote(&module, "cheap_g", None, before_cheap_g).await;

    let snap_after_promote = module.batch_stats().await;
    let promotions_after = snap_after_promote.as_ref().map(|s| s.promotions).unwrap_or(0);
    let promotions_gained = promotions_after.saturating_sub(promotions_start);

    if promotions_gained < 7 {
        println!(
            "FINDING: not all C# reducers promoted; promotions={promotions_after} \
             (gained {promotions_gained}/7; capture may not fire for C# wasm, \
             or analyzer did not wildcard as expected)"
        );
    } else {
        println!(
            "[csmixed] promotion phase done: promotions before={promotions_start} \
             after={promotions_after} (gained={promotions_gained})"
        );
    }

    // ── 3. Warm the pool ─────────────────────────────────────────────────────
    println!("[csmixed] warm-up start (cap={cap_label}, max={WARM_MAX})");
    for i in 0..WARM_MAX {
        module
            .call_reducer_binary("heavy_a", &product![N])
            .await
            .expect("heavy_a warm-up failed");
        if let Some(snap) = module.batch_stats().await {
            if snap.pool_state == spacetimedb::host::PoolStateTag::Ready {
                println!(
                    "[csmixed] pool Ready after {} warm-up calls; calibrated_ns={:?}",
                    i + 1,
                    snap.calibrated_ns
                );
                break;
            }
            if i + 1 == WARM_MAX {
                println!(
                    "[csmixed] pool did not reach Ready in {WARM_MAX} warm-up calls; \
                     pool_state={:?} calibrated_ns={:?}",
                    snap.pool_state, snap.calibrated_ns
                );
            }
        }
    }

    // ── 4. Mixed burst phase ─────────────────────────────────────────────────
    // Each round fires this 12-call burst concurrently (mirroring the Rust bench exactly):
    //   heavy_a, cheap_c, cheap_d, cheap_e, heavy_b, cheap_f, cheap_g,
    //   cheap_c, cheap_d, cheap_e, cheap_f, cheap_g
    // The deliberate repetition of cheap_c..g (each table hit twice per round) means
    // same-table writers self-conflict, exercising prefix truncation in the batch
    // admission predicate.
    let snap_before_mixed = module.batch_stats().await;
    println!("[csmixed] mixed phase start ({M_MIXED} rounds)");
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

    let snap_after_mixed = module.batch_stats().await;

    // ── 5. Phase deltas ──────────────────────────────────────────────────────
    let (mixed_forks, mixed_batches, mixed_widths) = phase_deltas(&snap_before_mixed, &snap_after_mixed);

    // ── 6. Emit machine-greppable summary lines ───────────────────────────────
    println!(
        "CSMIXED cap={} mixed_total_ms={} mixed_mean_ms={:.1} forks={} batches={} widths={}",
        cap_label, mixed_total_ms, mixed_mean_ms, mixed_forks, mixed_batches, mixed_widths,
    );

    let snap_final = module.batch_stats().await;
    let (final_forks, final_batches, final_widths_str, final_cal_ns, final_pool_state) = match &snap_final {
        Some(s) => (
            s.forks,
            s.batches,
            format!("{:?}", s.widths),
            format!("{:?}", s.calibrated_ns),
            format!("{:?}", s.pool_state),
        ),
        None => (0, 0, "N/A".to_string(), "N/A".to_string(), "N/A".to_string()),
    };

    println!(
        "CSMIXED-FINAL cap={} forks={} batches={} widths={} calibrated_ns={} pool_state={}",
        cap_label, final_forks, final_batches, final_widths_str, final_cal_ns, final_pool_state,
    );

    // ── 7. DB consistency: surface log_counts output ─────────────────────────
    module
        .call_reducer_binary("log_counts", &product![])
        .await
        .expect("log_counts failed");
    let log = module.read_log(None).await;
    // Surface the most recent "counts a=..." line as a sanity check.
    let counts_line = log.lines().rev().filter(|l| !l.is_empty()).find_map(|line| {
        let rec: serde_json::Value = serde_json::from_str(line).ok()?;
        let msg = rec.get("message")?.as_str()?;
        msg.contains("counts a=").then(|| msg.to_string())
    });
    match counts_line {
        Some(line) => println!("[csmixed] DB counts: {line}"),
        None => println!("[csmixed] WARNING: no 'counts a=...' line found in log"),
    }

    // ── 8. Red-flag: forks=0 with pool Ready ─────────────────────────────────
    if let Some(snap) = &snap_final {
        if snap.pool_state == spacetimedb::host::PoolStateTag::Ready && mixed_forks == 0 {
            println!(
                "WARNING: forks=0 with pool Ready — C# reducers may not have promoted \
                 (cap={cap_label})"
            );
        }
    }
}
