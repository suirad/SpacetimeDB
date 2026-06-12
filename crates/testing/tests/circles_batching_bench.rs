#![allow(clippy::disallowed_macros)]
//! Measurement harness for the reducer-batching scheduler on the `benchmarks`
//! module (circles / ia_loop family) — "shipping shape" data instead of the
//! synthetic fixture used by `reducer_batching_bench`.
//!
//! Run explicitly (skipped in normal CI):
//!   cargo test -p spacetimedb-testing --test circles_batching_bench -- --ignored --nocapture
//!
//! Set STDB_REDUCER_POOL_CAP externally before running to label the output;
//! default label is "1". This test never sets that variable itself.
//!
//! PURPOSE (kill-criterion stage 2): show whether width≥2 batches with members
//! above the calibrated fork threshold materialise on a module NOT built for
//! batching, and what prefix truncation does on realistic contention.

use futures::future::join_all;
use lazy_static::lazy_static;
use spacetimedb_lib::sats::product;
use spacetimedb_testing::modules::{CompilationMode, CompiledModule, DEFAULT_CONFIG};

// Circles tables are seeded once.  run_game_circles work is proportional to
// |circle| × |entity| × |food| — 100³ ≈ 10^6 inner loop iterations per call,
// which is the target "heavy" load for the fork admission gate.
const SEED_ENTITY: u32 = 100;
const SEED_CIRCLE: u32 = 100;
// Food ids are 1..=SEED_FOOD; entity autoinc starts at 1 — SEED_ENTITY must be
// >= SEED_FOOD to prevent the cross_join_circle_food panic.
const SEED_FOOD: u32 = 100;
// ia_loop tables.  update_position_with_velocity iterates all velocity rows and
// does a primary-key lookup per row — 30k is enough to be clearly above any
// realistic fork threshold without making the test prohibitively slow.
const SEED_POSITION: u32 = 30_000;
const SEED_VELOCITY: u32 = 30_000;
const ROUNDS: usize = 40;
// Maximum warm-up calls before giving up on pool reaching Ready.
const WARM_MAX: usize = 30;

lazy_static! {
    // Release mode: the shipping-shape analyzer sets gate admission thresholds
    // against Release-built wasm.  run_game_ia_loop / game_loop_enemy_ia are
    // wildcards in Release and intentionally excluded.
    static ref MODULE: CompiledModule =
        CompiledModule::compile("benchmarks", CompilationMode::Release);
}

#[tokio::test]
#[ignore]
async fn bench_circles() {
    // Read the cap label before touching the module so we reflect the caller's
    // environment, not anything we injected.
    let cap_label = std::env::var("STDB_REDUCER_POOL_CAP").unwrap_or_else(|_| "1".to_string());

    let module = MODULE.load_module(DEFAULT_CONFIG, None).await;

    // ── SEED (once, sequential) ──────────────────────────────────────────────
    // entity ids autoinc from 1; food ids are 1..=SEED_FOOD — seeding entity
    // first ensures every food row has a matching entity in cross_join_circle_food.
    module
        .call_reducer_binary("insert_bulk_entity", &product![SEED_ENTITY])
        .await
        .expect("insert_bulk_entity failed");
    module
        .call_reducer_binary("insert_bulk_circle", &product![SEED_CIRCLE])
        .await
        .expect("insert_bulk_circle failed");
    module
        .call_reducer_binary("insert_bulk_food", &product![SEED_FOOD])
        .await
        .expect("insert_bulk_food failed");
    module
        .call_reducer_binary("insert_bulk_position", &product![SEED_POSITION])
        .await
        .expect("insert_bulk_position failed");
    module
        .call_reducer_binary("insert_bulk_velocity", &product![SEED_VELOCITY])
        .await
        .expect("insert_bulk_velocity failed");

    println!(
        "[circles-bench] seed done: entity={SEED_ENTITY} circle={SEED_CIRCLE} food={SEED_FOOD} \
         position={SEED_POSITION} velocity={SEED_VELOCITY}"
    );

    // ── WARM-UP ──────────────────────────────────────────────────────────────
    // Sequential run_game_circles until the pool reports Ready (calibration
    // complete and worker spawned) or WARM_MAX is exhausted.  Printing per-call
    // elapsed µs here is the evidence that the call is "heavy enough" to pass
    // the fork admission gate.
    println!("[circles-bench] warm-up start (cap={cap_label}, max={WARM_MAX})");
    for i in 0..WARM_MAX {
        let t0 = std::time::Instant::now();
        module
            .call_reducer_binary("run_game_circles", &product![100_u32])
            .await
            .expect("run_game_circles warm-up failed");
        let elapsed_us = t0.elapsed().as_micros();
        println!("[circles-bench] warm-up[{i}] elapsed={elapsed_us}µs");

        if let Some(snap) = module.batch_stats().await {
            if snap.pool_state == spacetimedb::host::PoolStateTag::Ready {
                println!(
                    "[circles-bench] pool Ready after {} warm-up calls; calibrated_ns={:?}",
                    i + 1,
                    snap.calibrated_ns
                );
                break;
            }
            if i + 1 == WARM_MAX {
                println!(
                    "[circles-bench] pool did not reach Ready in {WARM_MAX} warm-up calls; \
                     pool_state={:?} calibrated_ns={:?}",
                    snap.pool_state, snap.calibrated_ns
                );
            }
        }
    }

    // ── PHASE HETERO ─────────────────────────────────────────────────────────
    // Concurrent pair: run_game_circles (reads entity/circle/food) paired with
    // update_position_with_velocity (reads velocity, writes position).  Disjoint
    // write sets — batchable if both are above the fork threshold.
    let snap_before_hetero = module.batch_stats().await;
    println!("[circles-bench] HETERO phase start ({ROUNDS} rounds)");
    let hetero_start = std::time::Instant::now();
    for round in 0..ROUNDS {
        let args_circles = product![100_u32];
        let args_vel = product![0_u32];
        let fa = module.call_reducer_binary("run_game_circles", &args_circles);
        let fb = module.call_reducer_binary("update_position_with_velocity", &args_vel);
        let (ra, rb) = tokio::join!(fa, fb);
        assert!(ra.is_ok(), "run_game_circles HETERO round {round} failed: {:?}", ra.err());
        assert!(
            rb.is_ok(),
            "update_position_with_velocity HETERO round {round} failed: {:?}",
            rb.err()
        );
    }
    let hetero_total_ms = hetero_start.elapsed().as_millis();
    let hetero_mean_ms = hetero_total_ms as f64 / ROUNDS as f64;
    let snap_after_hetero = module.batch_stats().await;

    let (hetero_forks, hetero_batches, hetero_widths) =
        phase_deltas(&snap_before_hetero, &snap_after_hetero);
    println!(
        "BENCH-CIRCLES phase=hetero cap={} total_ms={} mean_ms={:.1} forks={} batches={} widths={}",
        cap_label, hetero_total_ms, hetero_mean_ms, hetero_forks, hetero_batches, hetero_widths
    );

    // ── PHASE HOMO ───────────────────────────────────────────────────────────
    // Concurrent pair of identical pure-reader calls.  read∩read is always
    // admissible regardless of the fork threshold — shows the upper bound on
    // batch formation for this module.
    let snap_before_homo = module.batch_stats().await;
    println!("[circles-bench] HOMO phase start ({ROUNDS} rounds)");
    let homo_start = std::time::Instant::now();
    for round in 0..ROUNDS {
        let args_a = product![100_u32];
        let args_b = product![100_u32];
        let fa = module.call_reducer_binary("run_game_circles", &args_a);
        let fb = module.call_reducer_binary("run_game_circles", &args_b);
        let (ra, rb) = tokio::join!(fa, fb);
        assert!(ra.is_ok(), "run_game_circles HOMO-A round {round} failed: {:?}", ra.err());
        assert!(rb.is_ok(), "run_game_circles HOMO-B round {round} failed: {:?}", rb.err());
    }
    let homo_total_ms = homo_start.elapsed().as_millis();
    let homo_mean_ms = homo_total_ms as f64 / ROUNDS as f64;
    let snap_after_homo = module.batch_stats().await;

    let (homo_forks, homo_batches, homo_widths) = phase_deltas(&snap_before_homo, &snap_after_homo);
    println!(
        "BENCH-CIRCLES phase=homo cap={} total_ms={} mean_ms={:.1} forks={} batches={} widths={}",
        cap_label, homo_total_ms, homo_mean_ms, homo_forks, homo_batches, homo_widths
    );

    // ── PHASE MIXED ──────────────────────────────────────────────────────────
    // 5-call burst per round, fixed order:
    //   run_game_circles (reads entity/circle/food)
    //   update_position_with_velocity (reads velocity, writes position)
    //   cross_join_circle_food (reads entity/circle/food)
    //   update_position_all (reads + writes position)
    //   cross_join_all (reads entity/circle/food)
    //
    // update_position_all and update_position_with_velocity both write position
    // — deliberate truncation point.  Neither call grows any table so per-round
    // cost is stable across all ROUNDS.
    let snap_before_mixed = module.batch_stats().await;
    println!("[circles-bench] MIXED phase start ({ROUNDS} rounds)");
    let mixed_start = std::time::Instant::now();
    for round in 0..ROUNDS {
        // Hoist product! temporaries so the borrows outlive the futures vec.
        let a0 = product![100_u32];
        let a1 = product![0_u32];
        let a2 = product![0_u32];
        let a3 = product![0_u32];
        let a4 = product![0_u32];
        let futures: Vec<_> = vec![
            module.call_reducer_binary("run_game_circles", &a0),
            module.call_reducer_binary("update_position_with_velocity", &a1),
            module.call_reducer_binary("cross_join_circle_food", &a2),
            module.call_reducer_binary("update_position_all", &a3),
            module.call_reducer_binary("cross_join_all", &a4),
        ];
        let results = join_all(futures).await;
        for (idx, r) in results.iter().enumerate() {
            assert!(
                r.is_ok(),
                "MIXED round {round} call[{idx}] failed: {:?}",
                r.as_ref().err()
            );
        }
    }
    let mixed_total_ms = mixed_start.elapsed().as_millis();
    let mixed_mean_ms = mixed_total_ms as f64 / ROUNDS as f64;
    let snap_after_mixed = module.batch_stats().await;

    let (mixed_forks, mixed_batches, mixed_widths) =
        phase_deltas(&snap_before_mixed, &snap_after_mixed);
    println!(
        "BENCH-CIRCLES phase=mixed cap={} total_ms={} mean_ms={:.1} forks={} batches={} widths={}",
        cap_label, mixed_total_ms, mixed_mean_ms, mixed_forks, mixed_batches, mixed_widths
    );

    // ── FINAL SNAPSHOT ───────────────────────────────────────────────────────
    let snap_final = module.batch_stats().await;
    let (total_forks, total_batches, total_widths_str) = match &snap_final {
        Some(s) => (s.forks, s.batches, format!("{:?}", s.widths)),
        None => (0, 0, "N/A".to_string()),
    };
    let cal_ns_str = match &snap_final {
        Some(s) => format!("{:?}", s.calibrated_ns),
        None => "N/A".to_string(),
    };

    println!(
        "BENCH-CIRCLES-FINAL cap={} forks={} batches={} widths={} calibrated_ns={}",
        cap_label, total_forks, total_batches, total_widths_str, cal_ns_str
    );

    // RED-FLAG: pool is Ready but no forks fired — indicates an analyzer or
    // admission regression.  Print only; never assert (both cap arms must pass).
    if let Some(snap) = &snap_final
        && snap.pool_state == spacetimedb::host::PoolStateTag::Ready
        && total_forks == 0
    {
        println!(
            "WARNING: forks=0 with pool Ready — analyzer/admission regression suspect \
             (cap={cap_label} calibrated_ns={:?})",
            snap.calibrated_ns
        );
    }
}

/// Compute element-wise (forks_delta, batches_delta, widths_delta_string)
/// between two optional snapshots.  Falls back gracefully when either snapshot
/// is unavailable.
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
