#![allow(clippy::disallowed_macros)]
//! C# analogue of `bench_circles_learned`: base (cap=0) vs v2 (cap=1, learned)
//! comparison of reducer batching on the `benchmarks-cs` module (circles / ia_loop).
//!
//! C# has no static-batching path — the static analyzer is Rust-symbol-keyed and
//! returns wildcard for every C# reducer. Both `run_game_circles` and
//! `run_game_ia_loop` therefore start at `Unknown` tier. v2's only batching lever
//! is Phase-6 LEARNING (Unknown→Learned via inline capture). Run under both caps:
//!   STDB_REDUCER_POOL_CAP=0  → base lane (no pool, never forks)
//!   STDB_REDUCER_POOL_CAP=1  → v2 lane  (learned fork path)
//!
//! Run explicitly (skipped in normal CI):
//!   cargo test -p spacetimedb-testing --test csharp_batching_bench -- --ignored --nocapture
//!
//! Set STDB_REDUCER_POOL_CAP externally before running to label the output;
//! default label is "1". This test never sets that variable itself.

use lazy_static::lazy_static;
use spacetimedb_lib::sats::product;
use spacetimedb_testing::modules::{CompilationMode, CompiledModule, ModuleHandle, DEFAULT_CONFIG};

const IA_INITIAL_LOAD: u32 = 50;
const CIRCLES_LOAD: u32 = 100;
// Reduced circles load (~ia_loop body cost) for a balanced disjoint pair — the
// imbalanced pairs fork but circles dominates, hiding the overlap.
const CIRCLES_LIGHT: u32 = 40;
const SEED_ENTITY: u32 = 100;
const SEED_CIRCLE: u32 = 100;
const SEED_FOOD: u32 = 100;
const LEARN_RUNS: usize = 16;
const WARM_MAX: usize = 30;
const ROUNDS: usize = 20;

lazy_static! {
    static ref MODULE: CompiledModule = CompiledModule::compile("benchmarks-cs", CompilationMode::Release);
}

/// Base (cap=0) vs v2 (cap=1, learned) measurement of `run_game_circles ‖ run_game_ia_loop`
/// on the C# `benchmarks-cs` module. Both reducers are wildcard for C# wasm, so both
/// must be promoted before they can fork as a Learned×Learned pair.
#[tokio::test]
#[ignore]
async fn bench_csharp_base_v2() {
    let cap_label = std::env::var("STDB_REDUCER_POOL_CAP").unwrap_or_else(|_| "1".to_string());

    let module = MODULE.load_module(DEFAULT_CONFIG, None).await;

    module
        .call_reducer_binary("init_game_ia_loop", &product![IA_INITIAL_LOAD])
        .await
        .expect("init_game_ia_loop failed — world not seeded");
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

    println!(
        "[csbench] seed done: ia_initial_load={IA_INITIAL_LOAD} \
         entity={SEED_ENTITY} circle={SEED_CIRCLE} food={SEED_FOOD}"
    );

    let snap_before_learn = module.batch_stats().await;
    let promotions_before = snap_before_learn.as_ref().map(|s| s.promotions).unwrap_or(0);

    println!("[csbench] promoting run_game_circles (max={LEARN_RUNS} runs)");
    let mut circles_promotion_run: Option<usize> = None;
    for i in 0..LEARN_RUNS {
        let result = module
            .call_reducer_binary("run_game_circles", &product![CIRCLES_LOAD])
            .await;
        if let Err(e) = result {
            panic!("run_game_circles errored at promotion run {i}: {e:?}");
        }
        if let Some(snap) = module.batch_stats().await {
            if snap.promotions > promotions_before && circles_promotion_run.is_none() {
                circles_promotion_run = Some(i + 1);
                println!(
                    "[csbench] run_game_circles promotion observed after run {} (promotions={})",
                    i + 1,
                    snap.promotions
                );
            }
        }
    }

    println!("[csbench] promoting run_game_ia_loop (max={LEARN_RUNS} runs)");
    let mut ia_promotion_run: Option<usize> = None;
    let snap_mid_learn = module.batch_stats().await;
    let promotions_mid = snap_mid_learn.as_ref().map(|s| s.promotions).unwrap_or(0);
    for i in 0..LEARN_RUNS {
        let result = module
            .call_reducer_binary("run_game_ia_loop", &product![IA_INITIAL_LOAD])
            .await;
        if let Err(e) = result {
            panic!("run_game_ia_loop errored at promotion run {i}: {e:?}");
        }
        if let Some(snap) = module.batch_stats().await {
            if snap.promotions > promotions_mid && ia_promotion_run.is_none() {
                ia_promotion_run = Some(i + 1);
                println!(
                    "[csbench] run_game_ia_loop promotion observed after run {} (promotions={})",
                    i + 1,
                    snap.promotions
                );
            }
        }
    }

    println!("[csbench] promoting update_position_with_velocity (max={LEARN_RUNS} runs)");
    let mut upv_promotion_run: Option<usize> = None;
    let snap_mid2_learn = module.batch_stats().await;
    let promotions_mid2 = snap_mid2_learn.as_ref().map(|s| s.promotions).unwrap_or(0);
    for i in 0..LEARN_RUNS {
        let result = module
            .call_reducer_binary("update_position_with_velocity", &product![0_u32])
            .await;
        if let Err(e) = result {
            panic!("update_position_with_velocity errored at promotion run {i}: {e:?}");
        }
        if let Some(snap) = module.batch_stats().await {
            if snap.promotions > promotions_mid2 && upv_promotion_run.is_none() {
                upv_promotion_run = Some(i + 1);
                println!(
                    "[csbench] update_position_with_velocity promotion observed after run {} (promotions={})",
                    i + 1,
                    snap.promotions
                );
            }
        }
    }

    let snap_after_learn = module.batch_stats().await;
    let promotions_after = snap_after_learn.as_ref().map(|s| s.promotions).unwrap_or(0);
    if promotions_after < promotions_before + 3 {
        println!(
            "FINDING: not all C# reducers promoted — capture may not fire for C# wasm, \
             or analyzer did not wildcard as expected; promotions={}",
            promotions_after
        );
    } else {
        println!(
            "[csbench] promotion confirmed: promotions before={promotions_before} \
             after={promotions_after} circles_first_at_run={circles_promotion_run:?} \
             ia_first_at_run={ia_promotion_run:?} upv_first_at_run={upv_promotion_run:?}"
        );
    }

    println!("[csbench] warm-up start (cap={cap_label}, max={WARM_MAX})");
    for i in 0..WARM_MAX {
        let t0 = std::time::Instant::now();
        module
            .call_reducer_binary("run_game_circles", &product![CIRCLES_LOAD])
            .await
            .expect("run_game_circles warm-up failed");
        let elapsed_us = t0.elapsed().as_micros();
        println!("[csbench] warm-up[{i}] elapsed={elapsed_us}µs");

        if let Some(snap) = module.batch_stats().await {
            if snap.pool_state == spacetimedb::host::PoolStateTag::Ready {
                println!(
                    "[csbench] pool Ready after {} warm-up calls; calibrated_ns={:?}",
                    i + 1,
                    snap.calibrated_ns
                );
                break;
            }
            if i + 1 == WARM_MAX {
                println!(
                    "[csbench] pool did not reach Ready in {WARM_MAX} warm-up calls; \
                     pool_state={:?} calibrated_ns={:?}",
                    snap.pool_state, snap.calibrated_ns
                );
            }
        }
    }

    // Solo body times expose pair imbalance: overlap only saves wall-time when the
    // two bodies are comparable; if one dominates, forks fire but mean is unchanged.
    let solo_circles_ms = solo_time(&module, "run_game_circles", CIRCLES_LOAD).await;
    let solo_ia_ms = solo_time(&module, "run_game_ia_loop", IA_INITIAL_LOAD).await;
    let solo_upv_ms = solo_time(&module, "update_position_with_velocity", 0).await;
    println!("CSBENCH-SOLO cap={cap_label} circles_ms={solo_circles_ms} ia_loop_ms={solo_ia_ms} upv_ms={solo_upv_ms}");

    // Imbalanced pair (circles ≫ ia_loop) — forks but little wall gain.
    measure_pair(
        &module,
        &cap_label,
        "circles_ia_loop",
        ("run_game_circles", CIRCLES_LOAD),
        ("run_game_ia_loop", IA_INITIAL_LOAD),
    )
    .await;
    // Balanced heavy disjoint pair — the real v2 speedup test.
    measure_pair(
        &module,
        &cap_label,
        "circles_upv",
        ("run_game_circles", CIRCLES_LOAD),
        ("update_position_with_velocity", 0),
    )
    .await;
    // Balanced via reduced circles load (~comparable body cost) — clean overlap test.
    measure_pair(
        &module,
        &cap_label,
        "balanced_circles_ia",
        ("run_game_circles", CIRCLES_LIGHT),
        ("run_game_ia_loop", IA_INITIAL_LOAD),
    )
    .await;

    let snap_final = module.batch_stats().await;
    let (total_forks, total_batches, total_widths_str) = match &snap_final {
        Some(s) => (s.forks, s.batches, format!("{:?}", s.widths)),
        None => (0, 0, "N/A".to_string()),
    };
    let cal_ns_str = match &snap_final {
        Some(s) => format!("{:?}", s.calibrated_ns),
        None => "N/A".to_string(),
    };
    let pool_state_str = match &snap_final {
        Some(s) => format!("{:?}", s.pool_state),
        None => "N/A".to_string(),
    };

    println!(
        "CSBENCH-FINAL cap={cap_label} forks={total_forks} batches={total_batches} \
         widths={total_widths_str} calibrated_ns={cal_ns_str} pool_state={pool_state_str}"
    );

    if let Some(snap) = &snap_final
        && snap.pool_state == spacetimedb::host::PoolStateTag::Ready
        && snap.forks == 0
    {
        println!(
            "WARNING: forks=0 with pool Ready — C# reducers may not have promoted \
             (cap={cap_label})"
        );
    }
}

/// Time a single committed call of `name(arg)` in ms.
async fn solo_time(module: &ModuleHandle, name: &str, arg: u32) -> u128 {
    let t = std::time::Instant::now();
    module
        .call_reducer_binary(name, &product![arg])
        .await
        .unwrap_or_else(|e| panic!("solo {name} failed: {e:?}"));
    t.elapsed().as_millis()
}

/// Run `ROUNDS` concurrent rounds of `a ‖ b` and print a `CSBENCH workload={label}` line
/// with mean ms/round and the fork/width deltas across the phase.
async fn measure_pair(module: &ModuleHandle, cap_label: &str, label: &str, a: (&str, u32), b: (&str, u32)) {
    let before = module.batch_stats().await;
    let args_a = product![a.1];
    let args_b = product![b.1];
    let start = std::time::Instant::now();
    for round in 0..ROUNDS {
        let fa = module.call_reducer_binary(a.0, &args_a);
        let fb = module.call_reducer_binary(b.0, &args_b);
        let (ra, rb) = tokio::join!(fa, fb);
        ra.unwrap_or_else(|e| panic!("{} round {round}: {e:?}", a.0));
        rb.unwrap_or_else(|e| panic!("{} round {round}: {e:?}", b.0));
    }
    let mean_ms = start.elapsed().as_millis() as f64 / ROUNDS as f64;
    let after = module.batch_stats().await;
    let (forks, batches, widths) = phase_deltas(&before, &after);
    println!(
        "CSBENCH workload={label} cap={cap_label} mean_ms={mean_ms:.1} \
         forks={forks} batches={batches} widths={widths}"
    );
}

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
