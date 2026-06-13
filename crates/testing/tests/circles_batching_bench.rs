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
        assert!(
            ra.is_ok(),
            "run_game_circles HETERO round {round} failed: {:?}",
            ra.err()
        );
        assert!(
            rb.is_ok(),
            "update_position_with_velocity HETERO round {round} failed: {:?}",
            rb.err()
        );
    }
    let hetero_total_ms = hetero_start.elapsed().as_millis();
    let hetero_mean_ms = hetero_total_ms as f64 / ROUNDS as f64;
    let snap_after_hetero = module.batch_stats().await;

    let (hetero_forks, hetero_batches, hetero_widths) = phase_deltas(&snap_before_hetero, &snap_after_hetero);
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
        assert!(
            ra.is_ok(),
            "run_game_circles HOMO-A round {round} failed: {:?}",
            ra.err()
        );
        assert!(
            rb.is_ok(),
            "run_game_circles HOMO-B round {round} failed: {:?}",
            rb.err()
        );
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

    let (mixed_forks, mixed_batches, mixed_widths) = phase_deltas(&snap_before_mixed, &snap_after_mixed);
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

// ── ia_loop seed parameters ────────────────────────────────────────────────────
// initial_load=50 → num_players=50, big_table=2500, biggest_table=5000.
// game_loop_enemy_ia cost ≈ O(num_players²) via get_targetables_near_quad;
// 50 players ≈ 2500 inner iterations × per-row updates — well above any
// realistic calibrated fork threshold while keeping the test tractable.
const IA_INITIAL_LOAD: u32 = 50;
// Promotion requires LEARN_MIN_RUNS(8) + LEARN_QUIET_RUNS(4) quiet runs.
// 16 inline runs guarantees promotion (promotes after run ~8-9).
const LEARN_RUNS: usize = 16;
// Concurrent rounds for the A/B measurement phase.
const LEARNED_ROUNDS: usize = 20;

/// Exercises the learned-tier fork path on the Release-wildcard `run_game_ia_loop`
/// reducer: seeds its world, promotes it from Unknown→Learned via inline capture runs,
/// then A/B-measures `run_game_circles ‖ run_game_ia_loop` to show width-2 forks on
/// the learned path where the static path could not fork at all.
#[tokio::test]
#[ignore]
async fn bench_circles_learned() {
    let cap_label = std::env::var("STDB_REDUCER_POOL_CAP").unwrap_or_else(|_| "1".to_string());

    // Fresh module instance so learning-phase stats start at zero.
    let module = MODULE.load_module(DEFAULT_CONFIG, None).await;

    // ── SEED ────────────────────────────────────────────────────────────────────
    // init_game_ia_loop seeds position/velocity/world/agent-state in one shot.
    // Circles tables are seeded separately so run_game_circles has a heavy body.
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
        "[learned-bench] seed done: ia_initial_load={IA_INITIAL_LOAD} \
         entity={SEED_ENTITY} circle={SEED_CIRCLE} food={SEED_FOOD}"
    );

    // ── PROMOTE run_game_ia_loop ─────────────────────────────────────────────────
    // Run inline (sequentially) so the scheduler captures its access set each time.
    // Promotion fires after LEARN_MIN_RUNS=8 committed runs with LEARN_QUIET_RUNS=4
    // consecutive non-novel observations.  16 runs guarantees promotion.
    println!("[learned-bench] promotion runs start (max={LEARN_RUNS})");
    let mut promotion_run: Option<usize> = None;
    let snap_before_learn = module.batch_stats().await;
    let promotions_before = snap_before_learn.as_ref().map(|s| s.promotions).unwrap_or(0);

    for i in 0..LEARN_RUNS {
        let t0 = std::time::Instant::now();
        let result = module
            .call_reducer_binary("run_game_ia_loop", &product![IA_INITIAL_LOAD])
            .await;
        let elapsed_us = t0.elapsed().as_micros();
        match &result {
            Err(e) => {
                println!(
                    "[learned-bench] STOP: run_game_ia_loop run {i} errored (seed insufficient or \
                     panic): {e:?}"
                );
                panic!("run_game_ia_loop errored at promotion run {i}: {e:?}");
            }
            Ok(_) => {}
        }
        println!("[learned-bench] learn-run[{i}] elapsed={elapsed_us}µs");

        if let Some(snap) = module.batch_stats().await {
            if snap.promotions > promotions_before && promotion_run.is_none() {
                promotion_run = Some(i + 1);
                println!(
                    "[learned-bench] promotion observed after run {} (promotions={})",
                    i + 1,
                    snap.promotions
                );
            }
        }
    }

    let snap_after_learn = module.batch_stats().await;
    let promotions_after = snap_after_learn.as_ref().map(|s| s.promotions).unwrap_or(0);
    if promotions_after <= promotions_before {
        println!(
            "[learned-bench] FINDING: promotions did not tick after {LEARN_RUNS} runs \
             (promotions={promotions_after}) — run_game_ia_loop may not be Unknown-tier \
             (not Release-wildcard as assumed).  Continuing to measure anyway."
        );
    } else {
        println!(
            "[learned-bench] promotion confirmed: promotions before={promotions_before} \
             after={promotions_after} first_at_run={:?}",
            promotion_run
        );
    }

    // ── WARM POOL ────────────────────────────────────────────────────────────────
    // Drive run_game_circles sequentially until the pool reaches Ready or WARM_MAX.
    println!("[learned-bench] warm-up start (cap={cap_label}, max={WARM_MAX})");
    for i in 0..WARM_MAX {
        let t0 = std::time::Instant::now();
        module
            .call_reducer_binary("run_game_circles", &product![100_u32])
            .await
            .expect("run_game_circles warm-up failed");
        let elapsed_us = t0.elapsed().as_micros();
        println!("[learned-bench] warm-up[{i}] elapsed={elapsed_us}µs");

        if let Some(snap) = module.batch_stats().await {
            if snap.pool_state == spacetimedb::host::PoolStateTag::Ready {
                println!(
                    "[learned-bench] pool Ready after {} warm-up calls; calibrated_ns={:?}",
                    i + 1,
                    snap.calibrated_ns
                );
                break;
            }
            if i + 1 == WARM_MAX {
                println!(
                    "[learned-bench] pool did not reach Ready in {WARM_MAX} warm-up calls; \
                     pool_state={:?} calibrated_ns={:?}",
                    snap.pool_state, snap.calibrated_ns
                );
            }
        }
    }

    // ── A/B MEASURE: run_game_circles ‖ run_game_ia_loop ────────────────────────
    let snap_before_learned = module.batch_stats().await;
    println!("[learned-bench] LEARNED phase start ({LEARNED_ROUNDS} rounds, cap={cap_label})");
    let learned_start = std::time::Instant::now();
    for round in 0..LEARNED_ROUNDS {
        let args_circles = product![100_u32];
        let args_ia = product![IA_INITIAL_LOAD];
        let fa = module.call_reducer_binary("run_game_circles", &args_circles);
        let fb = module.call_reducer_binary("run_game_ia_loop", &args_ia);
        let (ra, rb) = tokio::join!(fa, fb);
        assert!(
            ra.is_ok(),
            "run_game_circles LEARNED round {round} failed: {:?}",
            ra.err()
        );
        assert!(
            rb.is_ok(),
            "run_game_ia_loop LEARNED round {round} failed: {:?}",
            rb.err()
        );
    }
    let learned_total_ms = learned_start.elapsed().as_millis();
    let learned_mean_ms = learned_total_ms as f64 / LEARNED_ROUNDS as f64;
    let snap_after_learned = module.batch_stats().await;

    let (learned_forks, learned_batches, learned_widths) = phase_deltas(&snap_before_learned, &snap_after_learned);

    let (learned_promotions, learned_demotions, learned_head_traps, learned_member_traps) =
        match (&snap_before_learned, &snap_after_learned) {
            (Some(b), Some(a)) => (
                a.promotions.saturating_sub(b.promotions),
                a.demotions.saturating_sub(b.demotions),
                a.head_traps.saturating_sub(b.head_traps),
                a.member_traps.saturating_sub(b.member_traps),
            ),
            (None, Some(a)) => (a.promotions, a.demotions, a.head_traps, a.member_traps),
            _ => (0, 0, 0, 0),
        };

    println!(
        "BENCH-CIRCLES phase=learned cap={} total_ms={} mean_ms={:.1} forks={} batches={} widths={}",
        cap_label, learned_total_ms, learned_mean_ms, learned_forks, learned_batches, learned_widths
    );
    println!(
        "[learned-bench] learned-counters promotions={learned_promotions} \
         demotions={learned_demotions} head_traps={learned_head_traps} \
         member_traps={learned_member_traps}"
    );

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
        "BENCH-CIRCLES-LEARNED-FINAL cap={} forks={} batches={} widths={} calibrated_ns={}",
        cap_label, total_forks, total_batches, total_widths_str, cal_ns_str
    );

    if let Some(snap) = &snap_final
        && snap.pool_state == spacetimedb::host::PoolStateTag::Ready
        && learned_forks == 0
    {
        println!(
            "WARNING: forks=0 with pool Ready in learned phase — analyzer/admission regression \
             suspect (cap={cap_label} calibrated_ns={:?})",
            snap.calibrated_ns
        );
    }
}

/// Three-way comparison of reducer-batching phases on the `benchmarks` module.
///
/// Emits `THREEWAY …` rows for two workloads (static pair, lost pair) across three
/// scheduler configurations: base (cap=0, inline), v1/phase-1-5 (cap=1, wildcard
/// excluded), and v2/phase-6 (cap=1, wildcard promoted to Learned).  The operator
/// runs this test twice — once with `STDB_REDUCER_POOL_CAP=0` and once with `=1` —
/// and assembles the 3-column table from the printed rows.
#[tokio::test]
#[ignore]
async fn bench_three_way() {
    let cap_label = std::env::var("STDB_REDUCER_POOL_CAP").unwrap_or_else(|_| "1".to_string());

    // ═══════════════════════════════════════════════════════════════════════════
    // WORKLOAD STATIC — run_game_circles ‖ update_position_with_velocity
    // Disjoint static write sets; forks under cap=1 regardless of phase (v1≡v2).
    // Under cap=0: this is the base lane.
    // ═══════════════════════════════════════════════════════════════════════════
    let static_mean_ms;
    {
        let module = MODULE.load_module(DEFAULT_CONFIG, None).await;

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
            "[three-way/static] seed done: entity={SEED_ENTITY} circle={SEED_CIRCLE} \
             food={SEED_FOOD} position={SEED_POSITION} velocity={SEED_VELOCITY}"
        );

        println!("[three-way/static] warm-up start (cap={cap_label}, max={WARM_MAX})");
        for i in 0..WARM_MAX {
            let t0 = std::time::Instant::now();
            module
                .call_reducer_binary("run_game_circles", &product![100_u32])
                .await
                .expect("run_game_circles warm-up failed");
            let elapsed_us = t0.elapsed().as_micros();
            println!("[three-way/static] warm-up[{i}] elapsed={elapsed_us}µs");

            if let Some(snap) = module.batch_stats().await {
                if snap.pool_state == spacetimedb::host::PoolStateTag::Ready {
                    println!(
                        "[three-way/static] pool Ready after {} warm-up calls; calibrated_ns={:?}",
                        i + 1,
                        snap.calibrated_ns
                    );
                    break;
                }
                if i + 1 == WARM_MAX {
                    println!(
                        "[three-way/static] pool did not reach Ready in {WARM_MAX} warm-up calls; \
                         pool_state={:?} calibrated_ns={:?}",
                        snap.pool_state, snap.calibrated_ns
                    );
                }
            }
        }

        let snap_before = module.batch_stats().await;
        println!("[three-way/static] measure start ({ROUNDS} rounds, cap={cap_label})");
        let t_start = std::time::Instant::now();
        for round in 0..ROUNDS {
            let args_circles = product![100_u32];
            let args_vel = product![0_u32];
            let fa = module.call_reducer_binary("run_game_circles", &args_circles);
            let fb = module.call_reducer_binary("update_position_with_velocity", &args_vel);
            let (ra, rb) = tokio::join!(fa, fb);
            assert!(
                ra.is_ok(),
                "run_game_circles static round {round} failed: {:?}",
                ra.err()
            );
            assert!(
                rb.is_ok(),
                "update_position_with_velocity static round {round} failed: {:?}",
                rb.err()
            );
        }
        let total_ms = t_start.elapsed().as_millis();
        let mean_ms = total_ms as f64 / ROUNDS as f64;
        let snap_after = module.batch_stats().await;

        let (forks, _batches, widths) = phase_deltas(&snap_before, &snap_after);
        static_mean_ms = mean_ms;
        println!("THREEWAY workload=static cap={cap_label} mean_ms={mean_ms:.1} forks={forks} widths={widths}");
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // WORKLOAD LOST — v1 arm (UNPROMOTED)
    // run_game_circles ‖ run_game_ia_loop, ia_loop kept as Unknown-tier wildcard.
    // Under cap=0: base-lost.  Under cap=1: phase-1-5 (wildcard excluded → no fork).
    // Stop measuring as soon as promotions ticks so the window is pre-promotion.
    // ═══════════════════════════════════════════════════════════════════════════
    let lost_v1_mean_ms;
    {
        let module = MODULE.load_module(DEFAULT_CONFIG, None).await;

        module
            .call_reducer_binary("init_game_ia_loop", &product![IA_INITIAL_LOAD])
            .await
            .expect("init_game_ia_loop failed");
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
            "[three-way/lost-v1] seed done: ia_initial_load={IA_INITIAL_LOAD} \
             entity={SEED_ENTITY} circle={SEED_CIRCLE} food={SEED_FOOD}"
        );

        println!("[three-way/lost-v1] warm-up start (cap={cap_label}, max={WARM_MAX})");
        for i in 0..WARM_MAX {
            let t0 = std::time::Instant::now();
            module
                .call_reducer_binary("run_game_circles", &product![100_u32])
                .await
                .expect("run_game_circles warm-up failed");
            let elapsed_us = t0.elapsed().as_micros();
            println!("[three-way/lost-v1] warm-up[{i}] elapsed={elapsed_us}µs");

            if let Some(snap) = module.batch_stats().await {
                if snap.pool_state == spacetimedb::host::PoolStateTag::Ready {
                    println!(
                        "[three-way/lost-v1] pool Ready after {} warm-up calls; calibrated_ns={:?}",
                        i + 1,
                        snap.calibrated_ns
                    );
                    break;
                }
                if i + 1 == WARM_MAX {
                    println!(
                        "[three-way/lost-v1] pool did not reach Ready in {WARM_MAX} warm-up calls; \
                         pool_state={:?} calibrated_ns={:?}",
                        snap.pool_state, snap.calibrated_ns
                    );
                }
            }
        }

        let snap_pre_phase = module.batch_stats().await;
        let promotions_baseline = snap_pre_phase.as_ref().map(|s| s.promotions).unwrap_or(0);

        println!("[three-way/lost-v1] measure start (up to {ROUNDS} rounds, cap={cap_label})");
        let t_start = std::time::Instant::now();
        let mut rounds_measured = 0usize;
        let mut forks_v1 = 0u64;
        let mut promotions_at_stop = promotions_baseline;

        for round in 0..ROUNDS {
            let snap_round_before = module.batch_stats().await;

            let args_circles = product![100_u32];
            let args_ia = product![IA_INITIAL_LOAD];
            let fa = module.call_reducer_binary("run_game_circles", &args_circles);
            let fb = module.call_reducer_binary("run_game_ia_loop", &args_ia);
            let (ra, rb) = tokio::join!(fa, fb);
            assert!(
                ra.is_ok(),
                "run_game_circles lost-v1 round {round} failed: {:?}",
                ra.err()
            );
            assert!(
                rb.is_ok(),
                "run_game_ia_loop lost-v1 round {round} failed: {:?}",
                rb.err()
            );

            rounds_measured += 1;

            if let Some(snap) = module.batch_stats().await {
                if snap.promotions > promotions_baseline {
                    promotions_at_stop = snap.promotions;
                    println!(
                        "[three-way/lost-v1] promotion detected at round {round}; \
                         stopping pre-promotion window"
                    );
                    break;
                }
                let (delta_forks, _, _) = phase_deltas(&snap_round_before, &Some(snap));
                forks_v1 += delta_forks;
            }
        }

        let total_ms = t_start.elapsed().as_millis();
        lost_v1_mean_ms = if rounds_measured > 0 {
            total_ms as f64 / rounds_measured as f64
        } else {
            0.0
        };

        println!(
            "THREEWAY workload=lost-v1 cap={cap_label} mean_ms={lost_v1_mean_ms:.1} \
             rounds_measured={rounds_measured} forks={forks_v1} \
             promotions_at_stop={promotions_at_stop}"
        );
    }

    // ═══════════════════════════════════════════════════════════════════════════
    // WORKLOAD LOST — v2 arm (PROMOTED)
    // Same pair, but ia_loop is promoted to Learned before the measurement phase.
    // Under cap=0: base-lost (promotes but never forks).  Under cap=1: phase-6.
    // ═══════════════════════════════════════════════════════════════════════════
    let lost_v2_mean_ms;
    let lost_v2_forks;
    {
        let module = MODULE.load_module(DEFAULT_CONFIG, None).await;

        module
            .call_reducer_binary("init_game_ia_loop", &product![IA_INITIAL_LOAD])
            .await
            .expect("init_game_ia_loop failed");
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
            "[three-way/lost-v2] seed done: ia_initial_load={IA_INITIAL_LOAD} \
             entity={SEED_ENTITY} circle={SEED_CIRCLE} food={SEED_FOOD}"
        );

        println!("[three-way/lost-v2] promotion runs start (max={LEARN_RUNS})");
        let mut promotion_run: Option<usize> = None;
        let snap_before_learn = module.batch_stats().await;
        let promotions_before = snap_before_learn.as_ref().map(|s| s.promotions).unwrap_or(0);

        for i in 0..LEARN_RUNS {
            let t0 = std::time::Instant::now();
            let result = module
                .call_reducer_binary("run_game_ia_loop", &product![IA_INITIAL_LOAD])
                .await;
            let elapsed_us = t0.elapsed().as_micros();
            match &result {
                Err(e) => {
                    println!(
                        "[three-way/lost-v2] STOP: run_game_ia_loop run {i} errored \
                         (seed insufficient or panic): {e:?}"
                    );
                    panic!("run_game_ia_loop errored at promotion run {i}: {e:?}");
                }
                Ok(_) => {}
            }
            println!("[three-way/lost-v2] learn-run[{i}] elapsed={elapsed_us}µs");

            if let Some(snap) = module.batch_stats().await {
                if snap.promotions > promotions_before && promotion_run.is_none() {
                    promotion_run = Some(i + 1);
                    println!(
                        "[three-way/lost-v2] promotion observed after run {} (promotions={})",
                        i + 1,
                        snap.promotions
                    );
                }
            }
        }

        let snap_after_learn = module.batch_stats().await;
        let promotions_after = snap_after_learn.as_ref().map(|s| s.promotions).unwrap_or(0);
        if promotions_after <= promotions_before {
            println!(
                "[three-way/lost-v2] FINDING: promotions did not tick after {LEARN_RUNS} runs \
                 (promotions={promotions_after}) — run_game_ia_loop may not be Unknown-tier. \
                 Continuing to measure anyway."
            );
        } else {
            println!(
                "[three-way/lost-v2] promotion confirmed: before={promotions_before} \
                 after={promotions_after} first_at_run={:?}",
                promotion_run
            );
        }

        println!("[three-way/lost-v2] warm-up start (cap={cap_label}, max={WARM_MAX})");
        for i in 0..WARM_MAX {
            let t0 = std::time::Instant::now();
            module
                .call_reducer_binary("run_game_circles", &product![100_u32])
                .await
                .expect("run_game_circles warm-up failed");
            let elapsed_us = t0.elapsed().as_micros();
            println!("[three-way/lost-v2] warm-up[{i}] elapsed={elapsed_us}µs");

            if let Some(snap) = module.batch_stats().await {
                if snap.pool_state == spacetimedb::host::PoolStateTag::Ready {
                    println!(
                        "[three-way/lost-v2] pool Ready after {} warm-up calls; calibrated_ns={:?}",
                        i + 1,
                        snap.calibrated_ns
                    );
                    break;
                }
                if i + 1 == WARM_MAX {
                    println!(
                        "[three-way/lost-v2] pool did not reach Ready in {WARM_MAX} warm-up calls; \
                         pool_state={:?} calibrated_ns={:?}",
                        snap.pool_state, snap.calibrated_ns
                    );
                }
            }
        }

        let snap_before_learned = module.batch_stats().await;
        println!("[three-way/lost-v2] LEARNED phase start ({LEARNED_ROUNDS} rounds, cap={cap_label})");
        let t_start = std::time::Instant::now();
        for round in 0..LEARNED_ROUNDS {
            let args_circles = product![100_u32];
            let args_ia = product![IA_INITIAL_LOAD];
            let fa = module.call_reducer_binary("run_game_circles", &args_circles);
            let fb = module.call_reducer_binary("run_game_ia_loop", &args_ia);
            let (ra, rb) = tokio::join!(fa, fb);
            assert!(
                ra.is_ok(),
                "run_game_circles lost-v2 round {round} failed: {:?}",
                ra.err()
            );
            assert!(
                rb.is_ok(),
                "run_game_ia_loop lost-v2 round {round} failed: {:?}",
                rb.err()
            );
        }
        let total_ms = t_start.elapsed().as_millis();
        lost_v2_mean_ms = total_ms as f64 / LEARNED_ROUNDS as f64;
        let snap_after_learned = module.batch_stats().await;

        let (forks, _batches, widths) = phase_deltas(&snap_before_learned, &snap_after_learned);
        lost_v2_forks = forks;

        let (_learned_promotions, learned_demotions, learned_head_traps, learned_member_traps) =
            match (&snap_before_learned, &snap_after_learned) {
                (Some(b), Some(a)) => (
                    a.promotions.saturating_sub(b.promotions),
                    a.demotions.saturating_sub(b.demotions),
                    a.head_traps.saturating_sub(b.head_traps),
                    a.member_traps.saturating_sub(b.member_traps),
                ),
                (None, Some(a)) => (a.promotions, a.demotions, a.head_traps, a.member_traps),
                _ => (0, 0, 0, 0),
            };

        println!(
            "THREEWAY workload=lost-v2 cap={cap_label} mean_ms={lost_v2_mean_ms:.1} \
             forks={forks} widths={widths} promotion_run={promotion_run:?} \
             demotions={learned_demotions} head_traps={learned_head_traps} \
             member_traps={learned_member_traps}"
        );

        let snap_final = module.batch_stats().await;
        if let Some(snap) = &snap_final
            && snap.pool_state == spacetimedb::host::PoolStateTag::Ready
            && forks == 0
        {
            println!(
                "WARNING: forks=0 with pool Ready in lost-v2 phase — analyzer/admission \
                 regression suspect (cap={cap_label} calibrated_ns={:?})",
                snap.calibrated_ns
            );
        }
    }

    // ── SUMMARY ─────────────────────────────────────────────────────────────────
    println!(
        "THREEWAY-SUMMARY cap={cap_label} static_mean_ms={static_mean_ms:.1} \
         lost_v1_mean_ms={lost_v1_mean_ms:.1} lost_v2_mean_ms={lost_v2_mean_ms:.1} \
         lost_v2_forks={lost_v2_forks}"
    );
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
