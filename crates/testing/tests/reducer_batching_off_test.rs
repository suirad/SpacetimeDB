#![allow(clippy::disallowed_macros)]
//! Integration test for the pool-off (kill-switch) path.
//!
//! Sets `STDB_REDUCER_POOL_CAP=0` BEFORE any module load so the scheduler is
//! constructed with `PoolState::Off`. Verifies that no forks ever occur and
//! that all reducers still commit correctly.
//!
//! This lives in a separate file so it runs in its own test process and the
//! env-var set cannot bleed into other tests.

use lazy_static::lazy_static;
use spacetimedb_lib::sats::product;
use spacetimedb_testing::modules::{CompilationMode, CompiledModule, LoggerRecord, DEFAULT_CONFIG};

const N: u64 = 5_000;

// Set the env var at static-init time — this runs before any module is loaded.
fn ensure_pool_off() {
    // Safety: this is the only thread alive at test startup in an isolated process.
    unsafe { std::env::set_var("STDB_REDUCER_POOL_CAP", "0") };
}

lazy_static! {
    static ref MODULE: CompiledModule = {
        ensure_pool_off();
        CompiledModule::compile("reducer-batching-fixture", CompilationMode::Debug)
    };
}

fn init_logger() {
    let _ = env_logger::builder()
        .parse_filters("spacetimedb=info")
        .is_test(true)
        .try_init();
}

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

#[tokio::test]
async fn pool_off_no_spawn_no_forks_all_commit() {
    init_logger();
    ensure_pool_off(); // belt-and-suspenders in case static init order is tricky

    let module = MODULE.load_module(DEFAULT_CONFIG, None).await;

    // Run several heavy sequential calls — with pool off these just run inline.
    for _ in 0..5 {
        module
            .call_reducer_binary("heavy_a", &product![N])
            .await
            .expect("heavy_a should succeed even with pool off");
    }

    // Run concurrent pairs — no fork should happen.
    let mut pairs: u64 = 0;
    for _ in 0..10 {
        let args_a = product![N];
        let args_b = product![N];
        let fa = module.call_reducer_binary("heavy_a", &args_a);
        let fb = module.call_reducer_binary("heavy_b", &args_b);
        let (ra, rb) = tokio::join!(fa, fb);
        assert!(ra.is_ok(), "heavy_a should succeed with pool off");
        assert!(rb.is_ok(), "heavy_b should succeed with pool off");
        pairs += 1;
    }

    // Assert pool stayed Off and no forks ever happened.
    let snap = module
        .batch_stats()
        .await
        .expect("batch_stats should return Some for a wasm host");

    assert_eq!(
        snap.pool_state,
        spacetimedb::host::PoolStateTag::Off,
        "pool should remain Off with STDB_REDUCER_POOL_CAP=0"
    );
    assert_eq!(snap.forks, 0, "no forks should occur with pool Off");
    assert!(
        snap.calibrated_ns.is_none(),
        "no calibration should happen with pool Off"
    );

    // DB consistency.
    module
        .call_reducer_binary("log_counts", &product![])
        .await
        .expect("log_counts failed");
    let log = module.read_log(None).await;
    let (a_count, b_count) = parse_counts(&log).expect("log_counts output not found in log");

    // 5 warm-up heavy_a calls + pairs × N = (5 + pairs) × N rows in a.
    let expected_a = (5 + pairs) * N;
    let expected_b = pairs * N;
    assert_eq!(a_count, expected_a, "table_a row count mismatch with pool off");
    assert_eq!(b_count, expected_b, "table_b row count mismatch with pool off");

    println!(
        "[pool_off_no_spawn_no_forks_all_commit] PASS — forks={} a={a_count} b={b_count}",
        snap.forks
    );
}
