#![allow(clippy::disallowed_macros)]
//! Hot-swap (republish) safety test for the reducer-batching scheduler.
//!
//! Validates DESIGN §19.1: learned access-set state is per-boot, owned by the
//! loop thread's `SchedulerState`. Republishing replaces the whole `ModuleHost`
//! (host_controller `Host::update_module` builds a fresh host + scheduler and
//! `send_replace`s it into the live watch channel), so learned tiers/strikes are
//! discarded for free — a stale learned set can never bleed across a republish
//! into a different schema/reducer layout. The database data, by contrast, must
//! survive the swap untouched.
//!
//! The handle is reused across the swap: `send_replace` keeps the watch channel,
//! so `module.batch_stats()` / `call_reducer_binary` transparently hit the new host.

use lazy_static::lazy_static;
use serial_test::serial;
use spacetimedb_lib::sats::product;
use spacetimedb_testing::modules::{CompilationMode, CompiledModule, LoggerRecord, DEFAULT_CONFIG};

const N: u64 = 5_000;
// Promotion fires at 8 committed inline runs (LEARN_MIN_RUNS); 14 is safety margin.
const LEARN_RUNS: usize = 14;

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

/// Parse the most recent `counts a=X b=Y` line from the JSON log.
fn parse_counts(log: &str) -> Option<(u64, u64)> {
    for line in log.lines().rev() {
        if line.is_empty() {
            continue;
        }
        let record: LoggerRecord = serde_json::from_str(line).ok()?;
        if let Some(rest) = record.message.strip_prefix("counts a=") {
            let mut parts = rest.split_whitespace();
            let a: u64 = parts.next()?.parse().ok()?;
            let b: u64 = parts.next()?.strip_prefix("b=")?.parse().ok()?;
            return Some((a, b));
        }
    }
    None
}

async fn promote_heavy_learn(module: &spacetimedb_testing::modules::ModuleHandle) -> u64 {
    for _ in 0..LEARN_RUNS {
        module
            .call_reducer_binary("heavy_learn", &product![N])
            .await
            .expect("heavy_learn failed");
    }
    module.batch_stats().await.map(|s| s.promotions).unwrap_or(0)
}

#[tokio::test]
#[serial]
async fn hotswap_resets_learned_state_and_preserves_data() {
    init_logger();
    let module = MODULE.load_module(DEFAULT_CONFIG, None).await;

    // Flag OFF so heavy_learn's observed access is the stable set {table_a}.
    module
        .call_reducer_binary("set_flag", &product![false])
        .await
        .expect("set_flag failed");

    // ── Learn on v1: promote the wildcard reducer heavy_learn ──────────────────
    let promotions_pre = promote_heavy_learn(&module).await;
    assert!(
        promotions_pre >= 1,
        "heavy_learn must promote before the swap (promotions={promotions_pre})"
    );

    // Capture committed DB state immediately before the swap.
    module
        .call_reducer_binary("log_counts", &product![])
        .await
        .expect("log_counts failed");
    let (a_pre, b_pre) = parse_counts(&module.read_log(None).await).expect("counts before swap");
    println!("[hotswap] pre-swap: promotions={promotions_pre} a={a_pre} b={b_pre}");

    // ── HOT SWAP: republish the same module bytes ──────────────────────────────
    module.republish(&MODULE).await.expect("republish failed");

    // ── SAFETY: the new host starts with NO learned state ──────────────────────
    let post = module.batch_stats().await.expect("stats after swap");
    assert_eq!(
        post.promotions, 0,
        "learned state must be discarded on hot-swap (promotions={})",
        post.promotions
    );
    assert_eq!(post.forks, 0, "fork counter must reset on hot-swap (forks={})", post.forks);
    assert_eq!(post.demotions, 0, "demotion counter must reset (demotions={})", post.demotions);
    assert_eq!(post.head_traps, 0, "head_traps must reset (head_traps={})", post.head_traps);
    assert_eq!(
        post.member_traps, 0,
        "member_traps must reset (member_traps={})",
        post.member_traps
    );
    println!("[hotswap] post-swap stats reset: {post:?}");

    // ── DATA must survive the swap untouched ───────────────────────────────────
    module
        .call_reducer_binary("log_counts", &product![])
        .await
        .expect("log_counts after swap failed");
    let (a_post, b_post) = parse_counts(&module.read_log(None).await).expect("counts after swap");
    assert_eq!(a_post, a_pre, "table_a data must survive hot-swap ({a_pre} -> {a_post})");
    assert_eq!(b_post, b_pre, "table_b data must survive hot-swap ({b_pre} -> {b_post})");

    // ── Re-learning works on the new host (Unknown again, learns from scratch) ──
    let promotions_relearn = promote_heavy_learn(&module).await;
    assert!(
        promotions_relearn >= 1,
        "re-learning must work after a hot-swap (promotions={promotions_relearn})"
    );

    // ── Correctness: ordinary reducers still commit + the DB grows consistently ─
    module
        .call_reducer_binary("heavy_a", &product![N])
        .await
        .expect("heavy_a after swap failed");
    module
        .call_reducer_binary("heavy_b", &product![N])
        .await
        .expect("heavy_b after swap failed");
    module
        .call_reducer_binary("log_counts", &product![])
        .await
        .expect("final log_counts failed");
    let (a_final, b_final) = parse_counts(&module.read_log(None).await).expect("final counts");
    assert!(a_final > a_post, "table_a must grow after post-swap writes ({a_post} -> {a_final})");
    assert!(b_final > b_post, "table_b must grow after post-swap writes ({b_post} -> {b_final})");

    println!(
        "[hotswap] PASS — learned state reset on swap, data preserved, re-learn ok \
         (relearn promotions={promotions_relearn}, final a={a_final} b={b_final})"
    );
}
