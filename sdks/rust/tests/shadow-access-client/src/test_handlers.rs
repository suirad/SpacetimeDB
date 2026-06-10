use crate::module_bindings::*;

use spacetimedb_sdk::{DbConnectionBuilder, DbContext};
use std::sync::{Arc, Mutex};
use test_counter::TestCounter;

const LOCALHOST: &str = "http://localhost:3000";

#[cfg(not(target_arch = "wasm32"))]
async fn build_and_run(builder: DbConnectionBuilder<RemoteModule>) -> DbConnection {
    let conn = builder.build().unwrap();
    conn.run_threaded();
    conn
}

#[cfg(target_arch = "wasm32")]
async fn build_and_run(builder: DbConnectionBuilder<RemoteModule>) -> DbConnection {
    let conn = builder.build().await.unwrap();
    conn.run_background_task();
    conn
}

pub async fn run(db_name: &str) {
    let test_counter = TestCounter::new();
    let sentinel_result = test_counter.add_test("sentinel_read_c");
    let sentinel_result = Arc::new(Mutex::new(Some(sentinel_result)));

    let name = db_name.to_owned();
    let sentinel_result_clone = sentinel_result.clone();

    let conn = build_and_run(
        DbConnection::builder()
            .with_database_name(name)
            .with_uri(LOCALHOST)
            .on_connect(move |ctx, _, _| {
                // P1: 200 calls alternating write_a/write_b — fire and forget, no awaiting per call
                for i in 0u64..200 {
                    if i % 2 == 0 {
                        ctx.reducers.write_a(i).unwrap();
                    } else {
                        ctx.reducers.write_b(i).unwrap();
                    }
                }
            })
            .on_connect_error(|_ctx, err| panic!("connect error: {err:?}")),
    )
    .await;

    std::thread::sleep(std::time::Duration::from_secs(2));

    // P2: 50 calls alternating heavy_a/heavy_b
    for i in 0u32..50 {
        if i % 2 == 0 {
            conn.reducers.heavy_a(500).unwrap();
        } else {
            conn.reducers.heavy_b(500).unwrap();
        }
    }

    std::thread::sleep(std::time::Duration::from_secs(2));

    // P3: 100 calls alternating write_c_1/write_c_2
    for i in 0u64..100 {
        if i % 2 == 0 {
            conn.reducers.write_c_1(i).unwrap();
        } else {
            conn.reducers.write_c_2(i).unwrap();
        }
    }

    std::thread::sleep(std::time::Duration::from_secs(2));

    // P4: 100 calls alternating read_d_1/read_d_2
    for i in 0u32..100 {
        if i % 2 == 0 {
            conn.reducers.read_d_1().unwrap();
        } else {
            conn.reducers.read_d_2().unwrap();
        }
    }

    std::thread::sleep(std::time::Duration::from_secs(2));

    // P5: 20 calls indirect_touch
    for i in 0u32..20 {
        conn.reducers.indirect_touch(i % 2 == 0).unwrap();
    }

    // Wait for the interval to elapse so the report timer fires on the sentinel call.
    std::thread::sleep(std::time::Duration::from_secs(3));

    // Sentinel: read_c with callback — waits for its on_executed to fire the report write.
    conn.reducers
        .read_c_then(move |_ctx, result| {
            if let Some(set_result) = sentinel_result_clone.lock().unwrap().take() {
                match result {
                    Ok(Ok(())) => set_result(Ok(())),
                    Ok(Err(e)) => set_result(Err(anyhow::anyhow!("read_c failed: {e}"))),
                    Err(e) => set_result(Err(anyhow::anyhow!("read_c internal error: {e:?}"))),
                }
            }
        })
        .unwrap();

    test_counter.wait_for_all().await;

    // Allow the report write to complete before the process exits.
    std::thread::sleep(std::time::Duration::from_secs(1));

    conn.disconnect().unwrap();
}
