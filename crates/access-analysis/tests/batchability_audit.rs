#![allow(clippy::disallowed_macros)]
//! Diagnostic test: prints per-module reducer-batchability matrices.
//!
//! Run with:
//!   cargo test -p spacetimedb-access-analysis --test batchability_audit \
//!     -- --ignored --nocapture

use std::ops::Deref;

use spacetimedb_access_analysis::{analyze, AccessSet, ConflictMatrix};
use spacetimedb_schema::def::ModuleDef;
use spacetimedb_testing::modules::{CompilationMode, CompiledModule};

fn analyze_module(name: &str, debug: bool) -> (Vec<String>, Vec<AccessSet>) {
    let mode = if debug {
        CompilationMode::Debug
    } else {
        CompilationMode::Release
    };
    let compiled = CompiledModule::compile(name, mode);
    let wasm = compiled.program_bytes();
    let module_def: ModuleDef = compiled.extract_schema_blocking();
    let sets = analyze(&wasm, &module_def).expect("analysis must not fail — harness is broken");

    let names: Vec<String> = module_def
        .reducer_ids_and_defs()
        .map(|(_, def)| def.name.to_string())
        .collect();

    (names, sets)
}

fn mode_label(debug: bool) -> &'static str {
    if debug {
        "Debug"
    } else {
        "Release"
    }
}

fn print_access_set(name: &str, set: &AccessSet) {
    let reads: Vec<&str> = set.reads.iter().map(|i| i.deref()).collect();
    let writes: Vec<&str> = set.writes.iter().map(|i| i.deref()).collect();
    println!(
        "  {:40} wildcard={} reads={:?} writes={:?}",
        name, set.wildcard, reads, writes
    );
}

fn audit_module(name: &str, debug: bool) {
    println!("\n=== {} / {} ===", name, mode_label(debug));

    let (names, sets) = analyze_module(name, debug);
    let n = names.len();

    println!("\n-- Reducer access sets ({} total) --", n);
    for (reducer_name, set) in names.iter().zip(sets.iter()) {
        print_access_set(reducer_name, set);
    }

    let wildcard_count = sets.iter().filter(|s| s.wildcard).count();
    println!("\nWildcard reducers: {}", wildcard_count);

    // Build the conflict matrix using the library's implementation, which uses
    // the same predicate: conflicts = wildcard || writes∩reads || writes∩writes.
    let matrix = ConflictMatrix::build(&sets);

    println!("\n-- Conflict matrix (. = admissible pair, X = conflict) --");
    println!("Index legend:");
    for (i, reducer_name) in names.iter().enumerate() {
        println!("  {:3} {}", i, reducer_name);
    }
    println!();

    // Print column index header in blocks to stay readable for N up to ~65.
    // Tens digit row, then units digit row.
    print!("     ");
    for j in 0..n {
        print!("{}", (j / 10) % 10);
    }
    println!();
    print!("     ");
    for j in 0..n {
        print!("{}", j % 10);
    }
    println!();

    for i in 0..n {
        print!("{:3}: ", i);
        for j in 0..n {
            // Upper-triangle only visually; matrix is symmetric so either half suffices.
            if j < i {
                print!(" ");
            } else if matrix.conflicts(i, j) {
                print!("X");
            } else {
                print!(".");
            }
        }
        println!();
    }

    // Count upper-triangle pairs (i <= j) — self-pairs included as specified.
    let total_pairs = n * (n + 1) / 2;
    let mut admissible_count = 0;
    let mut admissible_pairs: Vec<(String, String)> = Vec::new();

    for i in 0..n {
        for j in i..n {
            if !matrix.conflicts(i, j) {
                admissible_count += 1;
                admissible_pairs.push((names[i].clone(), names[j].clone()));
            }
        }
    }

    let density = if total_pairs == 0 {
        0.0
    } else {
        admissible_count as f64 / total_pairs as f64 * 100.0
    };

    println!("\n-- Summary --");
    println!(
        "Total ordered-unordered pairs (i<=j, self-pairs included): {}",
        total_pairs
    );
    println!("Admissible (non-conflicting): {}", admissible_count);
    println!("Density: {:.1}%", density);
    println!("\nAdmissible pairs:");
    for (a, b) in &admissible_pairs {
        println!("  {} , {}", a, b);
    }
}

#[test]
#[ignore]
fn batchability_audit() {
    let modules = ["benchmarks", "keynote-benchmarks", "perf-test"];

    for name in &modules {
        for debug in [true, false] {
            audit_module(name, debug);
        }
    }

    println!("\n=== Caveats ===");
    // Static analysis cannot see live subscription read sets; runtime admission
    // additionally excludes writers whose write-set overlaps the subscribed-view
    // read sets of connected clients, which depends on live subscriptions.
    println!(
        "(1) Static view only: runtime admission additionally excludes writers overlapping \
        subscribed-view read sets, which depends on live subscriptions."
    );
    // Heaviness threshold is a dynamic calibration; not everything admissible here
    // will actually be batched in production.
    println!(
        "(2) Admissible pairs only become real batches if members exceed the calibrated \
        heaviness threshold — that is measured dynamically, not here."
    );
}
