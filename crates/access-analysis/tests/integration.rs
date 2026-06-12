#![allow(clippy::disallowed_macros)]
//! Integration tests: analyze real compiled modules and assert the per-reducer
//! access sets, with zero under-approximation as the bar.

use std::collections::{BTreeSet, HashMap};

use spacetimedb_access_analysis::{analyze, AccessSet};
use spacetimedb_schema::def::ModuleDef;
use spacetimedb_testing::modules::{CompilationMode, CompiledModule};

fn analyze_named(name: &str, mode: CompilationMode) -> HashMap<String, AccessSet> {
    let compiled = CompiledModule::compile(name, mode);
    let wasm = compiled.program_bytes();
    let module_def: ModuleDef = compiled.extract_schema_blocking();
    let sets = analyze(&wasm, &module_def).expect("analysis should not fail");

    let mut out = HashMap::new();
    for ((_, def), set) in module_def.reducer_ids_and_defs().zip(sets) {
        out.insert(def.name.to_string(), set);
    }
    out
}

fn names(set: &BTreeSet<spacetimedb_schema::identifier::Identifier>) -> Vec<String> {
    set.iter().map(|i| i.to_string()).collect()
}

fn assert_reads_writes(set: &AccessSet, reads: &[&str], writes: &[&str]) {
    assert!(!set.wildcard, "expected not wildcard, got {set:?}");
    let want_reads: Vec<String> = reads.iter().map(|s| s.to_string()).collect();
    let want_writes: Vec<String> = writes.iter().map(|s| s.to_string()).collect();
    assert_eq!(names(&set.reads), want_reads, "reads mismatch: {set:?}");
    assert_eq!(names(&set.writes), want_writes, "writes mismatch: {set:?}");
}

fn run_perf_test(mode: CompilationMode) {
    let sets = analyze_named("perf-test", mode);

    assert_reads_writes(&sets["load_location_table"], &[], &["location"]);

    for r in [
        "test_index_scan_on_id",
        "test_index_scan_on_chunk",
        "test_index_scan_on_x_z_dimension",
        "test_index_scan_on_x_z",
    ] {
        assert_reads_writes(&sets[r], &["location"], &[]);
    }
}

#[test]
fn perf_test_debug() {
    run_perf_test(CompilationMode::Debug);
}

#[test]
fn perf_test_release() {
    run_perf_test(CompilationMode::Release);
}

/// Assert `set` soundly over-approximates: every actual read/write is present,
/// `wildcard` is false, and no table outside `universe` appears.
fn assert_sound_superset(set: &AccessSet, reads: &[&str], writes: &[&str], universe: &[&str]) {
    assert!(!set.wildcard, "expected not wildcard, got {set:?}");
    for r in reads {
        assert!(
            set.reads.iter().any(|i| i.to_string() == *r),
            "missing read `{r}` (under-approximation!) in {set:?}"
        );
    }
    for w in writes {
        assert!(
            set.writes.iter().any(|i| i.to_string() == *w),
            "missing write `{w}` (under-approximation!) in {set:?}"
        );
    }
    for name in names(&set.reads).iter().chain(names(&set.writes).iter()) {
        assert!(
            universe.contains(&name.as_str()),
            "unexpected table `{name}` outside universe {universe:?} in {set:?}"
        );
    }
}

fn run_fixture(mode: CompilationMode) {
    let exact = mode == CompilationMode::Debug;
    let sets = analyze_named("access-analysis-fixture", mode);

    if exact {
        // Debug: helpers aren't inlined, so the split is exact and minimal.
        assert_reads_writes(&sets["writes_a"], &[], &["a"]);
        assert_reads_writes(&sets["reads_b"], &["b"], &[]);
        assert_reads_writes(&sets["reads_a_writes_b"], &["a"], &["b"]);
        assert_reads_writes(&sets["touches_both"], &["a", "b"], &["a", "b"]);
    } else {
        // Release: inlining into the invoke wrapper can over-approximate (sound),
        // so we only require zero under-approximation within each universe.
        assert_sound_superset(&sets["writes_a"], &[], &["a"], &["a"]);
        assert_sound_superset(&sets["reads_b"], &["b"], &[], &["b"]);
        assert_sound_superset(&sets["reads_a_writes_b"], &["a"], &["b"], &["a", "b"]);
        assert_sound_superset(&sets["touches_both"], &["a", "b"], &["a", "b"], &["a", "b"]);
    }

    // The crux (both modes): writes_a.writes must be disjoint from reads_b.reads,
    // i.e. these two reducers are non-conflicting / batchable.
    let wa_writes: &BTreeSet<_> = &sets["writes_a"].writes;
    let rb_reads: &BTreeSet<_> = &sets["reads_b"].reads;
    assert!(
        wa_writes.is_disjoint(rb_reads),
        "writes_a.writes {wa_writes:?} must be disjoint from reads_b.reads {rb_reads:?}"
    );
    // Both non-empty, so the disjointness isn't vacuous.
    assert!(!wa_writes.is_empty() && !rb_reads.is_empty());
}

#[test]
fn fixture_debug() {
    run_fixture(CompilationMode::Debug);
}

#[test]
fn fixture_release() {
    run_fixture(CompilationMode::Release);
}

/// Validate the analyzer against the wasm-opt-optimized shape of perf-test —
/// the binary that actually ships — since wasm-opt reshapes significantly
/// (inlining, DCE, global rewriting). Skipped gracefully if wasm-opt is absent.
#[test]
fn perf_test_release_wasm_opt() {
    let wasm_opt_bin = {
        let path_var = std::env::var("PATH").unwrap_or_default();
        // Add the known install locations so resolution works even if the
        // calling process didn't inherit them.
        let local_bin = format!("{}/.local/bin", std::env::var("HOME").unwrap_or_default());
        let cargo_bin = format!("{}/.cargo/bin", std::env::var("HOME").unwrap_or_default());
        let extended = format!("{local_bin}:{cargo_bin}:{path_var}");
        // SAFETY: this suite requires --test-threads=1, so no other thread is
        // reading the environment concurrently.
        unsafe { std::env::set_var("PATH", &extended) };

        which_wasm_opt()
    };

    let Some(wasm_opt_bin) = wasm_opt_bin else {
        println!("SKIP perf_test_release_wasm_opt: wasm-opt not found on PATH");
        return;
    };

    let compiled = CompiledModule::compile("perf-test", CompilationMode::Release);
    let release_bytes: Vec<u8> = compiled.program_bytes().to_vec();
    // ModuleDef is identical pre/post wasm-opt: the schema lives in a custom
    // section that optimization passes don't touch.
    let module_def: ModuleDef = compiled.extract_schema_blocking();

    let tmp_dir = std::env::temp_dir();
    let input_path = tmp_dir.join("perf_test_release_input.wasm");
    let output_path = tmp_dir.join("perf_test_release_opt.wasm");

    std::fs::write(&input_path, &release_bytes)
        .expect("failed to write release wasm to temp file");

    let status = std::process::Command::new(&wasm_opt_bin)
        .args(["-all", "-g", "-O2"])
        .arg(&input_path)
        .arg("-o")
        .arg(&output_path)
        .status()
        .unwrap_or_else(|e| panic!("failed to spawn wasm-opt ({wasm_opt_bin:?}): {e}"));

    assert!(
        status.success(),
        "wasm-opt exited with {status}; input was {}",
        input_path.display()
    );

    let opt_bytes = std::fs::read(&output_path).expect("failed to read optimized wasm");

    println!(
        "perf_test_release_wasm_opt: release bytes = {}, opt bytes = {} (wasm-opt ran)",
        release_bytes.len(),
        opt_bytes.len()
    );

    let sets_vec = spacetimedb_access_analysis::analyze(&opt_bytes, &module_def)
        .expect("analysis of optimized wasm should not fail");

    let mut sets = HashMap::new();
    for ((_, def), set) in module_def.reducer_ids_and_defs().zip(sets_vec) {
        sets.insert(def.name.to_string(), set);
    }

    // Dump per-reducer sets to help diagnose wasm-opt-induced over-approximation
    // if this test ever needs updating.
    for (name, set) in &sets {
        println!(
            "  [{name}]: wildcard={}, reads={:?}, writes={:?}",
            set.wildcard,
            names(&set.reads),
            names(&set.writes)
        );
    }

    // Soundness on the optimized binary: aggressive inlining may over-approximate
    // (sound); under-approximation (a required table missing while not wildcard)
    // must never happen. `assert_sound_superset` enforces this within a universe.
    assert_sound_superset(
        &sets["load_location_table"],
        &[],
        &["location"],
        &["location"],
    );

    for r in [
        "test_index_scan_on_id",
        "test_index_scan_on_chunk",
        "test_index_scan_on_x_z_dimension",
        "test_index_scan_on_x_z",
    ] {
        assert_sound_superset(&sets[r], &["location"], &[], &["location"]);
    }

    // For reducers without exact ground-truth (e.g. lifecycle hooks) we only
    // guard the key invariant: a known table-touching reducer is never
    // non-wildcard with an empty set.
    for (name, set) in &sets {
        assert!(
            !(name == "load_location_table" && !set.wildcard && set.writes.is_empty()),
            "load_location_table must not have an empty non-wildcard write set: {set:?}"
        );
    }

    let _ = std::fs::remove_file(&input_path);
    let _ = std::fs::remove_file(&output_path);
}

fn which_wasm_opt() -> Option<std::path::PathBuf> {
    let path_var = std::env::var("PATH").ok()?;
    for dir in path_var.split(':') {
        let candidate = std::path::PathBuf::from(dir).join("wasm-opt");
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}
