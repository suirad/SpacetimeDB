//! Eyeball tool: compile a module by name and print its per-reducer access sets.
//!
//! `cargo run -p spacetimedb-access-analysis --example dump -- <module-name> [--release]`
#![allow(clippy::disallowed_macros)] // a stdout dump tool is allowed to print

use spacetimedb_access_analysis::analyze;
use spacetimedb_testing::modules::{CompilationMode, CompiledModule};

fn main() {
    let mut args = std::env::args().skip(1);
    let name = args.next().unwrap_or_else(|| {
        eprintln!("usage: dump <module-name> [--release]");
        std::process::exit(2);
    });
    let mode = if args.any(|a| a == "--release") {
        CompilationMode::Release
    } else {
        CompilationMode::Debug
    };

    eprintln!("compiling module `{name}` ({mode:?}) ...");
    let compiled = CompiledModule::compile(&name, mode);
    let wasm = compiled.program_bytes();
    let module_def = compiled.extract_schema_blocking();

    let sets = analyze(&wasm, &module_def).expect("analysis failed");

    println!("== access sets for `{name}` ==");
    for ((id, def), set) in module_def.reducer_ids_and_defs().zip(&sets) {
        let reads: Vec<&str> = set.reads.iter().map(|i| &**i).collect();
        let writes: Vec<&str> = set.writes.iter().map(|i| &**i).collect();
        println!(
            "  [{:>2}] {:<32} reads={:?} writes={:?} wildcard={}",
            id.0,
            &*def.name,
            reads,
            writes,
            set.wildcard,
        );
    }
}
