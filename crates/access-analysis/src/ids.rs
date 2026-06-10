//! Name + provenance recovery for table/index ids.
//!
//! Ids are never compile-time constants: each table/index has an accessor that
//! lazily calls `table_id_from_name`/`index_id_from_name` with a constant name.
//! In wasm this is a `const ptr; const len; call resolver-reaching-fn` sequence
//! (possibly through one thin wrapper). We read the UTF-8 name from the data
//! segment at the const pointer and bind it to that producing accessor function
//! (the monomorphized `T::table_id()` / `Idx::index_id()`), which is what the
//! rest of the analysis keys on.

use walrus::ir::{Instr, Value};
use walrus::{FunctionId, LocalFunction, Module};

use crate::imports::{HostImport, ImportTable};
use crate::map::{HashMap, HashSet};
use crate::reachability::CallGraph;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NameKind {
    Table,
    Index,
}

/// A name recovered from a resolver call site.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedName {
    pub name: String,
    pub kind: NameKind,
}

/// Maps each accessor function id to the single name it resolves. An accessor
/// resolving conflicting names is recorded `ambiguous` and treated as
/// unresolved (→ wildcard), preserving soundness.
#[derive(Debug, Default)]
pub struct Provenance {
    pub by_func: HashMap<FunctionId, ResolvedName>,
    pub ambiguous: HashSet<FunctionId>,
}

/// A flat view of active data segments, supporting address → bytes lookups.
struct DataMemory {
    /// `(start_addr, bytes)` per active segment, sorted by start.
    segments: Vec<(u64, Vec<u8>)>,
}

impl DataMemory {
    fn build(module: &Module) -> Self {
        let mut segments = Vec::new();
        for data in module.data.iter() {
            // Skip segments with a non-`i32.const` (e.g. global-relative) offset:
            // we can't statically resolve addresses into them, so names landing
            // there stay unrecoverable → wildcard.
            if let walrus::DataKind::Active { offset, .. } = &data.kind
                && let walrus::ConstExpr::Value(Value::I32(off)) = offset
            {
                segments.push((*off as u32 as u64, data.value.clone()));
            }
        }
        segments.sort_by_key(|(start, _)| *start);
        DataMemory { segments }
    }

    /// Read `len` bytes at `addr`, if fully contained in one active segment.
    fn read(&self, addr: u64, len: u64) -> Option<&[u8]> {
        for (start, bytes) in &self.segments {
            let start = *start;
            let end = start + bytes.len() as u64;
            if addr >= start && addr.checked_add(len)? <= end {
                let lo = (addr - start) as usize;
                let hi = lo + len as usize;
                return Some(&bytes[lo..hi]);
            }
        }
        None
    }
}

/// Recover the name → accessor-function provenance for the whole module.
pub fn recover(module: &Module, imports: &ImportTable, graph: &CallGraph) -> Provenance {
    let data = DataMemory::build(module);

    // A callee reaching exactly one resolver kind tells us how to interpret the
    // const string (table vs index name).
    let reaches = ResolverReach::compute(module, imports, graph);

    let mut prov = Provenance::default();
    for (fid, lf) in module.funcs.iter_local() {
        recover_in_func(fid, lf, &data, &reaches, &mut prov);
    }
    prov
}

/// Per-function flags: can this function reach the table / index resolver via
/// direct calls (including being the resolver itself)?
struct ResolverReach {
    table: HashSet<FunctionId>,
    index: HashSet<FunctionId>,
}

impl ResolverReach {
    fn compute(module: &Module, imports: &ImportTable, graph: &CallGraph) -> Self {
        let mut table_seed = HashSet::new();
        let mut index_seed = HashSet::new();
        for func in module.funcs.iter() {
            match imports.get(func.id()) {
                Some(HostImport::TableResolver) => {
                    table_seed.insert(func.id());
                }
                Some(HostImport::IndexResolver) => {
                    index_seed.insert(func.id());
                }
                _ => {}
            }
        }
        ResolverReach {
            table: graph.callers_closure(&table_seed),
            index: graph.callers_closure(&index_seed),
        }
    }

    /// The resolver kind `callee` reaches, or `None` if it reaches both or neither.
    fn kind_of(&self, callee: FunctionId) -> Option<NameKind> {
        match (self.table.contains(&callee), self.index.contains(&callee)) {
            (true, false) => Some(NameKind::Table),
            (false, true) => Some(NameKind::Index),
            _ => None,
        }
    }
}

/// Scan a single function for `const ptr; const len; call resolver-reaching-fn`.
fn recover_in_func(
    fid: FunctionId,
    lf: &LocalFunction,
    data: &DataMemory,
    reaches: &ResolverReach,
    prov: &mut Provenance,
) {
    let mut seqs = vec![lf.entry_block()];
    let mut visited = HashSet::new();
    while let Some(seq_id) = seqs.pop() {
        if !visited.insert(seq_id) {
            continue;
        }
        let seq = lf.block(seq_id);
        // Track ALL i32.consts since the last call, never resetting on non-const
        // instrs: release codegen interleaves the out-ptr computation
        // (`local.get; i32.const; i32.add`) between the name constants and the
        // call, and a fixed cap risked evicting the real name-pair under heavy
        // inlining. `find_name_pair` validates candidates against the data
        // segments and refuses conflicts, so surviving extra consts stay sound.
        let mut last_consts: Vec<i32> = Vec::new();
        for (instr, _) in &seq.instrs {
            match instr {
                Instr::Const(c) => {
                    if let Value::I32(v) = c.value {
                        last_consts.push(v);
                    }
                }
                Instr::Call(call) => {
                    if let Some(kind) = reaches.kind_of(call.func) {
                        if let Some(name) = find_name_pair(data, &last_consts) {
                            bind(prov, fid, ResolvedName { name, kind });
                        }
                    }
                    last_consts.clear();
                    push_nested(instr, &mut seqs);
                }
                other => {
                    push_nested(other, &mut seqs);
                }
            }
        }
    }
}

fn push_nested(instr: &Instr, out: &mut Vec<walrus::ir::InstrSeqId>) {
    match instr {
        Instr::Block(b) => out.push(b.seq),
        Instr::Loop(l) => out.push(l.seq),
        Instr::IfElse(ie) => {
            out.push(ie.consequent);
            out.push(ie.alternative);
        }
        _ => {}
    }
}

/// Find the resolver's `(name_ptr, name_len)` argument among recent `i32.const`s.
///
/// Scans consecutive pairs from the last backwards (args are nearest the call).
/// If two pairs read as *different* valid names we return `None` rather than
/// guess (→ caller wildcards), preserving soundness.
fn find_name_pair(data: &DataMemory, consts: &[i32]) -> Option<String> {
    if consts.len() < 2 {
        return None;
    }
    let mut found: Option<String> = None;
    for i in (0..consts.len() - 1).rev() {
        if let Some(name) = read_name(data, consts[i], consts[i + 1]) {
            match &found {
                None => found = Some(name),
                Some(prev) if *prev == name => {}
                Some(_) => return None,
            }
        }
    }
    found
}

fn read_name(data: &DataMemory, ptr: i32, len: i32) -> Option<String> {
    if ptr < 0 || len <= 0 || len > 4096 {
        return None;
    }
    let bytes = data.read(ptr as u32 as u64, len as u64)?;
    std::str::from_utf8(bytes).ok().map(|s| s.to_owned())
}

/// Bind a recovered name to its accessor function, detecting ambiguity.
fn bind(prov: &mut Provenance, fid: FunctionId, resolved: ResolvedName) {
    if prov.ambiguous.contains(&fid) {
        return;
    }
    match prov.by_func.get(&fid) {
        Some(existing) if *existing == resolved => {}
        Some(_) => {
            prov.by_func.remove(&fid);
            prov.ambiguous.insert(fid);
        }
        None => {
            prov.by_func.insert(fid, resolved);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::imports::classify_imports;
    use crate::reachability::CallGraph;
    use walrus::FunctionKind;

    const WAT: &str = r#"
        (module
          (import "spacetime_10.0" "table_id_from_name"
            (func $tres (param i32 i32 i32) (result i32)))
          (import "spacetime_10.0" "index_id_from_name"
            (func $ires (param i32 i32 i32) (result i32)))
          (memory 1)
          (data (i32.const 16) "widget")
          (data (i32.const 32) "widget_x_idx_btree")
          ;; thin wrapper that forwards to the table resolver
          (func $twrap (param i32 i32) (result i32)
            local.get 0
            local.get 1
            i32.const 0
            call $tres)
          ;; table accessor: const ptr/len then call the wrapper
          (func $table_accessor (result i32)
            i32.const 16
            i32.const 6
            call $twrap)
          ;; index accessor: const ptr/len then call the resolver directly
          (func $index_accessor (result i32)
            i32.const 32
            i32.const 18
            i32.const 0
            call $ires)
          (export "ta" (func $table_accessor))
          (export "ia" (func $index_accessor))
        )
    "#;

    fn func_id_by_name(module: &Module, export: &str) -> FunctionId {
        for e in module.exports.iter() {
            if e.name == export
                && let walrus::ExportItem::Function(f) = e.item
            {
                return f;
            }
        }
        panic!("no export {export}");
    }

    #[test]
    fn recovers_table_and_index_names_from_const_strings() {
        let wasm = wat::parse_str(WAT).unwrap();
        let module = Module::from_buffer(&wasm).unwrap();
        let imports = classify_imports(&module);
        let graph = CallGraph::build(&module);
        let prov = recover(&module, &imports, &graph);

        let ta = func_id_by_name(&module, "ta");
        let ia = func_id_by_name(&module, "ia");

        assert_eq!(
            prov.by_func.get(&ta),
            Some(&ResolvedName { name: "widget".into(), kind: NameKind::Table }),
            "table accessor should resolve to 'widget' through the thin wrapper",
        );
        assert_eq!(
            prov.by_func.get(&ia),
            Some(&ResolvedName {
                name: "widget_x_idx_btree".into(),
                kind: NameKind::Index
            }),
        );
        assert!(prov.ambiguous.is_empty());
    }

    #[test]
    fn ignores_local_functions_without_const_string_pairs() {
        let wat = r#"
            (module
              (import "spacetime_10.0" "table_id_from_name"
                (func $tres (param i32 i32 i32) (result i32)))
              (memory 1)
              (func $dyn (param i32) (result i32)
                local.get 0      ;; non-const ptr
                i32.const 6
                i32.const 0
                call $tres))
        "#;
        let wasm = wat::parse_str(wat).unwrap();
        let module = Module::from_buffer(&wasm).unwrap();
        let imports = classify_imports(&module);
        let graph = CallGraph::build(&module);
        let prov = recover(&module, &imports, &graph);
        let local_count = module
            .funcs
            .iter()
            .filter(|f| matches!(f.kind, FunctionKind::Local(_)))
            .count();
        assert!(local_count >= 1);
        assert!(prov.by_func.is_empty());
    }
}
