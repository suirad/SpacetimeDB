//! The analysis driver: ties imports + provenance + reachability together into
//! a per-reducer [`AccessSet`].

use spacetimedb_schema::def::ModuleDef;
use spacetimedb_schema::identifier::Identifier;
use walrus::{FunctionId, Module};

use crate::error::AnalysisError;
use crate::ids::{self, NameKind, Provenance};
use crate::imports::{self, HostImport, ImportTable, OpClass};
use crate::map::{BTreeSet, HashMap, HashSet};
use crate::reachability::{self, CallGraph, IndirectTargets};
use crate::reducers;

/// The set of tables a reducer reads and writes.
///
/// Tables are named (not id'd) because ids aren't assigned until publish.
/// `wildcard == true` means "conflicts with everything": no sound finite set
/// could be proven.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AccessSet {
    pub reads: BTreeSet<Identifier>,
    pub writes: BTreeSet<Identifier>,
    pub wildcard: bool,
}

impl AccessSet {
    fn wildcard() -> Self {
        AccessSet {
            wildcard: true,
            ..Default::default()
        }
    }
}

/// Analyze `wasm` against `module_def`, returning one [`AccessSet`] per reducer,
/// indexed by reducer id (== `ModuleDef` reducer order). Soundness is absolute:
/// every table a reducer touches is in its set, OR the reducer is `wildcard`.
pub fn analyze(wasm: &[u8], module_def: &ModuleDef) -> Result<Vec<AccessSet>, AnalysisError> {
    let module = Module::from_buffer(wasm).map_err(|e| AnalysisError::Parse(e.to_string()))?;

    let imports = imports::classify_imports(&module);
    let graph = CallGraph::build(&module);
    let provenance = ids::recover(&module, &imports, &graph);
    let indirect = IndirectTargets::build(&module);
    let table_relevant = reachability::table_relevant_funcs(&module, &imports, &graph);

    let index_to_table = build_index_to_table(module_def);
    let accessor_to_table = build_accessor_to_table(module_def);

    let reducer_names: Vec<&str> = module_def.reducers().map(|r| &*r.name).collect();
    let entries = reducers::locate(&module, &reducer_names);

    let ctx = AnalyzeCtx {
        module: &module,
        imports: &imports,
        graph: &graph,
        provenance: &provenance,
        indirect: &indirect,
        table_relevant: &table_relevant,
        index_to_table: &index_to_table,
        accessor_to_table: &accessor_to_table,
        module_def,
    };

    Ok(entries
        .iter()
        .map(|entry| match entry.body {
            // Body not found / ambiguous -> wildcard (sound).
            None => AccessSet::wildcard(),
            Some(body) => ctx.analyze_reducer(body),
        })
        .collect())
}

/// Shared, read-only context for analyzing each reducer.
struct AnalyzeCtx<'a> {
    module: &'a Module,
    imports: &'a ImportTable,
    graph: &'a CallGraph,
    provenance: &'a Provenance,
    indirect: &'a IndirectTargets,
    table_relevant: &'a HashSet<FunctionId>,
    index_to_table: &'a HashMap<String, Identifier>,
    accessor_to_table: &'a HashMap<String, Identifier>,
    module_def: &'a ModuleDef,
}

impl AnalyzeCtx<'_> {
    fn analyze_reducer(&self, body: FunctionId) -> AccessSet {
        let closure = self.graph.forward_closure([body]);

        let mut set = AccessSet::default();

        for &func in &closure {
            if self.has_dangerous_indirect(func) {
                return AccessSet::wildcard();
            }

            for callee in self.graph.callees_of(func) {
                match self.imports.get(callee) {
                    Some(HostImport::DangerousIfReachable) => return AccessSet::wildcard(),
                    None if self.imports.is_unknown(callee) => return AccessSet::wildcard(),
                    _ => {}
                }
            }

            // Skip functions that are themselves table-op proxies (thin wrappers
            // forwarding an id to the raw import): they never call an accessor,
            // so attributing here would spuriously wildcard. The real op-host is
            // the monomorphized helper that calls both the accessor and the proxy.
            if self.op_proxy_class(func).is_some() {
                continue;
            }
            if !self.attribute(func, &mut set) {
                // Op reached but no producing accessor resolved (or ambiguous):
                // cannot bound the table -> wildcard.
                return AccessSet::wildcard();
            }
        }

        set
    }

    /// Classify a callee as a table-op proxy: its forward closure reaches exactly
    /// one table-op class+id-kind and no resolver/unknown/dangerous import.
    fn op_proxy_class(&self, callee: FunctionId) -> Option<HostImport> {
        if let Some(host @ HostImport::TableOp { .. }) = self.imports.get(callee) {
            return Some(host);
        }
        // Some other recognized import (resolver/safe/dangerous) is not a proxy.
        if self.imports.get(callee).is_some() {
            return None;
        }
        let fwd = self.graph.forward_closure([callee]);
        let mut found: Option<HostImport> = None;
        for f in &fwd {
            match self.imports.get(*f) {
                Some(host @ HostImport::TableOp { class, id_kind }) => {
                    match found {
                        None => found = Some(host),
                        Some(HostImport::TableOp { class: c2, id_kind: k2 }) => {
                            if c2 != class || k2 != id_kind {
                                return None;
                            }
                        }
                        _ => return None,
                    }
                }
                Some(HostImport::TableResolver)
                | Some(HostImport::IndexResolver)
                | Some(HostImport::DangerousIfReachable) => return None,
                None if self.imports.is_unknown(*f) => return None,
                _ => {}
            }
        }
        found
    }

    /// Attribute the table ops `func` directly calls to the accessor names
    /// reachable from it. Returns `false` if an op was reached but its producing
    /// accessor couldn't be resolved (none, ambiguous, or unknown table/index) —
    /// caller must wildcard.
    fn attribute(&self, func: FunctionId, set: &mut AccessSet) -> bool {
        let mut read = false;
        let mut write = false;
        for callee in self.graph.callees_of(func) {
            if let Some(HostImport::TableOp { class, .. }) = self.op_proxy_class(callee) {
                match class {
                    OpClass::Read => read = true,
                    OpClass::Write => write = true,
                }
            }
        }
        if !read && !write {
            return true;
        }

        // No accessor (empty) or an ambiguous one (`None`): can't bound the op.
        let names = match self.accessors_reachable_from(func) {
            Some(names) if !names.is_empty() => names,
            _ => return false,
        };

        for resolved in names {
            let table = match resolved.kind {
                NameKind::Table => self.table_identifier(&resolved.name),
                NameKind::Index => self.index_parent_table(&resolved.name),
            };
            let Some(table) = table else {
                // Name not in the ModuleDef -> can't bound it -> wildcard.
                return false;
            };
            if read {
                set.reads.insert(table.clone());
            }
            if write {
                set.writes.insert(table);
            }
        }
        true
    }

    /// Accessor-resolved names reachable from `func`. `None` if any reachable
    /// accessor is ambiguous (can't be soundly bounded).
    fn accessors_reachable_from(&self, func: FunctionId) -> Option<Vec<ids::ResolvedName>> {
        let fwd = self.graph.forward_closure([func]);
        let mut out = Vec::new();
        for f in &fwd {
            if self.provenance.ambiguous.contains(f) {
                return None;
            }
            if let Some(resolved) = self.provenance.by_func.get(f) {
                out.push(resolved.clone());
            }
        }
        Some(out)
    }

    /// Resolve a table accessor name (recovered from a wasm const) to its
    /// `ModuleDef` table identifier: raw accessor map first (see
    /// `build_accessor_to_table` for why the names diverge), canonical name as
    /// fallback. A still-missing name → `None` → wildcard.
    fn table_identifier(&self, name: &str) -> Option<Identifier> {
        self.accessor_to_table
            .get(name)
            .cloned()
            .or_else(|| self.module_def.table(name).map(|t| t.name.clone()))
    }

    /// Map an index `source_name` (what `index_id_from_name` receives) to its
    /// parent table identifier.
    fn index_parent_table(&self, index_source_name: &str) -> Option<Identifier> {
        self.index_to_table.get(index_source_name).cloned()
    }

    /// Does `func` contain a `call_indirect` whose constrained possible targets
    /// (element-segment ∩ type-signature) include any table-relevant function?
    fn has_dangerous_indirect(&self, func: FunctionId) -> bool {
        use walrus::ir::Instr;
        let walrus::FunctionKind::Local(lf) = &self.module.funcs.get(func).kind else {
            return false;
        };
        let mut dangerous = false;
        reachability::for_each_instr(lf, |instr| {
            if dangerous {
                return;
            }
            if let Instr::CallIndirect(ci) = instr {
                for target in self.indirect.possible_for(self.module, ci.ty) {
                    if self.table_relevant.contains(&target) {
                        dangerous = true;
                        break;
                    }
                }
            }
        });
        dangerous
    }
}

/// Map each index's `source_name` (what `index_id_from_name` receives) to its
/// parent table identifier.
fn build_index_to_table(module_def: &ModuleDef) -> HashMap<String, Identifier> {
    let mut map = HashMap::new();
    for table in module_def.tables() {
        for index in table.indexes.values() {
            map.insert(index.source_name.to_string(), table.name.clone());
            // Also map `name` defensively (for V9 modules `name == source_name`).
            map.insert(index.name.to_string(), table.name.clone());
        }
    }
    map
}

/// Map each table's raw accessor name (what `table_id_from_name` receives) to its
/// canonical `ModuleDef` table identifier.
///
/// WHY: `TableDef.accessor_name` preserves the raw source identifier while
/// `TableDef.name` is snake-cased at letter-digit boundaries during validation;
/// the wasm const carries the raw form, so accessor-keyed attribution must look
/// up the raw name, not the canonical one.
fn build_accessor_to_table(module_def: &ModuleDef) -> HashMap<String, Identifier> {
    let mut map = HashMap::new();
    for table in module_def.tables() {
        map.insert(table.accessor_name.to_string(), table.name.clone());
    }
    map
}
