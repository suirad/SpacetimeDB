//! Classification of SpacetimeDB host imports into [`HostImport`].
//!
//! Modules reach the datastore only through the fixed `spacetime_10.x` ABI.
//! Soundness rule (absolute): any *unrecognized* `spacetime_*` import a reducer
//! can reach wildcards it — an unknown future ABI op must never be treated as safe.

use walrus::{FunctionId, FunctionKind, Module};

use crate::map::HashMap;

/// Whether a table op reads or writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpClass {
    Read,
    Write,
}

/// Whether a table op's id argument is a `TableId` or an `IndexId`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdKind {
    Table,
    Index,
}

/// Classification of a single imported host function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HostImport {
    TableOp {
        class: OpClass,
        id_kind: IdKind,
    },
    TableResolver,
    IndexResolver,
    /// A recognized host op that does not, on its own, touch any table.
    SafeIdFree,
    /// A recognized `spacetime_*` import that wildcards the reducer if reachable
    /// (e.g. `volatile_nonatomic_schedule_immediate` can invoke arbitrary other
    /// reducers; the procedure-only ops manipulate transactions directly).
    DangerousIfReachable,
}

/// The result of classifying a module's imports.
#[derive(Debug, Default)]
pub struct ImportTable {
    pub recognized: HashMap<FunctionId, HostImport>,
    /// `spacetime_*` imports we do NOT recognize; reaching any wildcards the reducer.
    pub unknown_spacetime: Vec<FunctionId>,
}

impl ImportTable {
    pub fn get(&self, func: FunctionId) -> Option<HostImport> {
        self.recognized.get(&func).copied()
    }

    pub fn is_unknown(&self, func: FunctionId) -> bool {
        self.unknown_spacetime.contains(&func)
    }

    /// Imports that make a reachable function "table-relevant": a table op,
    /// resolver, dangerous op, or an unrecognized `spacetime_*` import.
    pub fn is_table_relevant_import(&self, func: FunctionId) -> bool {
        match self.get(func) {
            Some(HostImport::TableOp { .. })
            | Some(HostImport::TableResolver)
            | Some(HostImport::IndexResolver)
            | Some(HostImport::DangerousIfReachable) => true,
            Some(HostImport::SafeIdFree) => false,
            None => self.is_unknown(func),
        }
    }
}

/// Classify a `spacetime_*` import; `None` means unrecognized (→ wildcard).
fn classify_name(name: &str) -> Option<HostImport> {
    use IdKind::*;
    use OpClass::*;
    Some(match name {
        "datastore_insert_bsatn" => HostImport::TableOp {
            class: Write,
            id_kind: Table,
        },
        // arg0 is the table_id (arg1 is an index_id); key on the table it writes.
        "datastore_update_bsatn" => HostImport::TableOp {
            class: Write,
            id_kind: Table,
        },
        "datastore_delete_all_by_eq_bsatn" => HostImport::TableOp {
            class: Write,
            id_kind: Table,
        },
        "datastore_clear" => HostImport::TableOp {
            class: Write,
            id_kind: Table,
        },
        "datastore_table_scan_bsatn" => HostImport::TableOp {
            class: Read,
            id_kind: Table,
        },
        "datastore_table_row_count" => HostImport::TableOp {
            class: Read,
            id_kind: Table,
        },

        "datastore_index_scan_point_bsatn" => HostImport::TableOp {
            class: Read,
            id_kind: Index,
        },
        "datastore_index_scan_range_bsatn" => HostImport::TableOp {
            class: Read,
            id_kind: Index,
        },
        // Deprecated alias of the range scan.
        "datastore_btree_scan_bsatn" => HostImport::TableOp {
            class: Read,
            id_kind: Index,
        },
        "datastore_delete_by_index_scan_point_bsatn" => HostImport::TableOp {
            class: Write,
            id_kind: Index,
        },
        "datastore_delete_by_index_scan_range_bsatn" => HostImport::TableOp {
            class: Write,
            id_kind: Index,
        },
        // Deprecated alias of the range delete.
        "datastore_delete_by_btree_scan_bsatn" => HostImport::TableOp {
            class: Write,
            id_kind: Index,
        },

        "table_id_from_name" => HostImport::TableResolver,
        "index_id_from_name" => HostImport::IndexResolver,

        "row_iter_bsatn_advance"
        | "row_iter_bsatn_close"
        | "bytes_sink_write"
        | "bytes_source_read"
        | "bytes_source_remaining_length"
        | "console_log"
        | "console_timer_start"
        | "console_timer_end"
        | "identity"
        | "get_jwt" => HostImport::SafeIdFree,

        "volatile_nonatomic_schedule_immediate"
        | "procedure_sleep_until"
        | "procedure_start_mut_tx"
        | "procedure_commit_mut_tx"
        | "procedure_abort_mut_tx"
        | "procedure_http_request" => HostImport::DangerousIfReachable,

        _ => return None,
    })
}

/// Build the [`ImportTable`] by scanning every imported function.
pub fn classify_imports(module: &Module) -> ImportTable {
    let mut table = ImportTable::default();
    for func in module.funcs.iter() {
        let FunctionKind::Import(import_fn) = &func.kind else {
            continue;
        };
        let import = &module.imports.get(import_fn.import);
        if !import.module.starts_with("spacetime") {
            // Only the `spacetime_10.x` ABI touches the datastore; foreign
            // imports (wasm-bindgen/libc) cannot, so ignore them.
            continue;
        }
        match classify_name(&import.name) {
            Some(host) => {
                table.recognized.insert(func.id(), host);
            }
            None => {
                table.unknown_spacetime.push(func.id());
            }
        }
    }
    table
}

#[cfg(test)]
mod tests {
    use super::*;

    const WAT: &str = r#"
        (module
          (import "spacetime_10.0" "datastore_insert_bsatn" (func $ins (param i32 i32 i32) (result i32)))
          (import "spacetime_10.4" "datastore_index_scan_point_bsatn" (func $iscan (param i32 i32 i32 i32) (result i32)))
          (import "spacetime_10.0" "table_id_from_name" (func $tres (param i32 i32 i32) (result i32)))
          (import "spacetime_10.0" "index_id_from_name" (func $ires (param i32 i32 i32) (result i32)))
          (import "spacetime_10.0" "console_log" (func $log (param i32 i32 i32 i32 i32 i32 i32 i32)))
          (import "spacetime_10.0" "volatile_nonatomic_schedule_immediate" (func $sched (param i32 i32 i32 i32)))
          (import "spacetime_10.9" "some_future_op" (func $future (param i32) (result i32)))
          (import "env" "memcpy" (func $foreign (param i32 i32 i32) (result i32)))
          (func (export "noop"))
        )
    "#;

    fn classify_kind(name: &str, table: &ImportTable, module: &Module) -> Option<HostImport> {
        for func in module.funcs.iter() {
            if let FunctionKind::Import(im) = &func.kind
                && module.imports.get(im.import).name == name
            {
                return table.get(func.id());
            }
        }
        None
    }

    #[test]
    fn classifies_all_host_op_kinds() {
        let wasm = wat::parse_str(WAT).unwrap();
        let module = Module::from_buffer(&wasm).unwrap();
        let table = classify_imports(&module);

        assert!(matches!(
            classify_kind("datastore_insert_bsatn", &table, &module),
            Some(HostImport::TableOp {
                class: OpClass::Write,
                id_kind: IdKind::Table
            })
        ));
        assert!(matches!(
            classify_kind("datastore_index_scan_point_bsatn", &table, &module),
            Some(HostImport::TableOp {
                class: OpClass::Read,
                id_kind: IdKind::Index
            })
        ));
        assert!(matches!(
            classify_kind("table_id_from_name", &table, &module),
            Some(HostImport::TableResolver)
        ));
        assert!(matches!(
            classify_kind("index_id_from_name", &table, &module),
            Some(HostImport::IndexResolver)
        ));
        assert!(matches!(
            classify_kind("console_log", &table, &module),
            Some(HostImport::SafeIdFree)
        ));
        assert!(matches!(
            classify_kind("volatile_nonatomic_schedule_immediate", &table, &module),
            Some(HostImport::DangerousIfReachable)
        ));
    }

    #[test]
    fn unknown_spacetime_import_is_tracked_and_table_relevant() {
        let wasm = wat::parse_str(WAT).unwrap();
        let module = Module::from_buffer(&wasm).unwrap();
        let table = classify_imports(&module);

        let future = module
            .funcs
            .iter()
            .find(|f| match &f.kind {
                FunctionKind::Import(im) => module.imports.get(im.import).name == "some_future_op",
                _ => false,
            })
            .unwrap()
            .id();
        assert!(table.is_unknown(future));
        assert!(table.is_table_relevant_import(future));
        assert!(table.get(future).is_none());
    }

    #[test]
    fn foreign_imports_are_ignored() {
        let wasm = wat::parse_str(WAT).unwrap();
        let module = Module::from_buffer(&wasm).unwrap();
        let table = classify_imports(&module);
        let foreign = module
            .funcs
            .iter()
            .find(|f| match &f.kind {
                FunctionKind::Import(im) => module.imports.get(im.import).name == "memcpy",
                _ => false,
            })
            .unwrap()
            .id();
        assert!(table.get(foreign).is_none());
        assert!(!table.is_unknown(foreign));
    }
}
