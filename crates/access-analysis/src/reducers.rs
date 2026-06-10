//! Locating each reducer's entry body in the wasm.
//!
//! We don't analyze through `__call_reducer__`: it dispatches by index via
//! `call_indirect`. Instead we find each reducer's body directly using the
//! name section, which survives both Debug and Release (`wasm-opt -g` keeps it).
//! A reducer `foo` in crate `my_module` appears as `my_module::foo::invoke`
//! (and mangled forms). A body that can't be unambiguously located → wildcard.

use walrus::{ExportItem, FunctionId, FunctionKind, Module};

use crate::map::HashSet;

/// The located entry for one reducer.
#[derive(Debug, Clone, Copy)]
pub struct ReducerEntry {
    pub body: Option<FunctionId>,
}

/// Locate the body function for each reducer name. `reducer_names` must be in
/// `ModuleDef.reducers` order so the result is indexed by reducer id.
pub fn locate(module: &Module, reducer_names: &[&str]) -> Vec<ReducerEntry> {
    let describers = describer_export_names(module);
    reducer_names
        .iter()
        .map(|name| {
            let body = find_body(module, name);
            // Presence cross-check only: describers are keyed by accessor name
            // (may differ from `name`), so a mismatch is not a soundness issue;
            // this just keeps the recognized-codegen assumption testable.
            let _ = describers.contains(*name);
            ReducerEntry { body }
        })
        .collect()
}

/// `__preinit__20_register_describer_<accessor_name>` export suffixes, for both
/// table and reducer/procedure/view describers (hence not a reducer list).
pub fn describer_export_names(module: &Module) -> HashSet<String> {
    const PREFIX: &str = "__preinit__20_register_describer_";
    let mut out = HashSet::new();
    for export in module.exports.iter() {
        if let ExportItem::Function(_) = export.item
            && let Some(rest) = export.name.strip_prefix(PREFIX)
        {
            out.insert(rest.to_owned());
        }
    }
    out
}

/// Find the entry function for `reducer`: the `<crate>::<reducer>::invoke`
/// wrapper. We anchor on `invoke` (not the standalone body) because it is the
/// function `__call_reducer__` dispatches to and it survives release inlining,
/// which folds the standalone body into it. A non-unique match → wildcard.
fn find_body(module: &Module, reducer: &str) -> Option<FunctionId> {
    let mut candidates: Vec<FunctionId> = Vec::new();

    for func in module.funcs.iter() {
        if !matches!(func.kind, FunctionKind::Local(_)) {
            continue;
        }
        let Some(name) = func.name.as_deref() else {
            continue;
        };
        if name_is_invoke_wrapper(name, reducer) {
            candidates.push(func.id());
        }
    }

    match candidates.as_slice() {
        [one] => Some(*one),
        _ => None,
    }
}

/// Match the `<reducer>::invoke` wrapper in mangled or demangled form. The
/// mangled needle `<reducer_len><reducer>6invoke` is anchored on the exact
/// name length, so a reducer that is a prefix of another can't match its wrapper.
fn name_is_invoke_wrapper(name: &str, reducer: &str) -> bool {
    let mangled = format!("{}{}6invoke", reducer.len(), reducer);
    if name.contains(&mangled) {
        return true;
    }
    let demangled = format!("::{reducer}::invoke");
    name.contains(&demangled)
}
