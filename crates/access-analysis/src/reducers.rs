//! Locating each reducer's entry body in the wasm.
//!
//! We don't analyze through `__call_reducer__`: it dispatches by index via
//! `call_indirect`. Instead we find each reducer's body directly using the
//! name section, which survives both Debug and Release (`wasm-opt -g` keeps it).
//! A reducer `foo` in crate `my_module` appears as `my_module::foo::invoke`
//! (and mangled forms). A body that can't be unambiguously located → wildcard.

use convert_case::{Case, Casing};
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
            let body = find_body(module, name).or_else(|| {
                // Schema snake-cases reducer names at letter-digit boundaries
                // (Rust `insert_bulk_u32` → ModuleDef `insert_bulk_u_32`) while
                // the wasm symbol keeps the raw identifier; describer exports
                // still carry the raw names.
                let raw_candidates = snake_case_candidates(&describers, name);
                let found: HashSet<FunctionId> = raw_candidates
                    .iter()
                    .filter_map(|raw| find_body(module, raw))
                    .collect();
                // Accept only an unambiguous single match; multiple distinct
                // bodies means the mapping is not injective here → wildcard.
                let mut iter = found.into_iter();
                match (iter.next(), iter.next()) {
                    (Some(one), None) => Some(one),
                    _ => None,
                }
            });
            ReducerEntry { body }
        })
        .collect()
}

/// Return the subset of `describers` whose entries are distinct from `name`
/// AND whose snake-cased form equals `name`. These are the raw Rust identifiers
/// that schema would have renamed into `name`.
pub(crate) fn snake_case_candidates<'a>(
    describers: &'a HashSet<String>,
    name: &str,
) -> Vec<&'a str> {
    describers
        .iter()
        .filter(|raw| raw.as_str() != name && raw.to_case(Case::Snake) == name)
        .map(|s| s.as_str())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn digit_adjacent_name_found_via_describer() {
        // `insert_bulk_u32` snake-cases to `insert_bulk_u_32`; the ModuleDef
        // name is `insert_bulk_u_32`, the raw wasm symbol is `insert_bulk_u32`.
        let describers = set(&["insert_bulk_u32", "other_reducer", "insert_bulk_u_32"]);
        let candidates = snake_case_candidates(&describers, "insert_bulk_u_32");
        assert_eq!(candidates, vec!["insert_bulk_u32"]);
    }

    #[test]
    fn raw_equals_name_excluded() {
        // If the raw identifier already equals the ModuleDef name, there is no
        // letter-digit mismatch; it should never appear as a candidate.
        let describers = set(&["writes_a"]);
        let candidates = snake_case_candidates(&describers, "writes_a");
        assert!(candidates.is_empty());
    }

    #[test]
    fn multiple_candidates_returns_all_for_dedup_check() {
        // GENUINE >1-candidate case. Two distinct raw names that BOTH snake-case
        // (convert_case 0.6) to the same target AND both differ from it:
        //   "writes_aV2".to_case(Snake) == "writes_a_v_2"
        //   "writesA_v2".to_case(Snake) == "writes_a_v_2"
        // (verified below with the real convert_case). `snake_case_candidates`
        // must therefore return BOTH; `locate`'s caller then dedups by
        // FunctionId and wildcards if they resolve to distinct bodies.
        // NB: the all-caps run does NOT collide — convert_case 0.6 yields
        // "writesAV2".to_case(Snake) == "writes_av_2" (a single `av` token),
        // not "writes_a_v_2"; hence the mixed-case raws above.
        let raws = ["writes_aV2", "writesA_v2"];
        for raw in raws {
            assert_eq!(
                raw.to_case(Case::Snake),
                "writes_a_v_2",
                "convert_case 0.6 must map {raw} to the colliding target",
            );
            assert_ne!(raw, "writes_a_v_2", "{raw} must differ from the target");
        }
        let describers = set(&raws);
        let mut candidates = snake_case_candidates(&describers, "writes_a_v_2");
        candidates.sort_unstable(); // HashSet iteration order is unspecified
        assert_eq!(candidates, vec!["writesA_v2", "writes_aV2"]);

        // `locate`'s ambiguity→None arm (distinct `FunctionId`s) needs a real
        // wasm `Module` for `find_body`, so this multi-candidate path is its
        // unit proxy; integration tests cover the single-body path.
    }

    #[test]
    fn v2_suffix_snake_cased_correctly() {
        // Rust fn `writes_a_v2` → schema snake-cases `v2` → `v_2`, so the
        // ModuleDef name becomes `writes_a_v_2`. The raw wasm ident is
        // `writes_a_v2`. Verify the helper maps it correctly.
        let describers = set(&["writes_a_v2", "writes_a"]);
        let candidates = snake_case_candidates(&describers, "writes_a_v_2");
        assert_eq!(candidates, vec!["writes_a_v2"]);
    }

    #[test]
    fn no_false_positives_for_unrelated_names() {
        let describers = set(&["load_location_table", "reads_b", "touches_both"]);
        let candidates = snake_case_candidates(&describers, "writes_a_v_2");
        assert!(candidates.is_empty());
    }
}
