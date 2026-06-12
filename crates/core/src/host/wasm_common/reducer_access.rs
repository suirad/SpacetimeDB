/// Per-module static access analysis: conflict matrix and per-reducer admission flags.
///
/// Built once at module load from the wasm binary and stored on the actor. Resolution
/// of table names → [`TableId`]s is deferred to first use because the datastore does
/// not yet exist at actor-construction time during initial publish; it is performed
/// lazily via a short-lived read transaction and cached in a [`OnceLock`].
use std::sync::OnceLock;

use spacetimedb_access_analysis::{analyze, AccessSet, ConflictMatrix};
use spacetimedb_datastore::execution_context::Workload;
use spacetimedb_primitives::TableId;
use spacetimedb_schema::def::ModuleDef;

use crate::db::relational_db::RelationalDB;

/// Static access analysis result for a module, indexed by reducer id (== `ModuleDef` reducer order).
pub(crate) struct ReducerAccessInfo {
    /// One `AccessSet` per reducer, in `ModuleDef` order (== `ReducerId` index).
    sets: Vec<AccessSet>,
    /// Pairwise conflict matrix built from `sets`.
    pub matrix: ConflictMatrix,
    /// `lifecycle[i]` is `true` iff reducer `i` is a lifecycle reducer (excluded from batch admission).
    pub lifecycle: Vec<bool>,
    /// `wildcard[i]` is `true` iff the static analyzer set the wildcard flag for reducer `i`.
    /// Distinct from `ResolvedWrites::wildcard` which additionally includes name-resolution failures.
    pub wildcard: Vec<bool>,
    /// Name→`TableId` mapping, resolved lazily on first access to a live datastore.
    resolved: OnceLock<ResolvedWrites>,
}

pub(crate) struct ResolvedWrites {
    /// Resolved write-table ids per reducer (empty for readers / wildcard reducers).
    pub write_tables: Vec<Vec<TableId>>,
    /// `wildcard[i]` is true if reducer i should be treated as conflicting with everything:
    /// either the analyzer set the wildcard flag, or at least one write-set table name could
    /// not be resolved at the time of resolution (sound: over-approximation).
    pub wildcard: Vec<bool>,
}

impl ResolvedWrites {
    #[cfg(test)]
    pub(crate) fn for_test(write_tables: Vec<Vec<TableId>>, wildcard: Vec<bool>) -> Self {
        Self { write_tables, wildcard }
    }
}

impl ReducerAccessInfo {
    /// Analysis failure never blocks publish: falls back to all-wildcard (module will not batch).
    pub fn new(wasm: &[u8], module_def: &ModuleDef) -> Self {
        match analyze(wasm, module_def) {
            Ok(sets) => Self::from_sets(sets, module_def),
            Err(err) => {
                log::warn!(
                    "static access analysis failed, falling back to all-wildcard \
                     (module will not batch): {err}"
                );
                Self::all_wildcard(module_def)
            }
        }
    }

    pub fn all_wildcard(module_def: &ModuleDef) -> Self {
        let n = module_def.reducers().count();
        let sets: Vec<AccessSet> = (0..n)
            .map(|_| AccessSet {
                wildcard: true,
                ..Default::default()
            })
            .collect();
        Self::from_sets(sets, module_def)
    }

    fn from_sets(sets: Vec<AccessSet>, module_def: &ModuleDef) -> Self {
        let matrix = ConflictMatrix::build(&sets);
        let lifecycle: Vec<bool> = module_def.reducers().map(|r| r.lifecycle.is_some()).collect();
        let wildcard: Vec<bool> = sets.iter().map(|s| s.wildcard).collect();
        Self {
            sets,
            matrix,
            lifecycle,
            wildcard,
            resolved: OnceLock::new(),
        }
    }

    /// The `resolved` OnceLock starts empty; callers needing `resolved()` must use a live DB
    /// or supply a separately-constructed [`ResolvedWrites`].
    #[cfg(test)]
    pub(crate) fn for_test(sets: Vec<AccessSet>, lifecycle: Vec<bool>) -> Self {
        let matrix = ConflictMatrix::build(&sets);
        let wildcard: Vec<bool> = sets.iter().map(|s| s.wildcard).collect();
        Self {
            sets,
            matrix,
            lifecycle,
            wildcard,
            resolved: OnceLock::new(),
        }
    }

    /// Resolve table names → [`TableId`]s, caching the result on first call.
    ///
    /// Must not be called before the datastore exists (e.g. during initial publish).
    /// Unknown table names are wildcarded (sound over-approximation).
    pub fn resolved(&self, db: &RelationalDB) -> &ResolvedWrites {
        self.resolved.get_or_init(|| {
            db.with_read_only(Workload::Internal, |tx| {
                let mut write_tables = Vec::with_capacity(self.sets.len());
                let mut wildcard = Vec::with_capacity(self.sets.len());

                for set in &self.sets {
                    if set.wildcard {
                        write_tables.push(Vec::new());
                        wildcard.push(true);
                        continue;
                    }

                    let mut ids = Vec::with_capacity(set.writes.len());
                    let mut is_wildcard = false;

                    for name in &set.writes {
                        match db.table_id_from_name(tx, name) {
                            Ok(Some(id)) => ids.push(id),
                            Ok(None) => {
                                // Table name present in access set but absent from schema;
                                // wildcard this reducer (sound over-approximation).
                                is_wildcard = true;
                                break;
                            }
                            Err(err) => {
                                log::warn!(
                                    "failed to resolve table '{}' during access-set resolution, \
                                     treating reducer as wildcard: {err}",
                                    &**name
                                );
                                is_wildcard = true;
                                break;
                            }
                        }
                    }

                    write_tables.push(if is_wildcard { Vec::new() } else { ids });
                    wildcard.push(is_wildcard);
                }

                ResolvedWrites { write_tables, wildcard }
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use spacetimedb_lib::db::raw_def::v9::{Lifecycle, RawModuleDefV9Builder};
    use spacetimedb_sats::ProductType;

    fn empty_module_def() -> ModuleDef {
        let raw = RawModuleDefV9Builder::new().finish();
        ModuleDef::try_from(raw).expect("empty module def")
    }

    fn module_def_with_reducers(names_lifecycles: &[(&str, Option<Lifecycle>)]) -> ModuleDef {
        let mut builder = RawModuleDefV9Builder::new();
        for (name, lifecycle) in names_lifecycles {
            builder.add_reducer(name.to_string(), ProductType::unit(), *lifecycle);
        }
        ModuleDef::try_from(builder.finish()).expect("module def")
    }

    #[test]
    fn garbage_wasm_bytes_empty_module_no_panic() {
        // Garbage bytes with no reducers: analysis fails → all-wildcard fallback, no panic.
        let module_def = empty_module_def();
        let info = ReducerAccessInfo::new(b"this is not valid wasm", &module_def);
        assert_eq!(info.lifecycle.len(), 0);
        assert_eq!(info.sets.len(), 0);
    }

    #[test]
    fn garbage_wasm_with_reducers_produces_all_wildcard_matrix() {
        let module_def = module_def_with_reducers(&[("foo", None), ("bar", None)]);
        let info = ReducerAccessInfo::new(b"garbage wasm bytes that will fail to parse", &module_def);

        assert_eq!(info.sets.len(), 2);
        assert!(info.sets.iter().all(|s| s.wildcard), "expected all wildcard on parse failure");

        // Wildcard reducers conflict with everything, including themselves.
        for i in 0..2 {
            for j in 0..2 {
                assert!(info.matrix.conflicts(i, j), "reducer {i} should conflict with {j}");
            }
        }
        assert!(!info.lifecycle[0]);
        assert!(!info.lifecycle[1]);
    }

    #[test]
    fn lifecycle_flag_extraction() {
        let module_def = module_def_with_reducers(&[
            ("on_connect", Some(Lifecycle::OnConnect)),
            ("normal", None),
            ("on_disconnect", Some(Lifecycle::OnDisconnect)),
        ]);
        let info = ReducerAccessInfo::all_wildcard(&module_def);

        assert_eq!(info.lifecycle.len(), 3);
        assert!(info.lifecycle[0], "on_connect should be lifecycle");
        assert!(!info.lifecycle[1], "normal should not be lifecycle");
        assert!(info.lifecycle[2], "on_disconnect should be lifecycle");
    }

    #[test]
    fn all_wildcard_ctor_conflicts_with_everything() {
        let module_def = module_def_with_reducers(&[("a", None), ("b", None)]);
        let info = ReducerAccessInfo::all_wildcard(&module_def);

        for i in 0..2 {
            for j in 0..2 {
                assert!(
                    info.matrix.conflicts(i, j),
                    "all_wildcard reducer {i} should conflict with {j}"
                );
            }
        }
    }
}
