//! Static per-reducer table access-set analysis for SpacetimeDB WASM modules.
//!
//! [`analyze`] computes, for each reducer, the tables it reads and writes — or
//! marks it `wildcard` (conflicts-with-everything) when a sound, finite access
//! set cannot be proven.
//!
//! Soundness is absolute: the result over-approximates (every table actually
//! touched is in the predicted set, OR the reducer is `wildcard`), because an
//! under-approximation would corrupt data for any consumer that schedules on
//! disjointness. Any uncertainty — unresolved id, unknown reachable host op,
//! dangerous `call_indirect`, non-constant name string, missing reducer body,
//! or any unmatched codegen pattern — wildcards the reducer.

mod analyze;
mod error;
mod ids;
mod imports;
mod map;
mod reachability;
mod reducers;

pub use analyze::{analyze, AccessSet};
pub use error::AnalysisError;
