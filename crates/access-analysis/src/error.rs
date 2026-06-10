use thiserror::Error;

/// Errors that can occur while analyzing a module.
///
/// Analysis *uncertainty* never produces an `Err` — it produces a `wildcard`
/// reducer (preserving soundness). `Err` is reserved for inputs that are not
/// analyzable at all (e.g. malformed wasm).
#[derive(Debug, Error)]
pub enum AnalysisError {
    #[error("failed to parse wasm module: {0}")]
    Parse(String),
}
