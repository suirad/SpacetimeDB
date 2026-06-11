pub mod db_metrics;
pub mod error;
pub mod execution_context;
pub mod locking_tx_datastore;
pub mod system_tables;
pub mod traits;

pub use locking_tx_datastore::{BatchTxState, FinishedBatchTx, ReducerTx, ReducerTxVariant};

use error::DatastoreError;

pub type Result<T> = core::result::Result<T, DatastoreError>;
