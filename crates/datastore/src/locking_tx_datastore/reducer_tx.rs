use super::{
    batch_tx::BatchTxState,
    mut_tx::{
        FuncCallType, IndexScanPoint, IndexScanPointOrRange, MutTxId, ObservedAccess, RowRefInsertion, ViewCallInfo,
    },
    state_view::{IterByColEqMutTx, IterByColRangeMutTx, IterMutTx, StateView},
};
use crate::{
    execution_context::ExecutionContext,
    locking_tx_datastore::datastore::Result,
    traits::{InsertFlags, RowTypeForTable, UpdateFlags},
};
use core::ops::RangeBounds;
use spacetimedb_lib::metrics::ExecutionMetrics;
use spacetimedb_primitives::{ColList, IndexId, SequenceId, TableId};
use spacetimedb_sats::{AlgebraicValue, ProductValue};
use spacetimedb_schema::schema::TableSchema;
use spacetimedb_table::table_index::IndexKey;
use spacetimedb_table::{indexes::RowPointer, table::RowRef};
use std::sync::Arc;

/// The reducer-accessible surface of a transaction.
///
/// Implementations are [`MutTxId`] (serial mutable tx) and [`BatchTxState`]
/// (concurrent overlay tx). Dispatch is via [`ReducerTxVariant`].
///
/// This trait is not object-safe (associated types, const generics); dispatch
/// is by enum match in [`ReducerTxVariant`].
pub trait ReducerTx: StateView {
    // -- Context / metrics ---------------------------------------------------
    fn ctx(&self) -> &ExecutionContext;
    fn metrics_mut(&mut self) -> &mut ExecutionMetrics;

    // -- Read helpers --------------------------------------------------------
    fn row_type_for_table(&self, table_id: TableId) -> Result<RowTypeForTable<'_>>;
    fn schema_for_table(&self, table_id: TableId) -> Result<Arc<TableSchema>>;
    fn table_id_from_name_or_alias(&self, name: &str) -> Result<Option<TableId>>;
    fn index_id_from_name_or_alias(&self, name: &str) -> Result<Option<IndexId>>;
    fn view_id_from_name(&self, name: &str) -> Result<Option<spacetimedb_primitives::ViewId>>;
    fn get(&self, table_id: TableId, row_ptr: RowPointer) -> Result<Option<RowRef<'_>>>;
    fn get_jwt_payload(&self, connection_id: spacetimedb_lib::ConnectionId) -> Result<Option<String>>;

    // -- Writes --------------------------------------------------------------
    fn insert<'a, const GENERATE: bool>(
        &'a mut self,
        table_id: TableId,
        row: &[u8],
    ) -> Result<(ColList, RowRefInsertion<'a>, InsertFlags)>;

    fn update(
        &mut self,
        table_id: TableId,
        index_id: IndexId,
        row: &[u8],
    ) -> Result<(ColList, RowRefInsertion<'_>, UpdateFlags)>;

    fn delete(&mut self, table_id: TableId, row_pointer: RowPointer) -> Result<bool>;

    fn delete_by_row_value(&mut self, table_id: TableId, rel: &ProductValue) -> Result<bool>;

    fn clear_table(&mut self, table_id: TableId) -> Result<u64>;

    // -- Sequences -----------------------------------------------------------
    fn get_next_sequence_value(&mut self, seq_id: SequenceId) -> Result<i128>;

    // -- Index scans (host fn surface) ---------------------------------------
    fn index_scan_point<'a, 'p>(
        &'a self,
        index_id: IndexId,
        point: &'p [u8],
    ) -> Result<(TableId, IndexKey<'p>, IndexScanPoint<'a>)>;

    fn index_scan_range<'de, 'a>(
        &'a self,
        index_id: IndexId,
        prefix: &'de [u8],
        prefix_elems: spacetimedb_primitives::ColId,
        rstart: &'de [u8],
        rend: &'de [u8],
    ) -> Result<(TableId, IndexScanPointOrRange<'de, 'a>)>
    where
        'de: 'a;

    // -- Observability hooks -------------------------------------------------
    fn record_table_scan(&mut self, op: &FuncCallType, table_id: TableId);
    fn record_index_scan_range(
        &mut self,
        op: &FuncCallType,
        table_id: TableId,
        index_id: IndexId,
        point: Option<IndexKey<'_>>,
    );
    fn record_index_scan_point(&mut self, op: &FuncCallType, table_id: TableId, index_id: IndexId, point: IndexKey<'_>);
    fn record_table_write(&mut self, op: &FuncCallType, table_id: TableId);
    fn record_index_write(&mut self, op: &FuncCallType, index_id: IndexId);
    fn enable_access_capture(&mut self);
    fn take_observed(&mut self) -> Option<Box<ObservedAccess>>;
    fn replace_view_read_set(&mut self, call: ViewCallInfo);
}

// -------------------------------------------------------------------------
// impl ReducerTx for MutTxId
// -------------------------------------------------------------------------

impl ReducerTx for MutTxId {
    fn ctx(&self) -> &ExecutionContext {
        &self.ctx
    }
    fn metrics_mut(&mut self) -> &mut ExecutionMetrics {
        &mut self.metrics
    }

    fn row_type_for_table(&self, table_id: TableId) -> Result<RowTypeForTable<'_>> {
        self.row_type_for_table(table_id)
    }
    fn schema_for_table(&self, table_id: TableId) -> Result<Arc<TableSchema>> {
        StateView::schema_for_table(self, table_id)
    }
    fn table_id_from_name_or_alias(&self, name: &str) -> Result<Option<TableId>> {
        StateView::table_id_from_name_or_alias(self, name)
    }
    fn index_id_from_name_or_alias(&self, name: &str) -> Result<Option<IndexId>> {
        self.index_id_from_name_or_alias(name)
    }
    fn view_id_from_name(&self, name: &str) -> Result<Option<spacetimedb_primitives::ViewId>> {
        self.view_id_from_name(name)
    }
    fn get(&self, table_id: TableId, row_ptr: RowPointer) -> Result<Option<RowRef<'_>>> {
        self.get(table_id, row_ptr)
    }
    fn get_jwt_payload(&self, connection_id: spacetimedb_lib::ConnectionId) -> Result<Option<String>> {
        StateView::get_jwt_payload(self, connection_id)
    }

    fn insert<'a, const GENERATE: bool>(
        &'a mut self,
        table_id: TableId,
        row: &[u8],
    ) -> Result<(ColList, RowRefInsertion<'a>, InsertFlags)> {
        self.insert::<GENERATE>(table_id, row)
    }
    fn update(
        &mut self,
        table_id: TableId,
        index_id: IndexId,
        row: &[u8],
    ) -> Result<(ColList, RowRefInsertion<'_>, UpdateFlags)> {
        self.update(table_id, index_id, row)
    }
    fn delete(&mut self, table_id: TableId, row_pointer: RowPointer) -> Result<bool> {
        self.delete(table_id, row_pointer)
    }
    fn delete_by_row_value(&mut self, table_id: TableId, rel: &ProductValue) -> Result<bool> {
        self.delete_by_row_value(table_id, rel)
    }
    fn clear_table(&mut self, table_id: TableId) -> Result<u64> {
        self.clear_table(table_id)
    }
    fn get_next_sequence_value(&mut self, seq_id: SequenceId) -> Result<i128> {
        self.get_next_sequence_value(seq_id)
    }

    fn index_scan_point<'a, 'p>(
        &'a self,
        index_id: IndexId,
        point: &'p [u8],
    ) -> Result<(TableId, IndexKey<'p>, IndexScanPoint<'a>)> {
        self.index_scan_point(index_id, point)
    }
    fn index_scan_range<'de, 'a>(
        &'a self,
        index_id: IndexId,
        prefix: &'de [u8],
        prefix_elems: spacetimedb_primitives::ColId,
        rstart: &'de [u8],
        rend: &'de [u8],
    ) -> Result<(TableId, IndexScanPointOrRange<'de, 'a>)>
    where
        'de: 'a,
    {
        self.index_scan_range(index_id, prefix, prefix_elems, rstart, rend)
    }

    fn record_table_scan(&mut self, op: &FuncCallType, table_id: TableId) {
        self.record_table_scan(op, table_id)
    }
    fn record_index_scan_range(
        &mut self,
        op: &FuncCallType,
        table_id: TableId,
        index_id: IndexId,
        point: Option<IndexKey<'_>>,
    ) {
        self.record_index_scan_range(op, table_id, index_id, point)
    }
    fn record_index_scan_point(
        &mut self,
        op: &FuncCallType,
        table_id: TableId,
        index_id: IndexId,
        point: IndexKey<'_>,
    ) {
        self.record_index_scan_point(op, table_id, index_id, point)
    }
    fn record_table_write(&mut self, op: &FuncCallType, table_id: TableId) {
        self.record_table_write(op, table_id)
    }
    fn record_index_write(&mut self, op: &FuncCallType, index_id: IndexId) {
        self.record_index_write(op, index_id)
    }
    fn enable_access_capture(&mut self) {
        self.enable_access_capture()
    }
    fn take_observed(&mut self) -> Option<Box<ObservedAccess>> {
        self.take_observed()
    }
    fn replace_view_read_set(&mut self, call: ViewCallInfo) {
        self.replace_view_read_set(call)
    }
}

// -------------------------------------------------------------------------
// impl ReducerTx for BatchTxState
// -------------------------------------------------------------------------

impl ReducerTx for BatchTxState {
    fn ctx(&self) -> &ExecutionContext {
        &self.ctx
    }
    fn metrics_mut(&mut self) -> &mut ExecutionMetrics {
        &mut self.metrics
    }

    fn row_type_for_table(&self, table_id: TableId) -> Result<RowTypeForTable<'_>> {
        if let Some(row_type) = self
            .committed_state_read_lock
            .get_table(table_id)
            .map(|t| t.get_row_type())
        {
            return Ok(RowTypeForTable::Ref(row_type));
        }
        Ok(RowTypeForTable::Arc(StateView::schema_for_table(self, table_id)?))
    }
    fn schema_for_table(&self, table_id: TableId) -> Result<Arc<TableSchema>> {
        StateView::schema_for_table(self, table_id)
    }
    fn table_id_from_name_or_alias(&self, name: &str) -> Result<Option<TableId>> {
        StateView::table_id_from_name_or_alias(self, name)
    }
    fn index_id_from_name_or_alias(&self, name: &str) -> Result<Option<IndexId>> {
        StateView::index_id_from_name_or_alias(self, name)
    }
    fn view_id_from_name(&self, name: &str) -> Result<Option<spacetimedb_primitives::ViewId>> {
        use crate::system_tables::{StViewFields, ST_VIEW_ID};
        let view_name = &name.into();
        let row = self
            .iter_by_col_eq(ST_VIEW_ID, StViewFields::ViewName, view_name)?
            .next();
        Ok(row.map(|row| row.read_col(StViewFields::ViewId).unwrap()))
    }
    fn get(&self, table_id: TableId, row_ptr: RowPointer) -> Result<Option<RowRef<'_>>> {
        self.get(table_id, row_ptr)
    }
    fn get_jwt_payload(&self, connection_id: spacetimedb_lib::ConnectionId) -> Result<Option<String>> {
        StateView::get_jwt_payload(self, connection_id)
    }

    fn insert<'a, const GENERATE: bool>(
        &'a mut self,
        table_id: TableId,
        row: &[u8],
    ) -> Result<(ColList, RowRefInsertion<'a>, InsertFlags)> {
        self.insert::<GENERATE>(table_id, row)
    }
    fn update(
        &mut self,
        table_id: TableId,
        index_id: IndexId,
        row: &[u8],
    ) -> Result<(ColList, RowRefInsertion<'_>, UpdateFlags)> {
        self.update(table_id, index_id, row)
    }

    fn delete(&mut self, table_id: TableId, row_pointer: RowPointer) -> Result<bool> {
        self.delete(table_id, row_pointer)
    }
    fn delete_by_row_value(&mut self, table_id: TableId, rel: &ProductValue) -> Result<bool> {
        self.delete_by_row_value(table_id, rel)
    }
    fn clear_table(&mut self, table_id: TableId) -> Result<u64> {
        self.clear_table(table_id)
    }
    fn get_next_sequence_value(&mut self, seq_id: SequenceId) -> Result<i128> {
        self.get_next_sequence_value(seq_id)
    }

    fn index_scan_point<'a, 'p>(
        &'a self,
        index_id: IndexId,
        point: &'p [u8],
    ) -> Result<(TableId, IndexKey<'p>, IndexScanPoint<'a>)> {
        self.index_scan_point(index_id, point)
    }
    fn index_scan_range<'de, 'a>(
        &'a self,
        index_id: IndexId,
        prefix: &'de [u8],
        prefix_elems: spacetimedb_primitives::ColId,
        rstart: &'de [u8],
        rend: &'de [u8],
    ) -> Result<(TableId, IndexScanPointOrRange<'de, 'a>)>
    where
        'de: 'a,
    {
        self.index_scan_range(index_id, prefix, prefix_elems, rstart, rend)
    }

    fn record_table_scan(&mut self, op: &FuncCallType, table_id: TableId) {
        self.record_table_scan(op, table_id)
    }
    fn record_index_scan_range(
        &mut self,
        op: &FuncCallType,
        table_id: TableId,
        index_id: IndexId,
        point: Option<IndexKey<'_>>,
    ) {
        self.record_index_scan_range(op, table_id, index_id, point)
    }
    fn record_index_scan_point(
        &mut self,
        op: &FuncCallType,
        table_id: TableId,
        index_id: IndexId,
        point: IndexKey<'_>,
    ) {
        self.record_index_scan_point(op, table_id, index_id, point)
    }
    fn record_table_write(&mut self, op: &FuncCallType, table_id: TableId) {
        self.record_table_write(op, table_id)
    }
    fn record_index_write(&mut self, op: &FuncCallType, index_id: IndexId) {
        self.record_index_write(op, index_id)
    }
    fn enable_access_capture(&mut self) {
        self.enable_access_capture()
    }
    fn take_observed(&mut self) -> Option<Box<ObservedAccess>> {
        self.take_observed()
    }
    fn replace_view_read_set(&mut self, call: ViewCallInfo) {
        self.replace_view_read_set(call)
    }
}

// -------------------------------------------------------------------------
// ReducerTxVariant enum
// -------------------------------------------------------------------------

/// Dispatch enum over the two reducer-reachable transaction types.
pub enum ReducerTxVariant {
    Mut(MutTxId),
    Batch(BatchTxState),
}

impl StateView for ReducerTxVariant {
    type Iter<'a> = IterMutTx<'a>;
    type IterByColRange<'a, R: RangeBounds<AlgebraicValue>> = IterByColRangeMutTx<'a, R>;
    type IterByColEq<'a, 'r>
        = IterByColEqMutTx<'a, 'r>
    where
        Self: 'a;

    fn get_schema(&self, table_id: TableId) -> Option<&Arc<TableSchema>> {
        match self {
            Self::Mut(tx) => tx.get_schema(table_id),
            Self::Batch(tx) => tx.get_schema(table_id),
        }
    }
    fn table_row_count(&self, table_id: TableId) -> Option<u64> {
        match self {
            Self::Mut(tx) => tx.table_row_count(table_id),
            Self::Batch(tx) => tx.table_row_count(table_id),
        }
    }
    fn iter(&self, table_id: TableId) -> Result<Self::Iter<'_>> {
        match self {
            Self::Mut(tx) => tx.iter(table_id),
            Self::Batch(tx) => tx.iter(table_id),
        }
    }
    fn iter_by_col_range<R: RangeBounds<AlgebraicValue>>(
        &self,
        table_id: TableId,
        cols: ColList,
        range: R,
    ) -> Result<Self::IterByColRange<'_, R>> {
        match self {
            Self::Mut(tx) => tx.iter_by_col_range(table_id, cols, range),
            Self::Batch(tx) => tx.iter_by_col_range(table_id, cols, range),
        }
    }
    fn iter_by_col_eq<'r>(
        &self,
        table_id: TableId,
        cols: impl Into<ColList>,
        value: &'r AlgebraicValue,
    ) -> Result<Self::IterByColEq<'_, 'r>> {
        match self {
            Self::Mut(tx) => tx.iter_by_col_eq(table_id, cols, value),
            Self::Batch(tx) => tx.iter_by_col_eq(table_id, cols, value),
        }
    }
}

impl ReducerTx for ReducerTxVariant {
    fn ctx(&self) -> &ExecutionContext {
        match self {
            Self::Mut(tx) => tx.ctx(),
            Self::Batch(tx) => tx.ctx(),
        }
    }
    fn metrics_mut(&mut self) -> &mut ExecutionMetrics {
        match self {
            Self::Mut(tx) => tx.metrics_mut(),
            Self::Batch(tx) => tx.metrics_mut(),
        }
    }
    fn row_type_for_table(&self, table_id: TableId) -> Result<RowTypeForTable<'_>> {
        match self {
            Self::Mut(tx) => ReducerTx::row_type_for_table(tx, table_id),
            Self::Batch(tx) => ReducerTx::row_type_for_table(tx, table_id),
        }
    }
    fn schema_for_table(&self, table_id: TableId) -> Result<Arc<TableSchema>> {
        match self {
            Self::Mut(tx) => ReducerTx::schema_for_table(tx, table_id),
            Self::Batch(tx) => ReducerTx::schema_for_table(tx, table_id),
        }
    }
    fn table_id_from_name_or_alias(&self, name: &str) -> Result<Option<TableId>> {
        match self {
            Self::Mut(tx) => ReducerTx::table_id_from_name_or_alias(tx, name),
            Self::Batch(tx) => ReducerTx::table_id_from_name_or_alias(tx, name),
        }
    }
    fn index_id_from_name_or_alias(&self, name: &str) -> Result<Option<IndexId>> {
        match self {
            Self::Mut(tx) => ReducerTx::index_id_from_name_or_alias(tx, name),
            Self::Batch(tx) => ReducerTx::index_id_from_name_or_alias(tx, name),
        }
    }
    fn view_id_from_name(&self, name: &str) -> Result<Option<spacetimedb_primitives::ViewId>> {
        match self {
            Self::Mut(tx) => ReducerTx::view_id_from_name(tx, name),
            Self::Batch(tx) => ReducerTx::view_id_from_name(tx, name),
        }
    }
    fn get(&self, table_id: TableId, row_ptr: RowPointer) -> Result<Option<RowRef<'_>>> {
        match self {
            Self::Mut(tx) => ReducerTx::get(tx, table_id, row_ptr),
            Self::Batch(tx) => ReducerTx::get(tx, table_id, row_ptr),
        }
    }
    fn get_jwt_payload(&self, connection_id: spacetimedb_lib::ConnectionId) -> Result<Option<String>> {
        match self {
            Self::Mut(tx) => ReducerTx::get_jwt_payload(tx, connection_id),
            Self::Batch(tx) => ReducerTx::get_jwt_payload(tx, connection_id),
        }
    }
    fn insert<'a, const GENERATE: bool>(
        &'a mut self,
        table_id: TableId,
        row: &[u8],
    ) -> Result<(ColList, RowRefInsertion<'a>, InsertFlags)> {
        match self {
            Self::Mut(tx) => tx.insert::<GENERATE>(table_id, row),
            Self::Batch(tx) => tx.insert::<GENERATE>(table_id, row),
        }
    }
    fn update(
        &mut self,
        table_id: TableId,
        index_id: IndexId,
        row: &[u8],
    ) -> Result<(ColList, RowRefInsertion<'_>, UpdateFlags)> {
        match self {
            Self::Mut(tx) => ReducerTx::update(tx, table_id, index_id, row),
            Self::Batch(tx) => ReducerTx::update(tx, table_id, index_id, row),
        }
    }
    fn delete(&mut self, table_id: TableId, row_pointer: RowPointer) -> Result<bool> {
        match self {
            Self::Mut(tx) => ReducerTx::delete(tx, table_id, row_pointer),
            Self::Batch(tx) => ReducerTx::delete(tx, table_id, row_pointer),
        }
    }
    fn delete_by_row_value(&mut self, table_id: TableId, rel: &ProductValue) -> Result<bool> {
        match self {
            Self::Mut(tx) => ReducerTx::delete_by_row_value(tx, table_id, rel),
            Self::Batch(tx) => ReducerTx::delete_by_row_value(tx, table_id, rel),
        }
    }
    fn clear_table(&mut self, table_id: TableId) -> Result<u64> {
        match self {
            Self::Mut(tx) => ReducerTx::clear_table(tx, table_id),
            Self::Batch(tx) => ReducerTx::clear_table(tx, table_id),
        }
    }
    fn get_next_sequence_value(&mut self, seq_id: SequenceId) -> Result<i128> {
        match self {
            Self::Mut(tx) => ReducerTx::get_next_sequence_value(tx, seq_id),
            Self::Batch(tx) => ReducerTx::get_next_sequence_value(tx, seq_id),
        }
    }
    fn index_scan_point<'a, 'p>(
        &'a self,
        index_id: IndexId,
        point: &'p [u8],
    ) -> Result<(TableId, IndexKey<'p>, IndexScanPoint<'a>)> {
        match self {
            Self::Mut(tx) => ReducerTx::index_scan_point(tx, index_id, point),
            Self::Batch(tx) => ReducerTx::index_scan_point(tx, index_id, point),
        }
    }
    fn index_scan_range<'de, 'a>(
        &'a self,
        index_id: IndexId,
        prefix: &'de [u8],
        prefix_elems: spacetimedb_primitives::ColId,
        rstart: &'de [u8],
        rend: &'de [u8],
    ) -> Result<(TableId, IndexScanPointOrRange<'de, 'a>)>
    where
        'de: 'a,
    {
        match self {
            Self::Mut(tx) => ReducerTx::index_scan_range(tx, index_id, prefix, prefix_elems, rstart, rend),
            Self::Batch(tx) => ReducerTx::index_scan_range(tx, index_id, prefix, prefix_elems, rstart, rend),
        }
    }
    fn record_table_scan(&mut self, op: &FuncCallType, table_id: TableId) {
        match self {
            Self::Mut(tx) => tx.record_table_scan(op, table_id),
            Self::Batch(tx) => tx.record_table_scan(op, table_id),
        }
    }
    fn record_index_scan_range(
        &mut self,
        op: &FuncCallType,
        table_id: TableId,
        index_id: IndexId,
        point: Option<IndexKey<'_>>,
    ) {
        match self {
            Self::Mut(tx) => tx.record_index_scan_range(op, table_id, index_id, point),
            Self::Batch(tx) => tx.record_index_scan_range(op, table_id, index_id, point),
        }
    }
    fn record_index_scan_point(
        &mut self,
        op: &FuncCallType,
        table_id: TableId,
        index_id: IndexId,
        point: IndexKey<'_>,
    ) {
        match self {
            Self::Mut(tx) => tx.record_index_scan_point(op, table_id, index_id, point),
            Self::Batch(tx) => tx.record_index_scan_point(op, table_id, index_id, point),
        }
    }
    fn record_table_write(&mut self, op: &FuncCallType, table_id: TableId) {
        match self {
            Self::Mut(tx) => tx.record_table_write(op, table_id),
            Self::Batch(tx) => tx.record_table_write(op, table_id),
        }
    }
    fn record_index_write(&mut self, op: &FuncCallType, index_id: IndexId) {
        match self {
            Self::Mut(tx) => tx.record_index_write(op, index_id),
            Self::Batch(tx) => tx.record_index_write(op, index_id),
        }
    }
    fn enable_access_capture(&mut self) {
        match self {
            Self::Mut(tx) => tx.enable_access_capture(),
            Self::Batch(tx) => tx.enable_access_capture(),
        }
    }
    fn take_observed(&mut self) -> Option<Box<ObservedAccess>> {
        match self {
            Self::Mut(tx) => tx.take_observed(),
            Self::Batch(tx) => tx.take_observed(),
        }
    }
    fn replace_view_read_set(&mut self, call: ViewCallInfo) {
        match self {
            Self::Mut(tx) => tx.replace_view_read_set(call),
            Self::Batch(tx) => tx.replace_view_read_set(call),
        }
    }
}
