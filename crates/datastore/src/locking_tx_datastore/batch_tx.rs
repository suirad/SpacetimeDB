use super::{
    committed_state::CommittedState,
    datastore::{Result, TxMetrics},
    mut_tx::{
        delete, get_next_sequence_value, insert, update, FuncCallType, IndexScanPoint,
        MutTxId, ObservedAccess, RowRefInsertion, ViewCallInfo, ViewReadSets,
    },
    sequence::SequencesState,
    state_view::{IterByColEqMutTx, IterByColRangeMutTx, IterMutTx, StateView},
    tx_state::TxState,
    SharedReadGuard,
};
use crate::{
    execution_context::ExecutionContext,
    traits::InsertFlags,
};
use core::ops::RangeBounds;
use parking_lot::Mutex;
use spacetimedb_lib::metrics::ExecutionMetrics;
use spacetimedb_primitives::{ColList, IndexId, SequenceId, TableId};
use spacetimedb_sats::AlgebraicValue;
use spacetimedb_schema::reducer_name::ReducerName;
use spacetimedb_table::{
    indexes::RowPointer,
    table::{RowRef, TableAndIndex},
    table_index::IndexKey,
};
use std::{sync::Arc, time::{Duration, Instant}};

/// `BatchTxState` is the transactional state for a single reducer running inside a
/// concurrent batch. Unlike `MutTxId` it holds a **read** guard (not write) on the
/// committed state and stores the `SequencesState` as an `Arc<Mutex<…>>` so that
/// the struct is `Send`. The write guard and the sequence guard are only acquired
/// momentarily (per-call for sequences; via `commit_batch_tx` for the committed
/// state after all overlays finish).
pub struct BatchTxState {
    pub(super) tx_state: TxState,
    pub(super) committed_state_read_lock: SharedReadGuard<CommittedState>,
    pub(super) sequence_state: Arc<Mutex<SequencesState>>,
    pub(super) read_sets: ViewReadSets,
    pub(super) lock_wait_time: Duration,
    pub timer: Instant,
    pub ctx: ExecutionContext,
    pub metrics: ExecutionMetrics,
    pub(super) observed: Option<Box<ObservedAccess>>,
}

/// `FinishedBatchTx` is the result of calling [`BatchTxState::finish`].
/// It carries the accumulated state after the read guard has been dropped,
/// so the serialising `commit_batch_tx` can acquire the write lock without deadlock.
pub struct FinishedBatchTx {
    pub(super) tx_state: TxState,
    pub(super) read_sets: ViewReadSets,
    pub(super) lock_wait_time: Duration,
    pub timer: Instant,
    pub ctx: ExecutionContext,
    pub metrics: ExecutionMetrics,
}

// Safety: BatchTxState deliberately omits the `_not_send: PhantomData<Rc<()>>` present
// on MutTxId. SharedReadGuard<CommittedState> is Send (parking_lot send_guard feature).
// Arc<Mutex<SequencesState>> is Send. All other fields are Send.
const _: fn() = || {
    fn assert_send<T: Send>() {}
    assert_send::<BatchTxState>();
};

impl BatchTxState {
    // -------------------------------------------------------------------------
    // Insert / delete / sequence
    // -------------------------------------------------------------------------

    /// Insert a BSATN-encoded row with auto-increment generation.
    pub fn insert<'a, const GENERATE: bool>(
        &'a mut self,
        table_id: TableId,
        row: &[u8],
    ) -> Result<(ColList, RowRefInsertion<'a>, InsertFlags)> {
        let mut seq = self.sequence_state.lock();
        insert::<GENERATE>(&mut self.tx_state, &self.committed_state_read_lock, &mut seq, table_id, row)
    }

    /// Delete a row by its `RowPointer`.
    pub fn delete(&mut self, table_id: TableId, row_pointer: RowPointer) -> Result<bool> {
        delete(&mut self.tx_state, &self.committed_state_read_lock, table_id, row_pointer)
    }

    /// Clear all rows from `table_id`. Keep in sync with `MutTxId::clear_table`.
    pub fn clear_table(&mut self, table_id: TableId) -> Result<u64> {
        let (commit_table, commit_bs, ..) = self.committed_state_read_lock.get_table_and_blob_store(table_id)?;

        let (tx_table, tx_blob_store, delete_table) = self
            .tx_state
            .get_table_and_blob_store_or_create_from(table_id, commit_table);
        let mut rows_removed = tx_table.clear(tx_blob_store);

        for row in commit_table.scan_rows(commit_bs) {
            delete_table.insert(row.pointer());
            rows_removed += 1;
        }

        Ok(rows_removed)
    }

    /// Get the next value for sequence `seq_id`.
    pub fn get_next_sequence_value(&mut self, seq_id: SequenceId) -> Result<i128> {
        let mut seq = self.sequence_state.lock();
        get_next_sequence_value(&mut self.tx_state, &self.committed_state_read_lock, &mut seq, seq_id)
    }

    /// Update a row via a unique index.
    pub fn update<'a>(
        &'a mut self,
        table_id: TableId,
        index_id: IndexId,
        row: &[u8],
    ) -> Result<(ColList, RowRefInsertion<'a>, crate::traits::UpdateFlags)> {
        let mut seq = self.sequence_state.lock();
        update(&mut self.tx_state, &self.committed_state_read_lock, &mut seq, table_id, index_id, row)
    }

    /// Get a row by pointer. Keep in sync with `MutTxId::get`.
    pub fn get(&self, table_id: TableId, row_ptr: RowPointer) -> Result<Option<RowRef<'_>>> {
        use spacetimedb_table::indexes::SquashedOffset;
        use crate::error::TableError;
        use crate::system_tables::SystemTable;
        if self.table_name(table_id).is_none() {
            return Err(TableError::IdNotFound(SystemTable::st_table, table_id.0).into());
        }
        Ok(match row_ptr.squashed_offset() {
            SquashedOffset::TX_STATE => Some(self.tx_state.get(table_id, row_ptr)),
            SquashedOffset::COMMITTED_STATE => {
                if self.tx_state.is_deleted(table_id, row_ptr) {
                    None
                } else {
                    Some(self.committed_state_read_lock.get(table_id, row_ptr))
                }
            }
            _ => unreachable!("Invalid SquashedOffset for row pointer: {:?}", row_ptr),
        })
    }

    /// Delete by matching row value. Keep in sync with `MutTxId::delete_by_row_value`.
    pub fn delete_by_row_value(
        &mut self,
        table_id: TableId,
        rel: &spacetimedb_sats::ProductValue,
    ) -> Result<bool> {
        use spacetimedb_table::table::Table;
        let page_pool = &self.committed_state_read_lock.page_pool;
        let (commit_table, ..) = self.committed_state_read_lock.get_table_and_blob_store(table_id)?;
        let (tx_table, tx_blob_store, _) = self
            .tx_state
            .get_table_and_blob_store_or_create_from(table_id, commit_table);

        let (temp_row_ref, _) = tx_table.insert_physically_pv(page_pool, tx_blob_store, rel)?;
        let temp_ptr = temp_row_ref.pointer();

        let (hash, to_delete) = unsafe { Table::find_same_row(commit_table, tx_table, tx_blob_store, temp_ptr, None) };
        let to_delete = to_delete.or_else(|| {
            let (_, to_delete) = unsafe { Table::find_same_row(tx_table, tx_table, tx_blob_store, temp_ptr, hash) };
            to_delete
        });

        unsafe { tx_table.delete_internal_skip_pointer_map(tx_blob_store, temp_ptr) };

        to_delete
            .map(|ptr| self.delete(table_id, ptr))
            .unwrap_or(Ok(false))
    }

    // -------------------------------------------------------------------------
    // Index scans
    // -------------------------------------------------------------------------

    pub fn index_scan_point<'a, 'p>(
        &'a self,
        index_id: IndexId,
        point: &'p [u8],
    ) -> Result<(TableId, IndexKey<'p>, IndexScanPoint<'a>)> {
        use crate::error::IndexError;
        let (table_id, commit_index, tx_index) = self
            .get_table_and_index(index_id)
            .ok_or_else(|| IndexError::NotFound(index_id))?;
        let point = commit_index.index().key_from_bsatn(point).map_err(IndexError::Decode)?;
        let iter = MutTxId::index_scan_point_inner(&self.tx_state, table_id, tx_index, commit_index, &point);
        Ok((table_id, point, iter))
    }

    pub fn index_scan_range<'de, 'a>(
        &'a self,
        index_id: IndexId,
        prefix: &'de [u8],
        prefix_elems: spacetimedb_primitives::ColId,
        rstart: &'de [u8],
        rend: &'de [u8],
    ) -> Result<(TableId, super::mut_tx::IndexScanPointOrRange<'de, 'a>)>
    where
        'de: 'a,
    {
        use crate::error::IndexError;
        use spacetimedb_table::table_index::PointOrRange;
        use super::mut_tx::IndexScanPointOrRange;
        let (table_id, commit_index, tx_index) = self
            .get_table_and_index(index_id)
            .ok_or_else(|| IndexError::NotFound(index_id))?;
        let bounds = commit_index
            .index()
            .bounds_from_bsatn(prefix, prefix_elems, rstart, rend)
            .map_err(IndexError::Decode)?;
        let iter = match bounds {
            PointOrRange::Point(point) => {
                let iter = MutTxId::index_scan_point_inner(&self.tx_state, table_id, tx_index, commit_index, &point);
                IndexScanPointOrRange::Point(point, iter)
            }
            PointOrRange::Range(start, end) => {
                let bounds = (start.as_ref(), end.as_ref());
                let iter = MutTxId::index_scan_range_inner(&self.tx_state, table_id, tx_index, commit_index, &bounds)
                    .map_err(|_| IndexError::IndexCannotSeekRange(index_id))?;
                IndexScanPointOrRange::Range(iter)
            }
            PointOrRange::Unsupported => return Err(IndexError::IndexCannotSeekRange(index_id).into()),
        };
        Ok((table_id, iter))
    }

    fn get_table_and_index(
        &self,
        index_id: IndexId,
    ) -> Option<(TableId, TableAndIndex<'_>, Option<TableAndIndex<'_>>)> {
        let table_id = self.committed_state_read_lock.get_table_for_index(index_id)?;
        let commit_index = self
            .committed_state_read_lock
            .get_index_by_id_with_table(table_id, index_id)?;
        let tx_index = self.tx_state.get_index_by_id_with_table(table_id, index_id);
        Some((table_id, commit_index, tx_index))
    }

    // -------------------------------------------------------------------------
    // Record hooks — keep in sync with MutTxId's
    // -------------------------------------------------------------------------

    pub fn record_table_scan(&mut self, op: &FuncCallType, table_id: TableId) {
        if let FuncCallType::View(view) = op {
            self.read_sets.insert_full_table_scan(table_id, view.clone());
        } else if matches!(op, FuncCallType::Reducer) && self.observed.is_some() {
            record_observed_read_inner(&mut self.observed, table_id);
        }
    }

    pub fn record_index_scan_range(
        &mut self,
        op: &FuncCallType,
        table_id: TableId,
        index_id: IndexId,
        point: Option<IndexKey<'_>>,
    ) {
        if let FuncCallType::View(view) = op {
            if let Some(point) = point {
                record_index_scan_point_inner_batch(self, view, table_id, index_id, point);
            } else {
                self.read_sets.insert_full_table_scan(table_id, view.clone());
            }
        } else if matches!(op, FuncCallType::Reducer) && self.observed.is_some() {
            record_observed_read_inner(&mut self.observed, table_id);
        }
    }

    pub fn record_index_scan_point(
        &mut self,
        op: &FuncCallType,
        table_id: TableId,
        index_id: IndexId,
        point: IndexKey<'_>,
    ) {
        if let FuncCallType::View(view) = op {
            record_index_scan_point_inner_batch(self, view, table_id, index_id, point);
        } else if matches!(op, FuncCallType::Reducer) && self.observed.is_some() {
            record_observed_read_inner(&mut self.observed, table_id);
        }
    }

    pub fn record_table_write(&mut self, op: &FuncCallType, table_id: TableId) {
        if matches!(op, FuncCallType::Reducer) && self.observed.is_some() {
            record_observed_write_inner(&mut self.observed, table_id);
        }
    }

    pub fn record_index_write(&mut self, op: &FuncCallType, index_id: IndexId) {
        if matches!(op, FuncCallType::Reducer) && self.observed.is_some()
            && let Some((table_id, _, _)) = self.get_table_and_index(index_id)
        {
            record_observed_write_inner(&mut self.observed, table_id);
        }
    }

    pub fn enable_access_capture(&mut self) {
        self.observed = Some(Box::default());
    }

    pub fn take_observed(&mut self) -> Option<Box<ObservedAccess>> {
        self.observed.take()
    }

    pub fn replace_view_read_set(&mut self, call: ViewCallInfo) {
        self.read_sets.replace_view_read_set(call);
    }

    // -------------------------------------------------------------------------
    // Lifecycle
    // -------------------------------------------------------------------------

    /// Drop the read guard and return the state needed for serial commit.
    /// Must be called before any sibling `BatchTxState`s are committed.
    pub fn finish(self) -> FinishedBatchTx {
        FinishedBatchTx {
            tx_state: self.tx_state,
            read_sets: self.read_sets,
            lock_wait_time: self.lock_wait_time,
            timer: self.timer,
            ctx: self.ctx,
            metrics: self.metrics,
        }
    }

    /// Returns true iff any table in `tables` has a live view read-set entry,
    /// evaluated under this batch's own committed-state read window so the check
    /// races no concurrent writer.
    pub fn view_read_overlap(&self, tables: impl Iterator<Item = TableId>) -> bool {
        self.committed_state_read_lock.view_read_overlap(tables)
    }

    /// Discard the transaction and return metrics.
    pub fn rollback(self) -> (TxMetrics, Option<ReducerName>) {
        debug_assert!(
            self.tx_state.pending_schema_changes.is_empty(),
            "BatchTxState must not perform DDL"
        );

        let tx_metrics = TxMetrics::new(
            &self.ctx,
            self.timer,
            self.lock_wait_time,
            self.metrics,
            false,
            None,
            &self.committed_state_read_lock,
        );
        let reducer = self.ctx.into_reducer_name();
        (tx_metrics, reducer)
    }
}

// -------------------------------------------------------------------------
// Shared helpers (extracted to avoid borrow-checker issues with &mut self)
// -------------------------------------------------------------------------

#[cold]
#[inline(never)]
fn record_observed_read_inner(observed: &mut Option<Box<ObservedAccess>>, table_id: TableId) {
    if let Some(obs) = observed {
        obs.reads.insert(table_id);
    }
}

#[cold]
#[inline(never)]
fn record_observed_write_inner(observed: &mut Option<Box<ObservedAccess>>, table_id: TableId) {
    if let Some(obs) = observed {
        obs.writes.insert(table_id);
    }
}

#[cold]
#[inline(never)]
fn record_index_scan_point_inner_batch(
    btx: &mut BatchTxState,
    view: &ViewCallInfo,
    table_id: TableId,
    index_id: IndexId,
    point: IndexKey<'_>,
) {
    let Some((_, idx, _)) = btx.get_table_and_index(index_id) else { return };
    let idx = idx.index();
    let cols = idx.indexed_columns().clone();
    let point_av = idx.key_into_algebraic_value(point);
    btx.read_sets.insert_index_scan(table_id, cols, point_av, view.clone());
}

// -------------------------------------------------------------------------
// StateView impl for BatchTxState
// -------------------------------------------------------------------------

impl StateView for BatchTxState {
    type Iter<'a> = IterMutTx<'a>;
    type IterByColRange<'a, R: RangeBounds<AlgebraicValue>> = IterByColRangeMutTx<'a, R>;
    type IterByColEq<'a, 'r>
        = IterByColEqMutTx<'a, 'r>
    where
        Self: 'a;

    fn get_schema(&self, table_id: TableId) -> Option<&std::sync::Arc<spacetimedb_schema::schema::TableSchema>> {
        self.committed_state_read_lock.get_schema(table_id)
    }

    fn table_row_count(&self, table_id: TableId) -> Option<u64> {
        super::mut_tx::table_row_count(&self.tx_state, &self.committed_state_read_lock, table_id)
    }

    fn iter(&self, table_id: TableId) -> Result<Self::Iter<'_>> {
        super::mut_tx::iter(&self.tx_state, &self.committed_state_read_lock, table_id)
    }

    fn iter_by_col_range<R: RangeBounds<AlgebraicValue>>(
        &self,
        table_id: TableId,
        cols: ColList,
        range: R,
    ) -> Result<Self::IterByColRange<'_, R>> {
        super::mut_tx::iter_by_col_range(&self.tx_state, &self.committed_state_read_lock, table_id, cols, range)
    }

    fn iter_by_col_eq<'r>(
        &self,
        table_id: TableId,
        cols: impl Into<ColList>,
        value: &'r AlgebraicValue,
    ) -> Result<Self::IterByColEq<'_, 'r>> {
        super::mut_tx::iter_by_col_eq(&self.tx_state, &self.committed_state_read_lock, table_id, cols, value)
    }
}

// -------------------------------------------------------------------------
// Tests
// -------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::locking_tx_datastore::datastore::Locking;
    use crate::traits::{IsolationLevel, MutTx};
    use spacetimedb_lib::db::auth::{StAccess, StTableType};
    use spacetimedb_sats::{product, AlgebraicType};
    use spacetimedb_schema::def::BTreeAlgorithm;
    use spacetimedb_schema::identifier::Identifier;
    use spacetimedb_schema::schema::{ColumnSchema, ConstraintSchema, IndexSchema, SequenceSchema, TableSchema};
    use spacetimedb_schema::table_name::TableName;
    use spacetimedb_table::page_pool::PagePool;
    use std::sync::Arc;

    fn get_datastore() -> crate::Result<Locking> {
        Locking::bootstrap(spacetimedb_lib::Identity::ZERO, PagePool::new_for_test())
    }

    fn simple_schema(name: &str) -> TableSchema {
        TableSchema::new(
            spacetimedb_primitives::TableId::SENTINEL,
            TableName::for_test(name),
            None,
            vec![ColumnSchema {
                table_id: spacetimedb_primitives::TableId::SENTINEL,
                col_pos: 0.into(),
                col_name: Identifier::for_test("val"),
                col_type: AlgebraicType::U32,
                alias: None,
            }],
            vec![],
            vec![],
            vec![],
            StTableType::User,
            StAccess::Public,
            None,
            None,
            false,
            None,
        )
    }

    fn schema_with_autoinc(name: &str) -> TableSchema {
        let table_id = spacetimedb_primitives::TableId::SENTINEL;
        let seq = SequenceSchema {
            sequence_id: spacetimedb_primitives::SequenceId::SENTINEL,
            sequence_name: format!("{name}_seq").into(),
            table_id,
            col_pos: 0.into(),
            increment: 1,
            start: 1,
            min_value: 1,
            max_value: i128::MAX,
        };
        TableSchema::new(
            table_id,
            TableName::for_test(name),
            None,
            vec![ColumnSchema {
                table_id,
                col_pos: 0.into(),
                col_name: Identifier::for_test("id"),
                col_type: AlgebraicType::I64,
                alias: None,
            }],
            vec![],
            vec![],
            vec![seq],
            StTableType::User,
            StAccess::Public,
            None,
            None,
            false,
            None,
        )
    }

    /// Two-column schema: col 0 = i64 autoinc+unique (update key), col 1 = u32 (payload).
    fn schema_with_autoinc_unique(name: &str) -> TableSchema {
        let table_id = spacetimedb_primitives::TableId::SENTINEL;
        let seq = SequenceSchema {
            sequence_id: spacetimedb_primitives::SequenceId::SENTINEL,
            sequence_name: format!("{name}_id_seq").into(),
            table_id,
            col_pos: 0.into(),
            increment: 1,
            start: 1,
            min_value: 1,
            max_value: i128::MAX,
        };
        let idx = IndexSchema::for_test(format!("{name}_id_idx"), BTreeAlgorithm::from(0u16));
        let constraint = ConstraintSchema::unique_for_test(format!("{name}_id_key"), 0u16);
        TableSchema::new(
            table_id,
            TableName::for_test(name),
            None,
            vec![
                ColumnSchema {
                    table_id,
                    col_pos: 0.into(),
                    col_name: Identifier::for_test("id"),
                    col_type: AlgebraicType::I64,
                    alias: None,
                },
                ColumnSchema {
                    table_id,
                    col_pos: 1.into(),
                    col_name: Identifier::for_test("val"),
                    col_type: AlgebraicType::U32,
                    alias: None,
                },
            ],
            vec![idx],
            vec![constraint],
            vec![seq],
            StTableType::User,
            StAccess::Public,
            None,
            None,
            false,
            None,
        )
    }

    // -------------------------------------------------------------------------
    // 1. Compile-time Send check
    // -------------------------------------------------------------------------
    #[test]
    fn batch_tx_state_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<BatchTxState>();
    }

    // -------------------------------------------------------------------------
    // 2. Equivalence: MutTxId vs BatchTxState produce identical committed state
    //    after insert / update / delete / clear_table + read-your-own-writes.
    // -------------------------------------------------------------------------
    #[test]
    fn equivalence_mut_vs_batch() -> crate::Result<()> {
        use crate::execution_context::Workload;

        let workload = Workload::Internal;

        // Helper: set up a fresh datastore with the two-column autoinc+unique table
        // plus a small auxiliary table for clear_table, then return (ds, tid_main, tid_aux).
        let setup_ds = || -> crate::Result<(Locking, spacetimedb_primitives::TableId, spacetimedb_primitives::TableId)> {
            let ds = get_datastore()?;
            let mut tx = ds.begin_mut_tx(IsolationLevel::Serializable, workload.clone());
            tx.create_table(schema_with_autoinc_unique("tbl"))?;
            tx.create_table(simple_schema("aux"))?;
            ds.commit_mut_tx(tx)?;
            let rtx = ds.begin_mut_tx(IsolationLevel::Serializable, workload.clone());
            let tid = rtx.table_id_from_name("tbl")?.unwrap();
            let aux = rtx.table_id_from_name("aux")?.unwrap();
            let _ = ds.rollback_mut_tx(rtx);
            Ok((ds, tid, aux))
        };

        // Seed rows to work with (inserted and committed before the test tx).
        // Returns (ds, tid, aux, index_id, seed_row_ids).
        let seed = |ds: &Locking, tid: spacetimedb_primitives::TableId, aux: spacetimedb_primitives::TableId| -> crate::Result<spacetimedb_primitives::IndexId> {
            // Insert several rows and two aux rows, commit.
            let mut tx = ds.begin_mut_tx(IsolationLevel::Serializable, workload.clone());
            for v in [10u32, 20, 30] {
                let zero_row = spacetimedb_sats::bsatn::to_vec(&product![0i64, v]).unwrap();
                tx.insert::<true>(tid, &zero_row)?;
            }
            for v in [1u32, 2] {
                let row = spacetimedb_sats::bsatn::to_vec(&product![v]).unwrap();
                tx.insert::<true>(aux, &row)?;
            }
            ds.commit_mut_tx(tx)?;

            // Retrieve the index id for col 0 of `tbl`.
            let rtx = ds.begin_mut_tx(IsolationLevel::Serializable, workload.clone());
            let index_id = rtx
                .iter_by_col_eq(
                    crate::system_tables::ST_INDEX_ID,
                    crate::system_tables::StIndexFields::TableId,
                    &tid.into(),
                )?
                .next()
                .map(|r| r.read_col(crate::system_tables::StIndexFields::IndexId).unwrap())
                .unwrap();
            let _ = ds.rollback_mut_tx(rtx);
            Ok(index_id)
        };

        // ---- MutTxId path ----
        let (ds_m, tid_m, aux_m) = setup_ds()?;
        let idx_m = seed(&ds_m, tid_m, aux_m)?;

        // Phase 1: commit seed is done.  Now run the op sequence in a single mut tx.
        let mut mtx = ds_m.begin_mut_tx(IsolationLevel::Serializable, workload.clone());

        // (a) insert two more rows
        for v in [40u32, 50] {
            let zero_row = spacetimedb_sats::bsatn::to_vec(&product![0i64, v]).unwrap();
            mtx.insert::<true>(tid_m, &zero_row)?;
        }

        // (b) update the row with val==10 (id==1) to val==99
        let update_row_m = spacetimedb_sats::bsatn::to_vec(&product![1i64, 99u32]).unwrap();
        mtx.update(tid_m, idx_m, &update_row_m)?;

        // (c) delete a committed row: find pointer of val==20 row by scanning
        let ptr_to_del_m = mtx
            .iter(tid_m)?
            .find(|r| r.to_product_value().elements[1] == spacetimedb_sats::AlgebraicValue::U32(20))
            .map(|r| r.pointer())
            .unwrap();
        mtx.delete(tid_m, ptr_to_del_m)?;

        // (d) clear_table on aux
        mtx.clear_table(aux_m)?;

        // (e) read-your-own-writes: row with val==40 must be visible via iter
        let saw_40_m = mtx
            .iter(tid_m)?
            .any(|r| r.to_product_value().elements[1] == spacetimedb_sats::AlgebraicValue::U32(40));
        assert!(saw_40_m, "MutTx: read-your-own-writes failed for val==40");

        // (f) iter_by_col_eq finds the updated row (val==99)
        let found_updated_m = mtx
            .iter(tid_m)?
            .any(|r| r.to_product_value().elements[1] == spacetimedb_sats::AlgebraicValue::U32(99));
        assert!(found_updated_m, "MutTx: updated row val==99 not found");

        let (_, mut_tx_data, _, _) = ds_m.commit_mut_tx(mtx)?.unwrap();

        // Collect final committed rows
        let rtx_m = ds_m.begin_mut_tx(IsolationLevel::Serializable, workload.clone());
        let mut mut_rows: Vec<spacetimedb_sats::ProductValue> =
            rtx_m.iter(tid_m)?.map(|r| r.to_product_value()).collect();
        let mut mut_aux: Vec<spacetimedb_sats::ProductValue> =
            rtx_m.iter(aux_m)?.map(|r| r.to_product_value()).collect();
        let _ = ds_m.rollback_mut_tx(rtx_m);

        // ---- BatchTxState path ----
        let (ds_b, tid_b, aux_b) = setup_ds()?;
        let idx_b = seed(&ds_b, tid_b, aux_b)?;

        let mut btx = ds_b.begin_batch_tx(workload.clone());

        // (a) insert two more rows
        for v in [40u32, 50] {
            let zero_row = spacetimedb_sats::bsatn::to_vec(&product![0i64, v]).unwrap();
            btx.insert::<true>(tid_b, &zero_row)?;
        }

        // (b) update val==10 row (id==1) to val==99
        let update_row_b = spacetimedb_sats::bsatn::to_vec(&product![1i64, 99u32]).unwrap();
        btx.update(tid_b, idx_b, &update_row_b)?;

        // (c) delete committed row with val==20
        let ptr_to_del_b = btx
            .iter(tid_b)?
            .find(|r| r.to_product_value().elements[1] == spacetimedb_sats::AlgebraicValue::U32(20))
            .map(|r| r.pointer())
            .unwrap();
        btx.delete(tid_b, ptr_to_del_b)?;

        // (d) clear_table on aux
        btx.clear_table(aux_b)?;

        // (e) read-your-own-writes
        let saw_40_b = btx
            .iter(tid_b)?
            .any(|r| r.to_product_value().elements[1] == spacetimedb_sats::AlgebraicValue::U32(40));
        assert!(saw_40_b, "BatchTx: read-your-own-writes failed for val==40");

        // (f) iter finds updated row
        let found_updated_b = btx
            .iter(tid_b)?
            .any(|r| r.to_product_value().elements[1] == spacetimedb_sats::AlgebraicValue::U32(99));
        assert!(found_updated_b, "BatchTx: updated row val==99 not found");

        let finished = btx.finish();
        let (_, batch_tx_data, _, _) = ds_b.commit_batch_tx(finished)?;

        let rtx_b = ds_b.begin_mut_tx(IsolationLevel::Serializable, workload.clone());
        let mut batch_rows: Vec<spacetimedb_sats::ProductValue> =
            rtx_b.iter(tid_b)?.map(|r| r.to_product_value()).collect();
        let mut batch_aux: Vec<spacetimedb_sats::ProductValue> =
            rtx_b.iter(aux_b)?.map(|r| r.to_product_value()).collect();
        let _ = ds_b.rollback_mut_tx(rtx_b);

        // Sort for stable comparison
        let sort_rows = |rows: &mut Vec<spacetimedb_sats::ProductValue>| {
            rows.sort_by_key(|r| format!("{r:?}"));
        };
        sort_rows(&mut mut_rows);
        sort_rows(&mut batch_rows);
        sort_rows(&mut mut_aux);
        sort_rows(&mut batch_aux);

        assert_eq!(mut_rows, batch_rows, "final committed row sets must match");
        assert_eq!(mut_aux, batch_aux, "aux table (after clear) must match");

        // TxData inserts and deletes must match per table, keyed by table name so that
        // differing TableIds across the two independent datastores don't cause false mismatches.
        let sort_pvs = |mut pvs: Vec<spacetimedb_sats::ProductValue>| {
            pvs.sort_by_key(|r| format!("{r:?}"));
            pvs
        };
        let collect_inserts_by_name = |tx_data: &crate::traits::TxData| {
            let mut map: std::collections::BTreeMap<String, Vec<spacetimedb_sats::ProductValue>> =
                std::collections::BTreeMap::new();
            for (table_id, rows) in tx_data.inserts() {
                let name = tx_data
                    .entry_for(table_id)
                    .map(|e| e.table_name.to_string())
                    .unwrap_or_else(|| format!("table_{}", table_id.0));
                map.entry(name).or_default().extend(rows.iter().cloned());
            }
            map.into_iter().map(|(k, v)| (k, sort_pvs(v))).collect::<std::collections::BTreeMap<_, _>>()
        };
        let collect_deletes_by_name = |tx_data: &crate::traits::TxData| {
            let mut map: std::collections::BTreeMap<String, Vec<spacetimedb_sats::ProductValue>> =
                std::collections::BTreeMap::new();
            for (table_id, rows) in tx_data.deletes() {
                let name = tx_data
                    .entry_for(table_id)
                    .map(|e| e.table_name.to_string())
                    .unwrap_or_else(|| format!("table_{}", table_id.0));
                map.entry(name).or_default().extend(rows.iter().cloned());
            }
            map.into_iter().map(|(k, v)| (k, sort_pvs(v))).collect::<std::collections::BTreeMap<_, _>>()
        };
        assert_eq!(
            collect_inserts_by_name(&mut_tx_data),
            collect_inserts_by_name(&batch_tx_data),
            "TxData inserted row values must match per table"
        );
        assert_eq!(
            collect_deletes_by_name(&mut_tx_data),
            collect_deletes_by_name(&batch_tx_data),
            "TxData deleted row values must match per table"
        );

        Ok(())
    }

    // -------------------------------------------------------------------------
    // 3. Sequence refill across boundary
    // -------------------------------------------------------------------------
    #[test]
    fn sequence_refill_across_batch_commit() -> crate::Result<()> {
        use crate::execution_context::Workload;
        use spacetimedb_lib::db::raw_def::SEQUENCE_ALLOCATION_STEP;

        let ds = get_datastore()?;
        let workload = Workload::Internal;

        // Create table with autoinc via MutTx DDL
        let mut setup = ds.begin_mut_tx(IsolationLevel::Serializable, workload.clone());
        setup.create_table(schema_with_autoinc("seq_tbl"))?;
        ds.commit_mut_tx(setup)?;

        // Insert enough rows via batch to force at least one refill
        let rows_to_insert = (SEQUENCE_ALLOCATION_STEP as usize) + 10;
        let mut last_val: Option<i64> = None;

        let rtx = ds.begin_mut_tx(IsolationLevel::Serializable, workload.clone());
        let tid = rtx.table_id_from_name("seq_tbl")?.unwrap();
        let _ = ds.rollback_mut_tx(rtx);

        for _ in 0..rows_to_insert {
            let mut btx = ds.begin_batch_tx(workload.clone());
            // Insert a zero placeholder; autoinc will fill it
            let zero_row = spacetimedb_sats::bsatn::to_vec(&product![0i64]).unwrap();
            let (_, row_ref, _) = btx.insert::<true>(tid, &zero_row)?;
            let inserted: i64 = row_ref.collapse().read_col(0u16)?;
            if let Some(prev) = last_val {
                assert!(inserted > prev, "sequence values must be strictly increasing: {prev} -> {inserted}");
            }
            last_val = Some(inserted);
            let finished = btx.finish();
            let _ = ds.commit_batch_tx(finished)?;
        }

        Ok(())
    }

    // -------------------------------------------------------------------------
    // 4. Two-thread overlay smoke test with delete coverage
    // -------------------------------------------------------------------------
    #[test]
    fn two_thread_overlay_smoke() -> crate::Result<()> {
        use crate::execution_context::Workload;

        let ds = Arc::new(get_datastore()?);
        let workload = Workload::Internal;

        // Schema setup: two tables A, B
        let mut setup = ds.begin_mut_tx(IsolationLevel::Serializable, workload.clone());
        setup.create_table(simple_schema("a"))?;
        setup.create_table(simple_schema("b"))?;
        ds.commit_mut_tx(setup)?;

        // Look up IDs
        let rtx = ds.begin_mut_tx(IsolationLevel::Serializable, workload.clone());
        let tid_a = rtx.table_id_from_name("a")?.unwrap();
        let tid_b = rtx.table_id_from_name("b")?.unwrap();
        let _ = ds.rollback_mut_tx(rtx);

        // Seed: insert one row into each table and commit.
        let mut seed = ds.begin_mut_tx(IsolationLevel::Serializable, workload.clone());
        seed.insert::<true>(tid_a, &spacetimedb_sats::bsatn::to_vec(&product![1u32]).unwrap())?;
        seed.insert::<true>(tid_b, &spacetimedb_sats::bsatn::to_vec(&product![2u32]).unwrap())?;
        ds.commit_mut_tx(seed)?;

        let row_a_new = spacetimedb_sats::bsatn::to_vec(&product![100u32]).unwrap();
        let row_b_new = spacetimedb_sats::bsatn::to_vec(&product![200u32]).unwrap();

        // Start two BatchTxStates — both hold read guards on the committed state.
        let btx1 = ds.begin_batch_tx(workload.clone());
        let btx2 = ds.begin_batch_tx(workload.clone());

        let ds1 = Arc::clone(&ds);
        let ds2 = Arc::clone(&ds);

        // Thread 1: find the seeded row in table A via iter, delete it, insert new row.
        let t1 = std::thread::spawn(move || -> crate::Result<FinishedBatchTx> {
            let mut btx = btx1;
            let old_ptr = btx.iter(tid_a)?.next().map(|r| r.pointer()).unwrap();
            btx.delete(tid_a, old_ptr)?;
            btx.insert::<true>(tid_a, &row_a_new)?;
            Ok(btx.finish())
        });

        // Thread 2: find the seeded row in table B via iter, delete it, insert new row.
        let t2 = std::thread::spawn(move || -> crate::Result<FinishedBatchTx> {
            let mut btx = btx2;
            let old_ptr = btx.iter(tid_b)?.next().map(|r| r.pointer()).unwrap();
            btx.delete(tid_b, old_ptr)?;
            btx.insert::<true>(tid_b, &row_b_new)?;
            Ok(btx.finish())
        });

        let f1 = t1.join().expect("thread 1 panicked")?;
        let f2 = t2.join().expect("thread 2 panicked")?;

        // FIFO commit; read guards already dropped by finish().
        let (off1, _, _, _) = ds1.commit_batch_tx(f1)?;
        let (off2, _, _, _) = ds2.commit_batch_tx(f2)?;

        assert!(off2 > off1, "tx offsets must be consecutive: {off1}, {off2}");

        // Each table must contain exactly the new row (old one deleted).
        let verify = ds.begin_mut_tx(IsolationLevel::Serializable, workload.clone());
        let rows_a: Vec<_> = verify.iter(tid_a)?.map(|r| r.to_product_value()).collect();
        let rows_b: Vec<_> = verify.iter(tid_b)?.map(|r| r.to_product_value()).collect();
        let _ = ds.rollback_mut_tx(verify);

        assert_eq!(rows_a, vec![product![100u32]], "table a must contain only the new row");
        assert_eq!(rows_b, vec![product![200u32]], "table b must contain only the new row");

        Ok(())
    }

    // -------------------------------------------------------------------------
    // 5. Commit-downgrade: BatchTxState vs MutTxId produce the same TxData rows
    //    and the returned TxId (read tx) can read the committed rows.
    // -------------------------------------------------------------------------
    #[test]
    fn commit_downgrade_equivalence() -> crate::Result<()> {
        use crate::execution_context::Workload;

        let workload = Workload::Internal;

        // Build two independent datastores and apply identical insert-then-commit
        // operations — one via MutTxId+commit_mut_tx_downgrade, one via
        // BatchTxState+commit_batch_tx_downgrade_and_then — then verify that the
        // returned TxIds can read the committed rows and that TxData matches.

        let setup = || -> crate::Result<(Locking, spacetimedb_primitives::TableId)> {
            let ds = get_datastore()?;
            let mut tx = ds.begin_mut_tx(IsolationLevel::Serializable, workload.clone());
            tx.create_table(simple_schema("tbl"))?;
            ds.commit_mut_tx(tx)?;
            let rtx = ds.begin_mut_tx(IsolationLevel::Serializable, workload.clone());
            let tid = rtx.table_id_from_name("tbl")?.unwrap();
            let _ = ds.rollback_mut_tx(rtx);
            Ok((ds, tid))
        };

        // ---- MutTxId path ----
        let (ds_m, tid_m) = setup()?;
        let mut mtx = ds_m.begin_mut_tx(IsolationLevel::Serializable, workload.clone());
        for v in [10u32, 20, 30] {
            let row = spacetimedb_sats::bsatn::to_vec(&product![v]).unwrap();
            mtx.insert::<true>(tid_m, &row)?;
        }
        let (mut_tx_data, _metrics_m, read_tx_m) = ds_m.commit_mut_tx_downgrade(mtx, workload.clone());

        // The returned read tx must see the committed rows.
        let count_m = read_tx_m.iter(tid_m)?.count();
        assert_eq!(count_m, 3, "MutTx downgrade: read tx must see 3 committed rows");
        drop(read_tx_m);

        // ---- BatchTxState path ----
        let (ds_b, tid_b) = setup()?;
        let mut btx = ds_b.begin_batch_tx(workload.clone());
        for v in [10u32, 20, 30] {
            let row = spacetimedb_sats::bsatn::to_vec(&product![v]).unwrap();
            btx.insert::<true>(tid_b, &row)?;
        }
        let finished = btx.finish();
        let (batch_tx_data, _metrics_b, read_tx_b) =
            ds_b.commit_batch_tx_downgrade_and_then(finished, workload.clone(), |_| {});

        // The returned read tx must see the committed rows.
        let count_b = read_tx_b.iter(tid_b)?.count();
        assert_eq!(count_b, 3, "BatchTx downgrade: read tx must see 3 committed rows");
        drop(read_tx_b);

        // TxData inserted rows must match (by value, keyed by table name).
        let sort_pvs = |mut v: Vec<spacetimedb_sats::ProductValue>| {
            v.sort_by_key(|r| format!("{r:?}"));
            v
        };
        let inserts_m: Vec<_> = sort_pvs(
            mut_tx_data
                .inserts()
                .flat_map(|(_, rows)| rows.iter().cloned())
                .collect(),
        );
        let inserts_b: Vec<_> = sort_pvs(
            batch_tx_data
                .inserts()
                .flat_map(|(_, rows)| rows.iter().cloned())
                .collect(),
        );
        assert_eq!(inserts_m, inserts_b, "TxData inserted row values must match");

        Ok(())
    }

    // -------------------------------------------------------------------------
    // 6. Tripwire smoke: a batch member writing a table with no read-set entry
    //    commits successfully (the admission debug_assert must not fire).
    // -------------------------------------------------------------------------
    #[test]
    fn tripwire_smoke_no_view_overlap() -> crate::Result<()> {
        use crate::execution_context::Workload;

        let workload = Workload::Internal;
        let ds = get_datastore()?;

        let mut setup = ds.begin_mut_tx(IsolationLevel::Serializable, workload.clone());
        setup.create_table(simple_schema("tbl"))?;
        ds.commit_mut_tx(setup)?;

        let rtx = ds.begin_mut_tx(IsolationLevel::Serializable, workload.clone());
        let tid = rtx.table_id_from_name("tbl")?.unwrap();
        let _ = ds.rollback_mut_tx(rtx);

        // No view subscriptions exist → no read-set entries → tripwire must not fire.
        let mut btx = ds.begin_batch_tx(workload.clone());
        let row = spacetimedb_sats::bsatn::to_vec(&product![42u32]).unwrap();
        btx.insert::<true>(tid, &row)?;
        let finished = btx.finish();

        // commit_batch_tx_downgrade_and_then contains the admission debug_assert;
        // if it fires the test panics.
        let (_tx_data, _metrics, read_tx) =
            ds.commit_batch_tx_downgrade_and_then(finished, workload, |_| {});

        let count = read_tx.iter(tid)?.count();
        assert_eq!(count, 1, "committed row must be visible via returned read tx");
        drop(read_tx);

        Ok(())
    }
}
