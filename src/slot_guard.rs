//! State store wrapper that stops etl from silently replacing a lost apply slot.
//!
//! When etl finds the apply slot missing, it deletes the apply worker's durable progress and
//! creates a new slot at the current WAL position, even if the events table already finished its
//! initial sync. It only logs a warning, and every event committed while the slot was missing is
//! skipped. The apply worker also does this when it reconnects after a connection error within a
//! running pipeline, e.g. after a database failover to a standby without the slot.
//!
//! [`SlotGuardStore`] fails that progress deletion instead, before the new slot is created. The
//! pipeline then stops with an error and [`crate::core`] recovers the slot with
//! [`crate::slot_recovery::handle_slot_recovery`], which replays the missed events.

use std::collections::BTreeMap;
use std::sync::Arc;

use etl::destination::DestinationTableMetadata;
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use etl::schema::{PgLsn, SnapshotId, TableId, TableSchema};
use etl::store::{
    CachedStore, SchemaStore, StateStore, TableState, TableStateLifecycleStore,
    TableStateOperation, TableStates, WorkerType,
};

/// Wraps an etl state store and rejects replacing the apply slot of a synced pipeline.
///
/// See the module docs.
#[derive(Debug, Clone)]
pub struct SlotGuardStore<S> {
    inner: S,
}

impl<S> SlotGuardStore<S> {
    pub fn new(inner: S) -> Self {
        Self { inner }
    }
}

impl<S: CachedStore + Sync> CachedStore for SlotGuardStore<S> {
    async fn load_cache(&self) -> EtlResult<()> {
        self.inner.load_cache().await
    }
}

impl<S: StateStore + Sync> StateStore for SlotGuardStore<S> {
    async fn get_table_state(&self, table_id: TableId) -> EtlResult<Option<TableState>> {
        self.inner.get_table_state(table_id).await
    }

    async fn get_table_states(&self) -> EtlResult<TableStates> {
        self.inner.get_table_states().await
    }

    async fn update_table_states(&self, updates: Vec<(TableId, TableState)>) -> EtlResult<()> {
        self.inner.update_table_states(updates).await
    }

    async fn rollback_table_state(&self, table_id: TableId) -> EtlResult<TableState> {
        self.inner.rollback_table_state(table_id).await
    }

    async fn get_replication_checkpoint(
        &self,
        worker_type: WorkerType,
    ) -> EtlResult<Option<PgLsn>> {
        self.inner.get_replication_checkpoint(worker_type).await
    }

    async fn upsert_replication_checkpoint(
        &self,
        worker_type: WorkerType,
        checkpoint_lsn: PgLsn,
    ) -> EtlResult<PgLsn> {
        self.inner
            .upsert_replication_checkpoint(worker_type, checkpoint_lsn)
            .await
    }

    /// Fails instead of deleting the apply progress while a table finished its initial sync.
    ///
    /// etl deletes the apply progress right before it creates a new apply slot. For a synced
    /// table, the new slot would skip everything committed since the previous slot was lost.
    async fn delete_replication_checkpoint(&self, worker_type: WorkerType) -> EtlResult<()> {
        if matches!(worker_type, WorkerType::Apply) {
            let table_states = self.inner.get_table_states().await?;
            if table_states
                .values()
                .any(|state| state.as_type().has_completed_table_sync())
            {
                return Err(etl_error!(
                    ErrorKind::ReplicationSlotNotFound,
                    "Apply replication slot is missing",
                    "The apply slot was lost after the events table finished its initial sync. \
                     pgstream recovers the slot and replays the missed events on restart."
                ));
            }
        }

        self.inner.delete_replication_checkpoint(worker_type).await
    }

    async fn get_destination_table_metadata(
        &self,
        table_id: TableId,
    ) -> EtlResult<Option<DestinationTableMetadata>> {
        self.inner.get_destination_table_metadata(table_id).await
    }

    async fn store_destination_table_metadata(
        &self,
        table_id: TableId,
        metadata: DestinationTableMetadata,
    ) -> EtlResult<()> {
        self.inner
            .store_destination_table_metadata(table_id, metadata)
            .await
    }
}

impl<S: SchemaStore + Sync> SchemaStore for SlotGuardStore<S> {
    async fn get_table_schema(
        &self,
        table_id: &TableId,
        snapshot_id: SnapshotId,
    ) -> EtlResult<Option<Arc<TableSchema>>> {
        self.inner.get_table_schema(table_id, snapshot_id).await
    }

    async fn get_table_schemas(&self) -> EtlResult<Vec<Arc<TableSchema>>> {
        self.inner.get_table_schemas().await
    }

    async fn store_table_schema(&self, table_schema: TableSchema) -> EtlResult<Arc<TableSchema>> {
        self.inner.store_table_schema(table_schema).await
    }

    async fn prune_table_schemas(
        &self,
        retention_snapshot_ids: BTreeMap<TableId, SnapshotId>,
    ) -> EtlResult<u64> {
        self.inner.prune_table_schemas(retention_snapshot_ids).await
    }
}

impl<S: TableStateLifecycleStore + Sync> TableStateLifecycleStore for SlotGuardStore<S> {
    async fn apply_table_state_operation(&self, operation: TableStateOperation) -> EtlResult<()> {
        self.inner.apply_table_state_operation(operation).await
    }
}
