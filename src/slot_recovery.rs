//! Slot recovery logic for handling invalidated replication slots.
//!
//! When a Postgres replication slot is invalidated (WAL exceeded `max_slot_wal_keep_size`),
//! this module provides functionality to:
//! 1. Detect the slot invalidation error
//! 2. Query the `confirmed_flush_lsn` from the invalidated slot
//! 3. Find the first event after that LSN
//! 4. Set a failover checkpoint to trigger event replay
//! 5. Delete ETL replication state (triggers fresh slot creation)
//! 6. Drop the invalidated slot
//!
//! After recovery, ETL will create a new slot and run DataSync (table sync workers).
//! The stream's `write_table_rows()` returns `Ok(())` to skip DataSync entirely.
//! When the first replication event arrives with the failover checkpoint set,
//! `handle_failover()` will COPY the missed events from the events table.
//!
//! This design is crash-safe: if the system crashes after setting the checkpoint,
//! the checkpoint persists and will be used on restart regardless of slot state.

use chrono::{DateTime, Utc};
use etl::error::{EtlError, EtlResult};
use sqlx::PgPool;
use tracing::{info, warn};

use crate::types::SlotName;

/// Checks if an error indicates a replication slot has been invalidated.
///
/// Postgres returns error code 55000 (OBJECT_NOT_IN_PREREQUISITE_STATE) with the message
/// "can no longer get changes from replication slot" when a slot is invalidated.
#[must_use]
pub fn is_slot_invalidation_error(error: &EtlError) -> bool {
    let msg = error.to_string().to_lowercase();
    msg.contains("can no longer get changes from replication slot")
}

/// Handles recovery from an invalidated replication slot.
///
/// This function is crash-safe:
/// 1. Queries `confirmed_flush_lsn` from the invalidated slot
/// 2. Finds the first event with LSN > confirmed_flush_lsn
/// 3. Sets failover checkpoint in pgstream.streams (transactional)
/// 4. Commits the transaction (checkpoint is now durable)
/// 5. Drops the invalidated slot (non-transactional, done AFTER commit)
///
/// The slot drop is done after commit because `pg_drop_replication_slot` is not
/// transactional. By setting the checkpoint first, we ensure crash safety:
/// - If crash before commit: checkpoint not set, slot still exists, recovery reruns
/// - If crash after commit but before drop: checkpoint is saved, slot drop will happen on next recovery
///
/// After recovery, ETL will create a new slot and may trigger DataSync. The stream's
/// `tick()` function handles this by filtering events before the checkpoint.
///
/// After this function returns Ok, the pipeline should be restarted.
pub async fn handle_slot_recovery(pool: &PgPool, stream_id: u64) -> EtlResult<()> {
    let slot_name = stream_id.slot_name();
    info!(
        slot_name = slot_name,
        stream_id = stream_id,
        "attempting slot recovery"
    );

    // Start a transaction for the checkpoint update
    let mut tx = pool.begin().await?;

    // 1. Get confirmed_flush_lsn BEFORE dropping the slot
    let confirmed_lsn: Option<String> = sqlx::query_scalar(
        "SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(&slot_name)
    .fetch_optional(&mut *tx)
    .await?
    .flatten();

    let Some(lsn) = confirmed_lsn else {
        warn!(
            slot_name = slot_name,
            "slot not found or has no confirmed_flush_lsn, will restart without checkpoint"
        );
        tx.rollback().await?;
        return Ok(());
    };

    info!(
        slot_name = slot_name,
        confirmed_flush_lsn = %lsn,
        "found confirmed_flush_lsn from invalidated slot"
    );

    // 2. Find the first event after the confirmed LSN
    let checkpoint: Option<(String, DateTime<Utc>)> = sqlx::query_as(
        "SELECT id::text, created_at FROM pgstream.events
         WHERE lsn > $1::pg_lsn AND stream_id = $2
         ORDER BY created_at, id LIMIT 1",
    )
    .bind(&lsn)
    .bind(stream_id as i64)
    .fetch_optional(&mut *tx)
    .await?;

    // 3. Set failover checkpoint BEFORE dropping slot (crash-safe ordering)
    if let Some((id, created_at)) = checkpoint {
        info!(
            event_id = %id,
            event_created_at = %created_at,
            "setting failover checkpoint for recovery"
        );

        sqlx::query(
            "INSERT INTO pgstream.streams (id, failover_checkpoint_id, failover_checkpoint_ts)
             VALUES ($1, $2, $3)
             ON CONFLICT (id) DO UPDATE
             SET failover_checkpoint_id = $2, failover_checkpoint_ts = $3",
        )
        .bind(stream_id as i64)
        .bind(&id)
        .bind(created_at)
        .execute(&mut *tx)
        .await?;
    } else {
        info!("no events found after confirmed_flush_lsn, pipeline will start fresh");
    }

    // 4. Delete ETL replication state so ETL will create a fresh slot on restart
    // This triggers DataSync, but we skip it by returning Ok(()) from write_table_rows.
    // The failover checkpoint ensures we COPY missed events when replication starts.
    let deleted = sqlx::query("DELETE FROM etl.replication_state WHERE pipeline_id = $1")
        .bind(stream_id as i64)
        .execute(&mut *tx)
        .await?;

    info!(
        rows_deleted = deleted.rows_affected(),
        "deleted ETL replication state to trigger fresh slot creation"
    );

    // 5. Commit the transaction - checkpoint is now durable
    tx.commit().await?;

    // 6. Drop the invalidated slot AFTER commit (non-transactional operation)
    // This ordering ensures crash safety: if we crash here, the checkpoint is
    // already saved, and the next recovery attempt will simply drop the slot.
    let drop_result = sqlx::query("SELECT pg_drop_replication_slot($1)")
        .bind(&slot_name)
        .execute(pool)
        .await;

    match &drop_result {
        Ok(_) => info!(slot_name = slot_name, "dropped invalidated slot"),
        Err(e) => warn!(
            slot_name = slot_name,
            error = %e,
            "failed to drop slot (may already be dropped)"
        ),
    }

    info!("slot recovery complete, ETL will create new slot on restart");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_slot_invalidation_error_matches() {
        let error = etl::etl_error!(
            etl::error::ErrorKind::InvalidState,
            "can no longer get changes from replication slot \"test_slot\""
        );
        assert!(is_slot_invalidation_error(&error));
    }

    #[test]
    fn test_is_slot_invalidation_error_case_insensitive() {
        let error = etl::etl_error!(
            etl::error::ErrorKind::InvalidState,
            "CAN NO LONGER GET CHANGES FROM REPLICATION SLOT \"test_slot\""
        );
        assert!(is_slot_invalidation_error(&error));
    }

    #[test]
    fn test_is_slot_invalidation_error_no_match() {
        let error = etl::etl_error!(etl::error::ErrorKind::InvalidState, "connection refused");
        assert!(!is_slot_invalidation_error(&error));
    }
}
