//! Slot recovery logic for handling invalidated replication slots.
//!
//! When a Postgres replication slot is invalidated (WAL exceeded `max_slot_wal_keep_size`),
//! this module provides functionality to:
//! 1. Query the `confirmed_flush_lsn` from the invalidated slot
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
use etl::error::EtlResult;
use sqlx::PgPool;
use tracing::{info, warn};

use crate::types::SlotName;

type Checkpoint = (String, DateTime<Utc>);

#[must_use]
fn checkpoint_is_earlier(a: &Checkpoint, b: &Checkpoint) -> bool {
    a.1 < b.1 || (a.1 == b.1 && a.0 < b.0)
}

#[must_use]
fn select_recovery_checkpoint(
    existing_checkpoint: Option<Checkpoint>,
    lsn_checkpoint: Option<Checkpoint>,
) -> Option<Checkpoint> {
    match (existing_checkpoint, lsn_checkpoint) {
        (Some(existing), Some(from_lsn)) => {
            if checkpoint_is_earlier(&existing, &from_lsn) {
                Some(existing)
            } else {
                Some(from_lsn)
            }
        }
        (Some(existing), None) => Some(existing),
        (None, Some(from_lsn)) => Some(from_lsn),
        (None, None) => None,
    }
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

    // Preserve an existing failover checkpoint if we are already in failover mode.
    let existing_checkpoint_row: Option<(Option<String>, Option<DateTime<Utc>>)> = sqlx::query_as(
        "SELECT failover_checkpoint_id, failover_checkpoint_ts FROM pgstream.streams WHERE id = $1",
    )
    .bind(stream_id as i64)
    .fetch_optional(&mut *tx)
    .await?;

    let existing_checkpoint = existing_checkpoint_row.and_then(|(id, ts)| id.zip(ts));

    if let Some((id, created_at)) = &existing_checkpoint {
        info!(
            event_id = %id,
            event_created_at = %created_at,
            "existing failover checkpoint found"
        );
    }

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
    let lsn_checkpoint: Option<Checkpoint> = sqlx::query_as(
        "SELECT id::text, created_at FROM pgstream.events
         WHERE lsn > $1::pg_lsn AND stream_id = $2
         ORDER BY created_at, id LIMIT 1",
    )
    .bind(&lsn)
    .bind(stream_id as i64)
    .fetch_optional(&mut *tx)
    .await?;

    // 3. Choose the earliest safe checkpoint.
    // If we are already in failover mode, keep the existing checkpoint unless
    // the LSN-derived checkpoint is earlier.
    let checkpoint = select_recovery_checkpoint(existing_checkpoint.clone(), lsn_checkpoint);

    // 4. Set failover checkpoint BEFORE dropping slot (crash-safe ordering)
    if checkpoint == existing_checkpoint {
        if let Some((id, created_at)) = checkpoint {
            info!(
                event_id = %id,
                event_created_at = %created_at,
                "preserving existing failover checkpoint during slot recovery"
            );
        } else {
            info!("no events found after confirmed_flush_lsn, pipeline will start fresh");
        }
    } else if let Some((id, created_at)) = checkpoint {
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

    // 5. Delete ETL replication state so ETL will create a fresh slot on restart
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

    // 6. Commit the transaction - checkpoint is now durable
    tx.commit().await?;

    // 7. Drop the invalidated slot AFTER commit (non-transactional operation)
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
    use chrono::TimeZone;

    #[test]
    fn test_select_recovery_checkpoint_prefers_earlier_existing_checkpoint() {
        let ts = Utc.with_ymd_and_hms(2024, 3, 15, 12, 0, 0).unwrap();
        let existing = Some(("00000000-0000-0000-0000-000000000001".to_string(), ts));
        let from_lsn = Some((
            "00000000-0000-0000-0000-000000000002".to_string(),
            ts + chrono::Duration::seconds(1),
        ));

        assert_eq!(
            select_recovery_checkpoint(existing.clone(), from_lsn),
            existing
        );
    }

    #[test]
    fn test_select_recovery_checkpoint_prefers_earlier_lsn_checkpoint() {
        let ts = Utc.with_ymd_and_hms(2024, 3, 15, 12, 0, 0).unwrap();
        let existing = Some((
            "00000000-0000-0000-0000-000000000002".to_string(),
            ts + chrono::Duration::seconds(1),
        ));
        let from_lsn = Some(("00000000-0000-0000-0000-000000000001".to_string(), ts));

        assert_eq!(
            select_recovery_checkpoint(existing, from_lsn.clone()),
            from_lsn
        );
    }

    #[test]
    fn test_select_recovery_checkpoint_uses_existing_when_lsn_checkpoint_missing() {
        let ts = Utc.with_ymd_and_hms(2024, 3, 15, 12, 0, 0).unwrap();
        let existing = Some(("00000000-0000-0000-0000-000000000001".to_string(), ts));

        assert_eq!(select_recovery_checkpoint(existing.clone(), None), existing);
    }
}
