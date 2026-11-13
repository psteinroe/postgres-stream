//! Slot recovery logic for handling lost replication slots.
//!
//! A slot is lost when Postgres invalidates it (WAL exceeded `max_slot_wal_keep_size`) or when it
//! is missing although the events table already finished its initial sync, e.g. after a database
//! failover to a standby that did not have the slot (yet). ETL would create a new slot at the
//! current WAL position and silently skip everything in between, so this module provides
//! functionality to:
//! 1. Determine the lowest event `lsn` that may not have been handled (the replay lower bound)
//! 2. Set a failover checkpoint at the first event at or past that bound and store the bound
//! 3. Delete ETL replication state (triggers fresh slot creation)
//! 4. Drop the invalidated slot
//!
//! After recovery, ETL will create a new slot and run DataSync (table sync workers).
//! The stream's `write_table_rows()` returns `Ok(())` to skip DataSync entirely.
//! When the first replication event arrives, the stream resolves the stored bound again with
//! [`resolve_slot_recovery`] and `handle_failover()` replays the missed events from the events
//! table.
//!
//! This design is crash-safe: if the system crashes after setting the checkpoint,
//! the checkpoint persists and will be used on restart regardless of slot state.
//!
//! # Replay lower bound
//!
//! An event's `lsn` is the WAL write position at the time it was inserted, while slot positions
//! are commit positions. A transaction that commits after the slot's confirmed position may have
//! inserted its events long before it. The slot's `restart_lsn` covers this: every transaction
//! that commits after the confirmed position wrote its changes at or after the `restart_lsn`.
//! Postgres clears `restart_lsn` when it invalidates a slot and drops it with the slot, so
//! [`record_slot_restart_lsn`] keeps a copy in `pgstream.streams.slot_restart_lsn` while the slot is
//! healthy. A stale copy is older and therefore still safe.
//!
//! The bound is the lowest of the recorded `restart_lsn` and the last durable position (the
//! invalidated slot's `confirmed_flush_lsn`, or etl's apply `flush_lsn` in
//! `etl.replication_progress` for a missing slot), lowered by `wal_buffers` plus one WAL page: the
//! write position returned by `pg_current_wal_lsn()` lags the position of the event's WAL record
//! by at most the size of the WAL buffers.
//!
//! Without a recorded `restart_lsn` (a slot lost before pgstream observed it), events of a
//! transaction that inserted them before the durable position and committed after it are missed.
//! Without any durable position, recovery fails and the operator has to choose a replay point.
//!
//! Transactions still running during recovery are invisible to it, and the new slot does not
//! decode transactions that commit while it is created. The bound is therefore stored in
//! `pgstream.streams.slot_recovery_lsn` and resolved again once the new slot delivers its first
//! batch, when all of them are visible.

use chrono::{DateTime, Utc};
use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use sqlx::{PgExecutor, PgPool};
use tracing::{info, warn};

use crate::queries::get_slot_state;
use crate::types::{PgLsn, SlotName};

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

/// Why the replication slot of a stream has to be recovered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotLoss {
    /// Postgres invalidated the slot (`wal_status = 'lost'`).
    Invalidated,
    /// The slot does not exist although the events table finished its initial sync.
    Missing,
}

/// Returns whether the stream's replication slot was lost and has to be recovered with
/// [`handle_slot_recovery`] before the pipeline starts.
pub async fn detect_slot_loss(pool: &PgPool, stream_id: u64) -> EtlResult<Option<SlotLoss>> {
    match get_slot_state(pool, &stream_id.slot_name()).await? {
        Some(state) if state.is_invalidated() => Ok(Some(SlotLoss::Invalidated)),
        Some(_) => Ok(None),
        None if has_synced_tables(pool, stream_id).await? => Ok(Some(SlotLoss::Missing)),
        None => Ok(None),
    }
}

/// Returns whether ETL finished the initial sync of a table of the stream's pipeline, i.e.
/// whether the pipeline relies on its apply slot to see every change.
async fn has_synced_tables<'c, E: PgExecutor<'c>>(executor: E, stream_id: u64) -> EtlResult<bool> {
    let synced = sqlx::query_scalar(
        "SELECT EXISTS (
           SELECT 1 FROM etl.replication_state
           WHERE pipeline_id = $1 AND is_current AND state IN ('sync_done', 'ready')
         )",
    )
    .bind(stream_id as i64)
    .fetch_one(executor)
    .await?;

    Ok(synced)
}

/// Records the `restart_lsn` of the stream's slot as the replay lower bound for slot recovery.
///
/// Does nothing if the slot is missing or invalidated.
pub async fn record_slot_restart_lsn(pool: &PgPool, stream_id: u64) -> EtlResult<()> {
    sqlx::query(
        "UPDATE pgstream.streams s
         SET slot_restart_lsn = r.restart_lsn
         FROM pg_replication_slots r
         WHERE s.id = $1
           AND r.slot_name = $2
           AND r.restart_lsn IS NOT NULL
           AND r.wal_status <> 'lost'
           AND s.slot_restart_lsn IS DISTINCT FROM r.restart_lsn",
    )
    .bind(stream_id as i64)
    .bind(stream_id.slot_name())
    .execute(pool)
    .await?;

    Ok(())
}

fn parse_lsn(lsn: &str) -> EtlResult<PgLsn> {
    lsn.parse().map_err(|_| {
        etl_error!(
            ErrorKind::InvalidData,
            "Invalid pg_lsn value",
            format!("Could not parse pg_lsn '{lsn}'")
        )
    })
}

/// Slot recovery state of a stream in `pgstream.streams`.
#[derive(sqlx::FromRow)]
struct RecoveryState {
    failover_checkpoint_id: Option<String>,
    failover_checkpoint_ts: Option<DateTime<Utc>>,
    slot_restart_lsn: Option<String>,
    slot_recovery_lsn: Option<String>,
}

/// Locks the stream's row and returns its slot recovery state.
async fn fetch_recovery_state<'c, E: PgExecutor<'c>>(
    executor: E,
    stream_id: u64,
) -> EtlResult<Option<RecoveryState>> {
    let row = sqlx::query_as(
        "SELECT failover_checkpoint_id, failover_checkpoint_ts,
                slot_restart_lsn::text AS slot_restart_lsn,
                slot_recovery_lsn::text AS slot_recovery_lsn
         FROM pgstream.streams WHERE id = $1 FOR UPDATE",
    )
    .bind(stream_id as i64)
    .fetch_optional(executor)
    .await?;

    Ok(row)
}

/// Returns the first event (by `created_at`, `id`) with an `lsn` at or past `lsn`.
///
/// `id` intentionally resolves to the selected text output column so its ordering matches the
/// persisted checkpoint representation.
async fn first_event_at_or_after<'c, E: PgExecutor<'c>>(
    executor: E,
    stream_id: u64,
    lsn: PgLsn,
) -> EtlResult<Option<Checkpoint>> {
    let checkpoint = sqlx::query_as(
        "SELECT id::text, created_at FROM pgstream.events
         WHERE lsn >= $1::pg_lsn AND stream_id = $2
         ORDER BY created_at, id LIMIT 1",
    )
    .bind(lsn.to_string())
    .bind(stream_id as i64)
    .fetch_optional(executor)
    .await?;

    Ok(checkpoint)
}

/// Handles recovery from a lost replication slot.
///
/// This function is crash-safe:
/// 1. Determines the replay lower bound (see the module docs)
/// 2. Finds the first event with `lsn` at or past the bound
/// 3. Sets failover checkpoint and the bound in pgstream.streams (transactional)
/// 4. Deletes ETL replication state and apply progress (transactional)
/// 5. Commits the transaction (checkpoint is now durable)
/// 6. Drops the invalidated slot (non-transactional, done AFTER commit)
///
/// The slot drop is done after commit because `pg_drop_replication_slot` is not
/// transactional. By setting the checkpoint first, we ensure crash safety:
/// - If crash before commit: checkpoint not set, slot still exists, recovery reruns
/// - If crash after commit but before drop: checkpoint is saved, slot drop will happen on next recovery
///
/// Does nothing if the slot is healthy, or missing before the initial sync finished. Fails if the
/// slot is missing and no durable replication position is known: neither etl's apply progress
/// nor a recorded `restart_lsn`. Pipelines that ran before etl persisted its progress and before
/// pgstream recorded the `restart_lsn` can end up there. The error explains how to continue.
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

    // Preserve an existing failover checkpoint if we are already in failover mode, and an
    // unresolved bound if an earlier recovery did not see a batch yet.
    let stream_row = fetch_recovery_state(&mut *tx, stream_id).await?;
    let (existing_checkpoint, recorded_restart_lsn, pending_recovery_lsn) = match stream_row {
        Some(row) => (
            row.failover_checkpoint_id.zip(row.failover_checkpoint_ts),
            row.slot_restart_lsn,
            row.slot_recovery_lsn,
        ),
        None => (None, None, None),
    };

    if let Some((id, created_at)) = &existing_checkpoint {
        info!(
            event_id = %id,
            event_created_at = %created_at,
            "existing failover checkpoint found"
        );
    }

    // 1. Collect the durable positions BEFORE dropping the slot
    let slot: Option<(Option<String>, Option<String>, Option<String>)> = sqlx::query_as(
        "SELECT confirmed_flush_lsn::text, restart_lsn::text, wal_status
         FROM pg_replication_slots WHERE slot_name = $1",
    )
    .bind(&slot_name)
    .fetch_optional(&mut *tx)
    .await?;

    let (loss, slot_positions) = match slot {
        Some((confirmed_flush_lsn, restart_lsn, wal_status))
            if wal_status.as_deref() == Some("lost") =>
        {
            info!(
                slot_name = slot_name,
                confirmed_flush_lsn = ?confirmed_flush_lsn,
                "found invalidated slot"
            );
            (
                SlotLoss::Invalidated,
                vec![confirmed_flush_lsn, restart_lsn],
            )
        }
        Some(_) => {
            info!(slot_name = slot_name, "slot is healthy, nothing to recover");
            tx.rollback().await?;
            return Ok(());
        }
        None => {
            if !has_synced_tables(&mut *tx, stream_id).await? {
                info!(
                    slot_name = slot_name,
                    "slot not found and initial sync not finished, nothing to recover"
                );
                tx.rollback().await?;
                return Ok(());
            }

            let flush_lsn: Option<String> = sqlx::query_scalar(
                "SELECT flush_lsn::text FROM etl.replication_progress
                 WHERE pipeline_id = $1 AND worker_type = 'apply' AND table_id IS NULL",
            )
            .bind(stream_id as i64)
            .fetch_optional(&mut *tx)
            .await?;

            info!(
                slot_name = slot_name,
                flush_lsn = ?flush_lsn,
                "slot is missing although the events table is synced"
            );
            (SlotLoss::Missing, vec![flush_lsn])
        }
    };

    if recorded_restart_lsn.is_none() {
        warn!(
            slot_name = slot_name,
            "no restart_lsn was recorded for the slot; events inserted before the last durable \
             position by transactions that committed after it will not be replayed"
        );
    }

    let mut known_positions = Vec::new();
    for lsn in slot_positions
        .iter()
        .chain([&recorded_restart_lsn])
        .flatten()
    {
        known_positions.push(parse_lsn(lsn)?);
    }

    let Some(position) = known_positions.into_iter().min() else {
        return Err(etl_error!(
            ErrorKind::ReplicationSlotNotFound,
            "Replication slot was lost without a durable replication position",
            format!(
                "The replication slot '{slot_name}' of stream {stream_id} is {} and neither \
                 etl.replication_progress nor pgstream.streams.slot_restart_lsn holds a position \
                 to replay missed events from. To replay from a known point, set \
                 pgstream.streams.slot_recovery_lsn of stream {stream_id} to an LSN at or before \
                 the lsn of the first event that may not have been delivered. Then delete the \
                 etl.replication_state rows of pipeline {stream_id} (and drop the slot if it \
                 still exists) and restart pgstream.",
                match loss {
                    SlotLoss::Invalidated => "invalidated",
                    SlotLoss::Missing => "missing",
                }
            )
        ));
    };

    // pg_current_wal_lsn() is the WAL write position, which lags the position the event's own
    // WAL record is inserted at by at most the WAL buffers.
    let wal_buffer_bytes: i64 = sqlx::query_scalar(
        "SELECT pg_size_bytes(current_setting('wal_buffers'))
                + current_setting('wal_block_size')::bigint",
    )
    .fetch_one(&mut *tx)
    .await?;
    let mut lower_bound = PgLsn::from(u64::from(position).saturating_sub(wal_buffer_bytes as u64));
    if let Some(pending) = pending_recovery_lsn {
        lower_bound = lower_bound.min(parse_lsn(&pending)?);
    }

    info!(
        position = %position,
        lower_bound = %lower_bound,
        "determined replay lower bound for slot recovery"
    );

    // 2. Find the first event at or past the lower bound.
    let lsn_checkpoint = first_event_at_or_after(&mut *tx, stream_id, lower_bound).await?;

    // 3. Choose the earliest safe checkpoint.
    // If we are already in failover mode, keep the existing checkpoint unless
    // the LSN-derived checkpoint is earlier.
    let checkpoint = select_recovery_checkpoint(existing_checkpoint.clone(), lsn_checkpoint);

    if checkpoint == existing_checkpoint {
        if let Some((id, created_at)) = &checkpoint {
            info!(
                event_id = %id,
                event_created_at = %created_at,
                "preserving existing failover checkpoint during slot recovery"
            );
        } else {
            info!("no events found after the replay lower bound yet");
        }
    } else if let Some((id, created_at)) = &checkpoint {
        info!(
            event_id = %id,
            event_created_at = %created_at,
            "setting failover checkpoint for recovery"
        );
    }

    // 4. Set failover checkpoint and lower bound BEFORE dropping slot (crash-safe ordering)
    let (checkpoint_id, checkpoint_ts) = checkpoint.unzip();
    sqlx::query(
        "INSERT INTO pgstream.streams
           (id, failover_checkpoint_id, failover_checkpoint_ts, slot_recovery_lsn)
         VALUES ($1, $2, $3, $4::pg_lsn)
         ON CONFLICT (id) DO UPDATE
         SET failover_checkpoint_id = $2, failover_checkpoint_ts = $3, slot_recovery_lsn = $4::pg_lsn",
    )
    .bind(stream_id as i64)
    .bind(checkpoint_id)
    .bind(checkpoint_ts)
    .bind(lower_bound.to_string())
    .execute(&mut *tx)
    .await?;

    // 5. Delete ETL replication state so ETL will create a fresh slot on restart
    // This triggers DataSync, but we skip it by returning Ok(()) from write_table_rows.
    // The failover checkpoint ensures we COPY missed events when replication starts.
    let deleted = sqlx::query("DELETE FROM etl.replication_state WHERE pipeline_id = $1")
        .bind(stream_id as i64)
        .execute(&mut *tx)
        .await?;

    // The apply progress belongs to the lost slot. ETL deletes it before creating a slot anyway.
    sqlx::query(
        "DELETE FROM etl.replication_progress
         WHERE pipeline_id = $1 AND worker_type = 'apply' AND table_id IS NULL",
    )
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
    if loss == SlotLoss::Invalidated {
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
    }

    info!("slot recovery complete, ETL will create new slot on restart");
    Ok(())
}

/// Turns the replay lower bound left by [`handle_slot_recovery`] into a failover checkpoint.
///
/// Called with the first batch of the new slot. By then every transaction that committed before
/// the new slot was created is visible, including those that were still running during recovery.
/// Sets the failover checkpoint to the earlier of the existing checkpoint and the first event at
/// or past the bound, clears the bound and returns the checkpoint.
pub async fn resolve_slot_recovery(pool: &PgPool, stream_id: u64) -> EtlResult<Option<Checkpoint>> {
    let mut tx = pool.begin().await?;

    let row = fetch_recovery_state(&mut *tx, stream_id).await?;
    let Some(RecoveryState {
        failover_checkpoint_id,
        failover_checkpoint_ts,
        slot_recovery_lsn: Some(recovery_lsn),
        ..
    }) = row
    else {
        tx.rollback().await?;
        return Ok(None);
    };
    let existing_checkpoint = failover_checkpoint_id.zip(failover_checkpoint_ts);

    let lsn_checkpoint =
        first_event_at_or_after(&mut *tx, stream_id, parse_lsn(&recovery_lsn)?).await?;
    let checkpoint = select_recovery_checkpoint(existing_checkpoint, lsn_checkpoint);

    let (checkpoint_id, checkpoint_ts) = checkpoint.clone().unzip();
    sqlx::query(
        "UPDATE pgstream.streams
         SET failover_checkpoint_id = $2, failover_checkpoint_ts = $3, slot_recovery_lsn = NULL
         WHERE id = $1",
    )
    .bind(stream_id as i64)
    .bind(checkpoint_id)
    .bind(checkpoint_ts)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;

    info!(
        recovery_lsn = %recovery_lsn,
        checkpoint = ?checkpoint,
        "resolved slot recovery lower bound"
    );

    Ok(checkpoint)
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
