# Replay & Slot Recovery Implementation Plan

## Overview

Two failure scenarios requiring replay mechanisms:

| Scenario | Cause | Slot Status | Recovery |
|----------|-------|-------------|----------|
| **Sink Data Loss** | Consumer bug, sink failure | Healthy | User triggers replay via SQL |
| **Slot Invalidation** | WAL exceeded `max_slot_wal_keep_size` | Dead | Daemon auto-recovers with new slot |

---

## Completed: Step 1 - Add LSN to Events Table

### Migration Created: `migrations/1766831075000_add_lsn.sql`
- Added `lsn pg_lsn` column to `pgstream.events`
- Modified trigger to capture `pg_current_wal_lsn()` on insert
- No index added (only needed for one-time recovery query, not worth the overhead)

### Rust Changes
- `TriggeredEvent` now has `lsn: Option<PgLsn>` field
- `PgLsn` type from tokio-postgres provides proper ordering and parsing
- COPY query includes `lsn` column for failover recovery
- LSN is parsed from text using `PgLsn::from_str()`

### Container Configuration
- Test container now starts with `wal_level=logical` for replication support

---

## Verified: Slot Invalidation Behavior

### Error Detection Pattern

When a slot is invalidated and we try to use it, PostgreSQL returns:

```
Error code: 55000 (SqlState::OBJECT_NOT_IN_PREREQUISITE_STATE)
Message: "can no longer get changes from replication slot \"<slot_name>\""
Detail: "This slot has been invalidated because it exceeded the maximum reserved size."
```

### Detection Function

```rust
fn is_slot_invalidation_error(error: &EtlError) -> bool {
    let msg = error.to_string().to_lowercase();
    msg.contains("can no longer get changes from replication slot")
}
```

### Key Finding: `confirmed_flush_lsn` is Preserved

Even when `wal_status = 'lost'`, the slot's `confirmed_flush_lsn` remains available in `pg_replication_slots`. This enables precise LSN-based recovery:

```sql
-- Get the last confirmed LSN from the invalidated slot
select confirmed_flush_lsn from pg_replication_slots
where slot_name = 'supabase_etl_apply_1' and wal_status = 'lost';
-- Returns: 0/2000000

-- Find the first event after that LSN for replay
select min(created_at) from pgstream.events
where lsn > '0/2000000'::pg_lsn and stream_id = 1;
```

### Slot Naming Convention

The etl crate names slots as: `supabase_etl_apply_{pipeline_id}`

---

## Scenario 1: Sink Lost Data (User-Initiated Replay)

### Trigger Mechanism
User sets `replay_from_ts` via SQL UPDATE:

```sql
-- Replay from specific timestamp
update pgstream.streams
set replay_from_ts = '2025-01-15 10:00:00'
where id = 1;

-- Replay all events in retention window
update pgstream.streams
set replay_from_ts = (select min(created_at) from pgstream.events where stream_id = 1)
where id = 1;
```

### Detecting Replay Request

Add a trigger on `pgstream.streams` that inserts a "control event" into `pgstream.events` when `replay_from_ts` is set:

```sql
create or replace function pgstream.notify_replay_request()
returns trigger as $$
begin
    if new.replay_from_ts is not null and (old.replay_from_ts is null or old.replay_from_ts != new.replay_from_ts) then
        insert into pgstream.events (payload, stream_id, lsn)
        values (
            jsonb_build_object(
                'type', 'control',
                'action', 'replay',
                'replay_from_ts', new.replay_from_ts,
                'replay_to_ts', new.replay_to_ts
            ),
            new.id,
            pg_current_wal_lsn()
        );
    end if;
    return new;
end;
$$ language plpgsql;

create trigger on_replay_request
after update of replay_from_ts on pgstream.streams
for each row execute function pgstream.notify_replay_request();
```

### Database Schema Changes

```sql
-- Migration: Add replay support to streams table
alter table pgstream.streams
add column replay_from_ts timestamptz,
add column replay_to_ts timestamptz;
```

### Stream Status Extension

```rust
// src/types/stream.rs
pub enum StreamStatus {
    Healthy,
    Failover { checkpoint_event_id: EventIdentifier },
    Replay { from_ts: DateTime<Utc>, to_ts: Option<DateTime<Utc>> },
}
```

### Files to Modify

| File | Changes |
|------|---------|
| `migrations/NNNN_add_replay.sql` | Add `replay_from_ts`, `replay_to_ts` columns |
| `src/types/stream.rs` | Add `Replay` variant to `StreamStatus` |
| `src/store.rs` | Add `get_replay_request()`, `clear_replay_request()` |
| `src/stream.rs` | Add `handle_user_replay()`, check in `tick()` |
| `src/failover_client.rs` | Add `get_events_by_timestamp_range()` |

---

## Scenario 2: Slot Invalidation (Automatic Recovery)

### Recovery Flow

```
┌─────────────────────────────────────────────────────────────────┐
│   Pipeline fails with "can no longer get changes" error         │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│   Query pg_replication_slots for confirmed_flush_lsn            │
│   (slot still exists with wal_status='lost')                    │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│   Find replay point: SELECT MIN(created_at) FROM events        │
│   WHERE lsn > confirmed_flush_lsn AND stream_id = ?             │
└─────────────────────────────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│   1. Drop old invalidated slot                                  │
│   2. Replay events from the found timestamp                     │
│   3. Create new slot and resume normal operation                │
└─────────────────────────────────────────────────────────────────┘
```

### Implementation in core.rs

```rust
pub async fn start_pipeline_with_config(config: PipelineConfig) -> EtlResult<()> {
    loop {
        let result = run_pipeline(&config).await;

        match result {
            Ok(()) => break Ok(()),
            Err(e) if is_slot_invalidation_error(&e) => {
                warn!("Replication slot invalidated: {}", e);
                handle_slot_recovery(&config).await?;
                // Loop continues, will recreate pipeline
            }
            Err(e) => break Err(e),
        }
    }
}

fn is_slot_invalidation_error(error: &EtlError) -> bool {
    let msg = error.to_string().to_lowercase();
    msg.contains("can no longer get changes from replication slot")
}

async fn handle_slot_recovery(config: &PipelineConfig) -> EtlResult<()> {
    let pool = create_pool(&config.stream.pg_connection).await?;
    let slot_name = format!("supabase_etl_apply_{}", config.stream.id);

    // Get confirmed_flush_lsn BEFORE dropping the slot
    let slot_lsn: Option<String> = sqlx::query_scalar(
        "select confirmed_flush_lsn::text from pg_replication_slots
         where slot_name = $1 and wal_status = 'lost'"
    )
    .bind(&slot_name)
    .fetch_optional(&pool)
    .await?
    .flatten();

    // Drop the invalid slot
    let _ = sqlx::query("select pg_drop_replication_slot($1)")
        .bind(&slot_name)
        .execute(&pool)
        .await;

    // Find replay point from LSN
    if let Some(lsn) = slot_lsn {
        let replay_ts: Option<DateTime<Utc>> = sqlx::query_scalar(
            "select min(created_at) from pgstream.events
             where lsn > $1::pg_lsn and stream_id = $2"
        )
        .bind(&lsn)
        .bind(config.stream.id as i64)
        .fetch_one(&pool)
        .await?;

        if let Some(ts) = replay_ts {
            // Set replay_from_ts to trigger replay
            sqlx::query("update pgstream.streams set replay_from_ts = $1 where id = $2")
                .bind(ts)
                .bind(config.stream.id as i64)
                .execute(&pool)
                .await?;
        }
    }

    Ok(())
}
```

### Files to Modify

| File | Changes |
|------|---------|
| `src/core.rs` | Add recovery loop, `is_slot_invalidation_error()`, `handle_slot_recovery()` |
| `src/store.rs` | Add slot recovery state methods if needed |

---

## Implementation Order

### Step 1: Add LSN to Events Table ✅ COMPLETE
- Added `lsn pg_lsn` column to `pgstream.events`
- Modified trigger to capture `pg_current_wal_lsn()` on insert
- Updated Rust types and parsing

### Step 2: Add Replay Columns to Streams Table
- Add `replay_from_ts`, `replay_to_ts` columns
- Add control event trigger

### Step 3: Implement User-Initiated Replay
- Handle control events in daemon
- Add `get_events_by_timestamp_range()` to FailoverClient
- Add replay handling in `tick()`

### Step 4: Implement Automatic Slot Recovery
- Add recovery loop to `start_pipeline_with_config()`
- Query `confirmed_flush_lsn` from invalidated slot
- Implement LSN-based recovery point lookup

---

## Test Coverage

### `tests/slot_recovery_tests.rs`

| Test | Status | Purpose |
|------|--------|---------|
| `test_pipeline_fails_on_invalidated_slot` | ✅ Passes | Verifies current behavior - pipeline fails correctly |
| `test_can_detect_slot_invalidation_and_get_recovery_info` | ✅ Passes | Confirms `confirmed_flush_lsn` is preserved |
| `test_pipeline_recovers_automatically_from_invalidated_slot` | ⏸️ Ignored | TDD test for automatic recovery (will pass after Step 4) |

---

## Operator Playbook

### Scenario 1: Sink Lost Data
```sql
-- Check available events
select min(created_at), max(created_at), count(*)
from pgstream.events
where stream_id = 1;

-- Trigger replay from specific timestamp
update pgstream.streams
set replay_from_ts = '2025-01-15 10:00:00'
where id = 1;
```

### Scenario 2: Slot Invalidated
After Step 4 is implemented, the daemon will automatically:
1. Detect the slot error
2. Query `confirmed_flush_lsn` from the invalidated slot
3. Find the replay point using the `lsn` column
4. Set `replay_from_ts` to trigger replay
5. Drop the old slot and create a new one
6. Resume normal operation
