# Failover and Recovery

Postgres Stream handles both sink failures and slot invalidation automatically.

## How It Works

Events are stored in a partitioned table, not just the WAL. This means:

- WAL can be released immediately
- Recovery reads from the table, not the WAL
- 7-day partition retention = 7-day recovery window

## Sink Failure

When the sink is unavailable:

1. Checkpoint saved at failed event
2. Retries until sink recovers
3. Replays missed events from the events table
4. Resumes normal streaming

## Slot Loss

The replication slot is lost when Postgres invalidates it (`max_slot_wal_keep_size` exceeded), or when it is missing although the events table already finished its initial sync, e.g. after a database failover to a standby that did not have the slot. pgstream then:

1. Determines the lowest event `lsn` that may not have been delivered
2. Sets the checkpoint to the first event at or after that position
3. Drops the slot and restarts the pipeline with a new one
4. Replays from the checkpoint

An event's `lsn` is the WAL position at insert time, not at commit. To include events of transactions that were open at the last delivered position, pgstream records the slot's `restart_lsn` every 10 seconds and replays from the lowest of the recorded `restart_lsn` and the last durable position (the slot's `confirmed_flush_lsn`, or etl's apply progress in `etl.replication_progress` for a missing slot), minus `wal_buffers`.

If the slot is lost while the pipeline runs, the pipeline stops and pgstream recovers on restart, instead of letting etl create a new slot that skips the missed events.

No operator intervention required, with one exception: if a missing slot has no durable position at all (neither etl's apply progress nor a recorded `restart_lsn`), pgstream refuses to start with `Replication slot was lost without a durable replication position`. To replay from a known point, set `pgstream.streams.slot_recovery_lsn` to an LSN at or before the `lsn` of the first event that may not have been delivered, delete the `etl.replication_state` rows of the pipeline and restart. To accept the gap instead, only delete the `etl.replication_state` rows.

## Database Failover

Replication slots normally live only on the primary, so a switchover or failover to a standby loses the slot. On Postgres 17+, set [`stream.failover`](../reference/configuration.md#streamfailover) to `true` to make the slot a failover slot that standbys keep in sync. If the slot is lost anyway, it is recovered as described in [Slot Loss](#slot-loss).

## Guarantees

- **Zero data loss** - As long as downtime < partition retention (7 days)
- **At-least-once delivery** - Events may repeat during replay
- **Order preserved** - Events replay in order
