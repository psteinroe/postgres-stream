# Failover and Recovery

Postgres Stream handles both sink failures and slot invalidation automatically.

## How It Works

Events are stored in a partitioned table, not just the WAL. This means:

- WAL can be released immediately
- Recovery reads from the table, not the WAL
- 7-day partition retention = 7-day recovery window

## Sink Failure

When the sink is unavailable:

1. Records the failed batch in a replay window: the checkpoint is its earliest event and the window end its latest, by `(created_at, id)`
2. Retries the checkpoint event with every new batch. While the sink is still down, the new batch is dropped and the window extended to cover it, before the batch is acknowledged
3. Once the sink accepts the checkpoint event, replays every event from the checkpoint up to the window end, or up to the current batch if that is later
4. Publishes the current batch and resumes normal streaming

Batches arrive in commit order, not in `created_at` order, so a failed or dropped batch can hold events that sort before its first event or after the batch that ends failover. The window covers both. The checkpoint and window end live in `pgstream.streams` and survive restarts. Replay progress is saved after every replayed batch.

## Slot Loss

The replication slot is lost when Postgres invalidates it (`max_slot_wal_keep_size` exceeded), or when it is missing although the events table already finished its initial sync, e.g. after a database failover to a standby that did not have the slot. pgstream then:

1. Determines the lowest event `lsn` that may not have been delivered
2. Sets the checkpoint to the first event at or after that position
3. Drops the slot and restarts the pipeline with a new one
4. When the new slot delivers its first batch, replays every event from the checkpoint up to that time

Events the new slot does not deliver were committed before it existed, so their `created_at` is earlier than its first batch. pgstream adds one minute to allow for clock differences between database hosts.

An event's `lsn` is the WAL position at insert time, not at commit. To include events of transactions that were open at the last delivered position, pgstream records the slot's `restart_lsn` every 10 seconds and replays from the lowest of the recorded `restart_lsn` and the last durable position (the slot's `confirmed_flush_lsn`, or etl's apply progress in `etl.replication_progress` for a missing slot), minus `wal_buffers`.

If the slot is lost while the pipeline runs, the pipeline stops and pgstream recovers on restart, instead of letting etl create a new slot that skips the missed events.

No operator intervention required, with one exception: if a missing slot has no durable position at all (neither etl's apply progress nor a recorded `restart_lsn`), pgstream refuses to start with `Replication slot was lost without a durable replication position`. To replay from a known point, set `pgstream.streams.slot_recovery_lsn` to an LSN at or before the `lsn` of the first event that may not have been delivered, delete the `etl.replication_state` rows of the pipeline and restart. To accept the gap instead, only delete the `etl.replication_state` rows.

## Database Failover

Replication slots normally live only on the primary, so a switchover or failover to a standby loses the slot. On Postgres 17+, set [`stream.failover`](../reference/configuration.md#streamfailover) to `true` to make the slot a failover slot that standbys keep in sync. After a promotion, the stream reconnects to the new primary and resumes from the synced slot. Events that were published but not yet confirmed to the synced slot are published again.

The slot survives a promotion only if the promoted standby synced it:

| Where | Setting |
|--|--|
| Standby | `sync_replication_slots = on` |
| Standby | `hot_standby_feedback = on` |
| Standby | `primary_slot_name` set, and a `dbname` in `primary_conninfo` |
| Primary | the standby's physical slot listed in `synchronized_standby_slots` |

`synchronized_standby_slots` makes the primary wait until the listed standbys received the WAL before it sends the changes to the stream, so the stream never gets ahead of the standby it fails over to. When `stream.failover` is enabled and `synchronized_standby_slots` is empty, the stream logs a warning on startup. It still starts, because managed providers may configure standbys in ways the stream cannot see.

A new slot is first synced as a temporary slot, and becomes usable on the standby once the stream confirmed changes on the primary. Promote only after `pg_replication_slots.synced` is `true` and `temporary` is `false` for the slot on the standby.

If the promoted standby did not sync the slot (or `stream.failover` is off), the slot is missing on the new primary. The stream does not let etl create a new slot that skips the missed events: it recovers the slot as described in [Slot Loss](#slot-loss), on startup or after the running pipeline stops on the missing slot, and replays every event that may not have been delivered from the events table.

### PlanetScale

PlanetScale Postgres manages the standbys and `synchronized_standby_slots`, and waits for failover slots to be synced before a planned failover. Enable the following cluster parameters (Clusters > Parameters), then set `stream.failover: true`:

- `hot_standby_feedback = on`
- `sync_replication_slots = on`

An existing slot is altered to be a failover slot on the next start. PlanetScale notifies you about slots without `failover = true` and eventually proceeds with planned failovers anyway, which loses such slots.

## Guarantees

- **At-least-once delivery** - Every event is published at least once, as long as it is replayed before partition retention drops it (7 days). Events may repeat during replay
- **Replay order** - Replayed events are published in `(created_at, id)` order, followed by the batch that ended failover

Assumptions:

- Events are not deleted from the events table before they are delivered
- `created_at` is not set more than one minute ahead of the database clock (the default, `clock_timestamp()`, never is)
- A lost slot is replayed from the lowest of its recorded `restart_lsn` and its last durable position. If it was lost before pgstream recorded its `restart_lsn`, events inserted before that position by transactions that committed after it are not replayed
