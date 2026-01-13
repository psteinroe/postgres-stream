# Failover and Recovery

How Postgres Stream handles failures and recovers without data loss.

## Overview

Postgres Stream automatically handles two types of failures:

1. **Sink failures** - When the destination (queue, webhook, etc.) is unavailable
2. **Slot invalidation** - When the replication slot is invalidated due to WAL retention

Both use the same recovery mechanism: replaying events from the `events` table, not the WAL.

## Sink Failure Recovery

When the sink fails (queue goes down, webhook returns errors, etc.):

1. **Checkpoint saved** - The failed event's ID is saved as a checkpoint
2. **Replication continues** - Postgres Stream keeps consuming the replication stream (events are still written to the events table)
3. **Retry loop** - Periodically retries delivering the checkpoint event
4. **Replay on recovery** - When the sink recovers, uses `COPY` to stream all events between checkpoint and current position
5. **Resume normal operation** - After replay completes, returns to streaming

```
Timeline:
───────────────────────────────────────────────────────────►
     │                    │                     │
     ▼                    ▼                     ▼
  Sink fails        Sink recovers         Replay complete
  (checkpoint)      (retry succeeds)      (resume streaming)
     │                    │                     │
     └────────────────────┴─────────────────────┘
           Events accumulate in table
```

## Slot Invalidation Recovery

When the replication slot is invalidated (WAL exceeded `max_slot_wal_keep_size`):

1. **Error detected** - Postgres Stream detects the "can no longer get changes from replication slot" error
2. **LSN queried** - Queries `confirmed_flush_lsn` from the invalidated slot (Postgres preserves this)
3. **Checkpoint set** - Finds the first event with `lsn > confirmed_flush_lsn` and sets it as the failover checkpoint
4. **Slot dropped** - Drops the invalidated slot
5. **Pipeline restart** - Restarts the pipeline, which creates a fresh slot
6. **Replay triggered** - When replication events arrive, triggers failover replay from checkpoint

This happens automatically without operator intervention.

## Guarantees

Both recovery mechanisms provide:

- **No events lost** - As long as downtime is less than partition retention (7 days by default)
- **At-least-once delivery** - Events may be delivered more than once during replay
- **Order preserved** - Events are replayed in order within partitions
- **No WAL retention required** - Events are stored in the table, not dependent on WAL

## Why This Works

Traditional CDC tools read directly from the WAL, which creates problems:

| Issue | Traditional CDC | Postgres Stream |
|-------|-----------------|-----------------|
| Slow sink | WAL retention grows | Events stored in table |
| Slot invalidation | Data loss | Automatic recovery |
| Long outage | Potential data loss | Recovers if within retention |

Because events are stored in a partitioned table:

- WAL can be released immediately
- Recovery doesn't depend on WAL availability
- 7-day retention provides a generous recovery window

## Partition Retention

The events table uses daily partitions with automatic management:

- **Creates partitions** 7 days in advance
- **Drops partitions** older than 7 days
- **Runs on startup** and then daily

This means you can recover from outages up to 7 days long without data loss.

## Monitoring

Monitor these to ensure recovery works:

- **Partition count** - Should have ~14 partitions (7 past, 7 future)
- **Checkpoint status** - Check if a failover checkpoint is set
- **Replication lag** - Large lag may indicate sink issues

## Configuration

The default 7-day retention is usually sufficient. For high-volume systems, monitor:

- Partition size growth
- Disk space usage
- Event processing rate vs. generation rate

## Next Steps

- [How It Works](how-it-works.md) - Understand the architecture
- [Configuration](../getting-started/configuration.md) - Configure your stream
