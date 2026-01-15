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
3. Replays missed events via `COPY`
4. Resumes normal streaming

## Slot Invalidation

When the replication slot is invalidated:

1. Queries last confirmed LSN from invalidated slot
2. Sets checkpoint to first event after that LSN
3. Drops slot and restarts pipeline
4. Replays from checkpoint

No operator intervention required.

## Guarantees

- **Zero data loss** - As long as downtime < partition retention (7 days)
- **At-least-once delivery** - Events may repeat during replay
- **Order preserved** - Events replay in order
