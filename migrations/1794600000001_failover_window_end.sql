-- Inclusive end of the failover replay window.
--
-- failover_checkpoint_id/_ts is the window's start. Batches arrive in commit
-- order, not in (created_at, id) order, so events of a failed or dropped batch
-- can sort after the batch that ends failover. The stream records the largest
-- (created_at, id) of every failed and dropped batch here, and slot recovery
-- records the time the new slot delivered its first batch. Recovery replays
-- the whole window. A null end replays up to the batch that ends failover.
ALTER TABLE pgstream.streams ADD COLUMN IF NOT EXISTS failover_window_end_id text;
ALTER TABLE pgstream.streams ADD COLUMN IF NOT EXISTS failover_window_end_ts timestamptz;
