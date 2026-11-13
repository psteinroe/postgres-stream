-- WAL positions used to recover from a lost replication slot.
--
-- slot_restart_lsn: the last observed restart_lsn of the stream's slot. Every
-- transaction that commits after the slot's confirmed position wrote its
-- changes at or after this LSN. Postgres clears restart_lsn when a slot is
-- invalidated and drops it with the slot, so pgstream records it while the
-- slot is healthy.
--
-- slot_recovery_lsn: set by slot recovery to the lowest event lsn that may not
-- have been delivered. The stream resolves it into a failover checkpoint on
-- its first batch, once the new slot exists and every transaction that was
-- still running during recovery is visible.
ALTER TABLE pgstream.streams ADD COLUMN IF NOT EXISTS slot_restart_lsn pg_lsn;
ALTER TABLE pgstream.streams ADD COLUMN IF NOT EXISTS slot_recovery_lsn pg_lsn;
