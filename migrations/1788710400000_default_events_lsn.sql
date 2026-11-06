-- Default the event's WAL position for every inserter.
--
-- Slot recovery replays from the first event whose lsn is past the invalidated
-- slot's confirmed_flush_lsn. Events inserted without an lsn (manual inserts,
-- application functions that only set payload and metadata) were invisible to
-- that lookup: when they were the only events after the slot's position,
-- recovery found nothing to replay and they were never delivered.
ALTER TABLE pgstream.events ALTER COLUMN lsn SET DEFAULT pg_current_wal_lsn();
