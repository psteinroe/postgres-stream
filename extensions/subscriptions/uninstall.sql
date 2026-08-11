-- Remove the optional subscription package and all triggers it manages.
-- Run this as the package owner with ownership of every subscribed table.
BEGIN;

-- Deleting definitions invokes the package coordinator, which drops the
-- generated target-table triggers and functions before the schema is removed.
DELETE FROM pgstream_subscriptions.subscriptions;

DROP SCHEMA pgstream_subscriptions CASCADE;

COMMIT;
