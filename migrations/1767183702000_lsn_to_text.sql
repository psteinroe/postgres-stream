-- Change lsn column from pg_lsn to text to avoid ETL schema mismatch
-- The ETL crate doesn't have pg_lsn in its type mappings, causing "schema has changed" errors
alter table pgstream.events
alter column lsn type text using lsn::text;
