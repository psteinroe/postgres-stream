# Migrations

pgstream applies these migrations with sqlx on startup, before the pipeline starts. Each file runs in its own transaction; the version is the file name prefix.

## Altering `pgstream.events`

etl decodes replicated rows of `pgstream.events` against a schema it stores per stream, and only updates it from a `supabase_etl_ddl` logical message in the replication stream. pgstream cannot install etl's event trigger that emits that message (it needs superuser), so every migration that runs `alter table pgstream.events` must announce the change itself, in the same transaction and after the `alter table`:

```sql
alter table pgstream.events add column tenant_id text;
select pgstream.emit_events_schema_change();
```

Notes:

- Alter the partitioned table `pgstream.events`, never a single partition. The publication uses `publish_via_partition_root`, so etl only sees the root table.
- Do not mark the migration `-- no-transaction`; the message must commit together with the schema change.
- Rows are converted to events by column name (`src/types/event.rs`). New columns are ignored until the conversion reads them; dropping or changing the type of a column it reads breaks event conversion.
- `migrations_altering_events_announce_the_schema_change` in `src/migrations.rs` fails if a migration forgets the call.

A forgotten call makes replication fail with `CorruptedTableSchema` on every restart, until the slot is recreated by hand (see "Schema Changes of the Events Table" in `docs/concepts/how-it-works.md`).
