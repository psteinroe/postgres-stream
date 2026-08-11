# Upgrade Subscriptions from pgstream 0.1

The breaking-change release stops installing subscription SQL through the pgstream daemon. Existing subscription objects must be moved from `pgstream` to the user-managed `pgstream_subscriptions` schema before starting the new version.

## Before upgrading

1. Back up the database.
2. Test the migration against a production-like copy.
3. Identify the owner of every subscribed table.
4. Connect as a role that can administer the legacy pgstream objects and is the owner, or a member of the owner role, for every subscribed table.

The migration preserves subscription rows and target triggers, but future calls to `set_subscriptions()` need the package owner to drop and recreate those triggers.

## Run the upgrade

Copy and review [`extensions/subscriptions/upgrades/from-pgstream-0.1.sql`](https://github.com/psteinroe/postgres-stream/blob/main/extensions/subscriptions/upgrades/from-pgstream-0.1.sql), then run it before deploying the new pgstream binary:

```bash
psql "$DATABASE_URL" \
  --set ON_ERROR_STOP=1 \
  --file extensions/subscriptions/upgrades/from-pgstream-0.1.sql
```

The script runs in a transaction and:

1. Creates `pgstream_subscriptions`.
2. Moves the subscription enum, table, helper functions, and generated functions with `ALTER ... SET SCHEMA`.
3. Transfers ownership to the role running the migration.
4. Replaces the coordinator body so future trigger rebuilds use the new schema.
5. Adds `pgstream_subscriptions.set_subscriptions()`.

`ALTER ... SET SCHEMA` preserves table rows, object OIDs, and dependencies. Existing subscription UUIDs and target-trigger function references are not recreated.

## Verify

```sql
select count(*) from pgstream_subscriptions.subscriptions;

select to_regclass('pgstream.subscriptions') is null as legacy_removed;

select namespace.nspname, procedure.proname
from pg_catalog.pg_proc as procedure
join pg_catalog.pg_namespace as namespace
  on namespace.oid = procedure.pronamespace
where namespace.nspname = 'pgstream_subscriptions';
```

Insert or update a test row covered by an existing subscription and confirm that an event appears in `pgstream.events`.

After verification, deploy the new pgstream version. Its core migration accepts the moved installation but rejects non-empty legacy `pgstream.subscriptions` installations with an upgrade hint.

## Rollback

The upgrade is atomic before commit. If it fails, PostgreSQL rolls back the schema and ownership moves. After commit, restore the database backup or write a reverse migration appropriate for your customized ownership and schema choices.
