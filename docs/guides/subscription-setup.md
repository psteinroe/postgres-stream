# Subscription Setup

Subscriptions are an optional SQL package that you install through your own database migration system. The pgstream daemon creates no triggers on application tables and does not migrate the `pgstream_subscriptions` schema.

## Ownership model

Subscription changes drop and recreate target-table triggers. Postgres requires the role dropping a trigger to own its table. Run the package migration as the application migration role that owns subscribed tables, or as a role that is a member of all relevant owner roles.

This privilege belongs to the database deployment path, not the long-lived pgstream runtime role.

The package accepts trusted SQL through `when_clause`, `payload_extensions`, and `metadata_extensions`. Only trusted database deployment roles should be able to change subscriptions.

## Install

Run pgstream's core migrations first, then copy [`extensions/subscriptions/migrations/0001_create_pgstream_subscriptions.sql`](https://github.com/psteinroe/postgres-stream/blob/main/extensions/subscriptions/migrations/0001_create_pgstream_subscriptions.sql) into your migration system. Review and change its schema, ownership, or grants if needed.

To apply the repository file directly:

```bash
psql "$DATABASE_URL" \
  --set ON_ERROR_STOP=1 \
  --single-transaction \
  --file extensions/subscriptions/migrations/0001_create_pgstream_subscriptions.sql
```

The installer needs:

- Ownership of subscribed tables.
- `USAGE` on the core `pgstream` schema.
- `INSERT` on `pgstream.events`.
- Permission to create and own `pgstream_subscriptions`.

The pgstream runtime role does not need access to this schema or ownership of application tables.

## Optional reconciliation helper

The package does not install `set_subscriptions()`. If you want to reconcile the complete desired subscription set for a stream, copy the optional [`examples/set_subscriptions.sql`](https://github.com/psteinroe/postgres-stream/blob/main/extensions/subscriptions/examples/set_subscriptions.sql) function into your own migrations and adapt it as needed.

The migration role that creates the function already owns it and can execute it; no additional `EXECUTE` grant is needed. Add grants only when intentionally delegating the helper to another trusted role.

See [Subscriptions](../concepts/subscriptions.md) for its behavior.

## Customize

The package is intentionally plain SQL rather than a Postgres `CREATE EXTENSION` package. You may copy and change:

- The `pgstream_subscriptions` schema name.
- Ownership and grants.
- The `set_subscriptions()` interface.
- Trigger names or generated payload logic.

Once copied, those migrations are owned and versioned by your application.
