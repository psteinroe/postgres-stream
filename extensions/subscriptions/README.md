# Subscription SQL package

This optional SQL package captures changes from application tables and writes
them to the core `pgstream.events` table. It lives in the separate
`pgstream_subscriptions` schema and is managed by your database migrations, not
by the pgstream daemon.

## Why installation is separate

Changing subscriptions drops and recreates triggers on their target tables.
Postgres requires the role dropping a trigger to own its table. Install this
package as the application migration role that owns the subscribed tables (or a
role that is a member of their owner roles). The long-lived pgstream runtime
role does not need application-table ownership.

The package uses `SECURITY DEFINER` functions and accepts trusted SQL expressions
in conditions and extensions. Only grant package administration to trusted
database deployment roles.

## Install

The core pgstream migrations must run first because the package writes to
`pgstream.events` and uses `pgstream.portable_uuidv7()`.

Copy [`migrations/0001_create_pgstream_subscriptions.sql`](migrations/0001_create_pgstream_subscriptions.sql)
into your migration system and adapt it as needed. To apply it directly:

```shell
psql "$DATABASE_URL" \
  --set ON_ERROR_STOP=1 \
  --single-transaction \
  --file extensions/subscriptions/migrations/0001_create_pgstream_subscriptions.sql
```

The installing role needs `USAGE` on `pgstream` and `INSERT` on
`pgstream.events`. It also needs ownership of every target table so future
subscription changes can drop their triggers.

## Optional reconciliation helper

The package does not install `set_subscriptions()`. The optional
[`examples/set_subscriptions.sql`](examples/set_subscriptions.sql) defines the
helper documented in the subscription guide. Copy it into your own migrations
and adapt its interface or security settings as needed.

The migration role that creates the function owns it and can execute it without
an additional grant. Add grants only if you intentionally delegate execution to
another role.

## Upgrade from pgstream 0.1

Before starting the breaking-change pgstream version, copy and run
[`upgrades/from-pgstream-0.1.sql`](upgrades/from-pgstream-0.1.sql) as a role that
can administer both the legacy pgstream objects and all subscribed tables.

The migration moves objects with `ALTER ... SET SCHEMA`. It does not copy
subscription rows or recreate target triggers: Postgres preserves object OIDs
and dependencies while moving them. It updates ownership and replaces only the
coordinator function body so future subscription changes use the new schema.

Back up the database and test the migration against a production-like copy
first. The migration aborts when the legacy source is absent or the destination
already exists.

## Uninstall

[`uninstall.sql`](uninstall.sql) deletes every subscription first, allowing the
coordinator to remove generated target triggers and functions, and then drops
the package schema. It must run as the package owner with ownership of every
subscribed table.
