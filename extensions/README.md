# Database extensions

This directory contains optional, user-installed SQL packages for pgstream.
These are not PostgreSQL `CREATE EXTENSION` packages and are never installed or
migrated by the pgstream daemon.

Copy the relevant SQL into your application's migration system, review it, and
adapt schema names, ownership, and grants to your environment.

- [`subscriptions`](subscriptions/README.md) — capture table changes with database triggers
