# Implementation Notes

Running notes on how the subscription extraction interprets or diverges from the agreed plan.

## Design decisions

- The optional package lives under `extensions/subscriptions/`; `extensions/README.md` explicitly distinguishes these user-managed SQL packages from PostgreSQL `CREATE EXTENSION` packages.
- Subscription objects use the `pgstream_subscriptions` schema, while emitted events continue to target the core `pgstream.events` table.
- Existing SQLx migration files remain unchanged. A forward core migration removes an empty legacy installation, accepts an already-moved installation, and blocks upgrades that still contain legacy subscription rows.
- The upgrade migration moves existing objects with `ALTER ... SET SCHEMA` so subscription rows, UUIDs, generated function OIDs, and target-trigger dependencies are preserved.

## Deviations

- None yet.

## Tradeoffs

- The package keeps the existing drop-and-recreate trigger model. This intentionally requires the SQL package owner to own subscribed tables, but removes that requirement from the long-lived pgstream runtime role.
- The package migrations are plain, editable SQL and are not executed or versioned by the Rust daemon. This maximizes compatibility with user migration systems at the cost of centralized automatic upgrades.

## Open questions

- None yet.
