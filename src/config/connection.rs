//! Postgres session options for the connections pgstream opens itself.
//!
//! etl no longer exports its connection option presets, so pgstream defines
//! its own with the same timeouts and application names as before.

use std::sync::LazyLock;

use etl::config::PgConnectionOptions;

/// Connection options for running pgstream and etl helper migrations.
pub static MIGRATION_OPTIONS: LazyLock<PgConnectionOptions> = LazyLock::new(|| {
    PgConnectionOptions::builder("supabase_etl_replicator_migrations")
        .statement_timeout(300_000)
        .lock_timeout(10_000)
        .idle_in_transaction_session_timeout(60_000)
        .build()
});

/// Connection options for long-running reads such as the failover replay COPY.
///
/// Disables statement, lock, and idle timeouts.
pub static REPLICATION_OPTIONS: LazyLock<PgConnectionOptions> = LazyLock::new(|| {
    PgConnectionOptions::builder("supabase_etl_replicator_streaming")
        .statement_timeout(0)
        .lock_timeout(0)
        .idle_in_transaction_session_timeout(0)
        .build()
});

/// Connection options for short state management queries.
pub static STATE_MANAGEMENT_OPTIONS: LazyLock<PgConnectionOptions> = LazyLock::new(|| {
    PgConnectionOptions::builder("supabase_etl_replicator_state")
        .statement_timeout(30_000)
        .lock_timeout(10_000)
        .idle_in_transaction_session_timeout(60_000)
        .build()
});
