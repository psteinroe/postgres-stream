use std::time::Duration;

use chrono::{Days, NaiveDate, Utc};
use etl::config::{IntoConnectOptions, PgConnectionConfig};
use sqlx::{Connection, Executor, PgConnection, PgPool, postgres::PgPoolOptions};
use tokio::runtime::Handle;

use crate::migrations::{migrate_etl, migrate_pgstream};
use crate::test_utils::test_pg_config;

/// Test database wrapper with automatic cleanup on drop.
///
/// Each instance:
/// - creates a fresh random database inside the testcontainer,
/// - and drops the database (and terminates connections) on `Drop`.
pub struct TestDatabase {
    pub config: PgConnectionConfig,
    pub pool: PgPool,
    destroy_on_drop: bool,
}

impl TestDatabase {
    /// Creates a new test database with migrations.
    ///
    /// - Ensures the Postgres testcontainer is running.
    /// - Creates a random database.
    /// - Runs ETL and pgstream migrations.
    /// - Returns a connection pool.
    pub async fn spawn() -> Self {
        let config = test_pg_config().await;
        let pool = create_pg_database(&config).await;

        // Run ETL migrations first (required for PostgresStore)
        migrate_etl(&config)
            .await
            .expect("Failed to run ETL migrations");

        // Run pgstream migrations
        migrate_pgstream(&config)
            .await
            .expect("Failed to run pgstream migrations");

        Self {
            config,
            pool,
            destroy_on_drop: true,
        }
    }

    /// Creates a new test database without running migrations.
    ///
    /// Useful for testing migration logic itself or when you need a blank DB.
    pub async fn spawn_without_migrations() -> Self {
        let config = test_pg_config().await;
        let pool = create_pg_database(&config).await;

        Self {
            config,
            pool,
            destroy_on_drop: true,
        }
    }

    /// Optional helper: create today's partition for `pgstream.events`,
    /// like in your original `setup_database`.
    pub async fn ensure_today_partition(&self) {
        create_today_partition(&self.pool)
            .await
            .expect("Failed to create pgstream partition for today");
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        if !self.destroy_on_drop {
            return;
        }

        // Clone config so we can move it into the async cleanup task.
        let config = self.config.clone();

        // Drop can be called in contexts without a runtime; be defensive.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tokio::task::block_in_place(move || {
                // This will panic if there's no current runtime; that's why it's in catch_unwind.
                let handle = Handle::current();
                handle.block_on(async move {
                    drop_pg_database(&config).await;
                });
            });
        }));
    }
}

/// Creates a new Postgres database and returns a connection pool.
async fn create_pg_database(config: &PgConnectionConfig) -> PgPool {
    // Create the database via a single connection (connect without specifying DB).
    let mut connection = PgConnection::connect_with(&config.without_db())
        .await
        .expect("Failed to connect to Postgres (admin)");

    connection
        .execute(&*format!(r#"create database "{}";"#, config.name))
        .await
        .expect("Failed to create test database");

    // Create a connection pool to the new database.
    PgPoolOptions::new()
        .max_connections(5)
        .connect_with(config.with_db())
        .await
        .expect("Failed to connect to test database")
}

/// Drops a Postgres database and terminates all connections.
///
/// Errors are logged but do not panic.
async fn drop_pg_database(config: &PgConnectionConfig) {
    // Connect to the default database.
    let mut connection = match PgConnection::connect_with(&config.without_db()).await {
        Ok(conn) => conn,
        Err(e) => {
            eprintln!("[test] warning: failed to connect to Postgres for cleanup: {e}");
            return;
        }
    };

    // Terminate any remaining connections.
    if let Err(e) = connection
        .execute(&*format!(
            r#"
            select pg_terminate_backend(pg_stat_activity.pid)
            from pg_stat_activity
            where pg_stat_activity.datname = '{}'
            and pid <> pg_backend_pid();
            "#,
            config.name
        ))
        .await
    {
        eprintln!(
            "[test] warning: failed to terminate connections for database {}: {}",
            config.name, e
        );
    }

    // Give Postgre a moment to clean up.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drop the database.
    if let Err(e) = connection
        .execute(&*format!(r#"drop database if exists "{}";"#, config.name))
        .await
    {
        eprintln!(
            "[test] warning: failed to drop database {}: {}",
            config.name, e
        );
    } else {
        eprintln!("[test] Dropped test database {}", config.name);
    }
}

/// Create today's partition for `pgstream.events` if needed.
///
/// This is extracted from your original `setup_database` so tests
/// can opt-in when they touch that table.
pub async fn create_today_partition(pool: &PgPool) -> Result<(), sqlx::Error> {
    let today: NaiveDate = Utc::now().date_naive();
    let tomorrow = today + Days::new(1);
    let partition_name = format!("events_{}", today.format("%Y%m%d"));

    let sql = format!(
        "create table if not exists pgstream.{partition_name} \
         partition of pgstream.events \
         for values from ('{today}') to ('{tomorrow}')"
    );

    sqlx::query(&sql).execute(pool).await.map(|_| ())
}
