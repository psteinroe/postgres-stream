use etl_config::shared::{IntoConnectOptions, PgConnectionConfig};
use sqlx::{
    Executor,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tracing::info;

/// Number of database connections to use for the migration pool.
const NUM_POOL_CONNECTIONS: u32 = 1;

/// Runs database migrations on the state store.
///
/// Creates a connection pool to the source database, sets up the `etl` schema,
/// and applies all pending migrations. The migrations are run in the `etl` schema
/// to avoid cluttering the public schema with migration metadata tables created by `sqlx`.
pub async fn migrate_etl(connection_config: &PgConnectionConfig) -> Result<(), sqlx::Error> {
    let options: PgConnectOptions = connection_config.with_db();

    let pool = PgPoolOptions::new()
        .max_connections(NUM_POOL_CONNECTIONS)
        .min_connections(NUM_POOL_CONNECTIONS)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                // Create the etl schema if it doesn't exist
                conn.execute("create schema if not exists etl;").await?;
                // We set the search_path to etl so that the _sqlx_migrations
                // metadata table is created inside that schema instead of the public
                // schema
                conn.execute("set search_path = 'etl';").await?;

                Ok(())
            })
        })
        .connect_with(options)
        .await?;

    info!("applying migrations in the state store before starting replicator");

    let migrator = sqlx::migrate!("./etl-migrations");
    migrator.run(&pool).await?;

    info!("migrations successfully applied in the state store");

    Ok(())
}

/// Runs database migrations on pgstream state store.
///
/// Creates a connection pool to the source database, sets up the `pgstream` schema,
/// and applies all pending migrations. The migrations are run in the `pgstream` schema
/// to avoid cluttering the public schema with migration metadata tables created by `sqlx`.
pub async fn migrate_pgstream(connection_config: &PgConnectionConfig) -> Result<(), sqlx::Error> {
    let options: PgConnectOptions = connection_config.with_db();

    let pool = PgPoolOptions::new()
        .max_connections(NUM_POOL_CONNECTIONS)
        .min_connections(NUM_POOL_CONNECTIONS)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                // Create uuid-ossp extension first
                conn.execute("create extension if not exists \"uuid-ossp\";")
                    .await?;
                // Create the pgstream schema if it doesn't exist
                conn.execute("create schema if not exists pgstream;")
                    .await?;
                // We set the search_path to pgstream so that the _sqlx_migrations
                // metadata table is created inside that schema instead of the public
                // schema
                conn.execute("set search_path = 'pgstream';").await?;

                Ok(())
            })
        })
        .connect_with(options)
        .await?;

    info!("applying migrations in pgstream schema");

    let migrator = sqlx::migrate!("./migrations");
    migrator.run(&pool).await?;

    info!("migrations successfully applied in pgstream schema");

    Ok(())
}

/// Runs database migrations on pgstream state store using a connection string.
/// This is a simpler version suitable for testing.
pub async fn migrate_pgstream_from_url(database_url: &str) -> Result<(), sqlx::Error> {
    let pool = PgPoolOptions::new()
        .max_connections(NUM_POOL_CONNECTIONS)
        .min_connections(NUM_POOL_CONNECTIONS)
        .after_connect(|conn, _meta| {
            Box::pin(async move {
                // Create uuid-ossp extension first
                conn.execute("create extension if not exists \"uuid-ossp\";")
                    .await?;
                // Create the pgstream schema if it doesn't exist
                conn.execute("create schema if not exists pgstream;")
                    .await?;
                // We set the search_path to pgstream so that the _sqlx_migrations
                // metadata table is created inside that schema instead of the public
                // schema
                conn.execute("set search_path = 'pgstream';").await?;

                Ok(())
            })
        })
        .connect(database_url)
        .await?;

    info!("applying migrations in pgstream schema");

    let migrator = sqlx::migrate!("./migrations");
    migrator.run(&pool).await?;

    info!("migrations successfully applied in pgstream schema");

    Ok(())
}
