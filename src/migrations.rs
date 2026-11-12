use etl::config::{IntoConnectOptions, PgConnectionConfig};
use etl::error::EtlResult;
use etl::store::PostgresStore;
use sqlx::{
    Executor,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tracing::info;

use crate::config::MIGRATION_OPTIONS;

/// Number of database connections to use for the migration pool.
const NUM_POOL_CONNECTIONS: u32 = 1;

/// Runs etl's state store migrations.
///
/// etl owns these migrations (`crates/etl/migrations/postgres_store`) and applies them in
/// [`PostgresStore::new`], recording them in `etl._sqlx_migrations`. The base migration is
/// byte-identical to the one pgstream used to vendor, so databases migrated by earlier
/// pgstream versions upgrade in place.
pub async fn migrate_etl(connection_config: &PgConnectionConfig) -> EtlResult<()> {
    info!("applying etl state store migrations");

    // The pipeline id only scopes the store's queries; migrations are global.
    PostgresStore::new(0, connection_config.clone()).await?;

    info!("etl state store migrations successfully applied");

    Ok(())
}

/// Runs database migrations on pgstream state store.
///
/// Creates a connection pool to the source database, sets up the `pgstream` schema,
/// and applies all pending migrations. The migrations are run in the `pgstream` schema
/// to avoid cluttering the public schema with migration metadata tables created by `sqlx`.
pub async fn migrate_pgstream(connection_config: &PgConnectionConfig) -> Result<(), sqlx::Error> {
    let options: PgConnectOptions = connection_config.with_db(Some(&MIGRATION_OPTIONS));

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

#[cfg(test)]
mod tests {
    use std::path::Path;

    /// The migration that added `pgstream.emit_events_schema_change()`. Older migrations ran
    /// before etl tracked the events table schema and are exempt.
    const EMIT_SCHEMA_CHANGE_VERSION: u64 = 1_794_600_000_002;

    const EMIT_SCHEMA_CHANGE_CALL: &str = "pgstream.emit_events_schema_change()";

    /// Lowercases `sql`, strips line comments and quotes and collapses whitespace.
    fn normalize(sql: &str) -> String {
        sql.lines()
            .map(|line| line.split("--").next().unwrap_or_default())
            .collect::<Vec<_>>()
            .join(" ")
            .to_lowercase()
            .replace('"', "")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Returns the byte offset of the last `alter table pgstream.events` in normalized `sql`.
    fn last_events_alter(sql: &str) -> Option<usize> {
        let mut last = None;
        for (offset, _) in sql.match_indices("alter table ") {
            let mut tokens = sql[offset + "alter table ".len()..].split(' ').peekable();
            if tokens.peek() == Some(&"if") {
                tokens.next();
                tokens.next();
            }
            if tokens.peek() == Some(&"only") {
                tokens.next();
            }
            let table = tokens.next().unwrap_or_default();
            if table.trim_end_matches(';') == "pgstream.events" {
                last = Some(offset);
            }
        }
        last
    }

    /// Returns why `sql` does not announce its schema change of `pgstream.events`, if it alters
    /// the table.
    fn schema_change_violation(sql: &str) -> Option<&'static str> {
        if sql.trim_start().starts_with("-- no-transaction") {
            return last_events_alter(&normalize(sql)).map(
                |_| "must run in a transaction so the schema change message is transactional",
            );
        }

        let sql = normalize(sql);
        let alter = last_events_alter(&sql)?;
        match sql.rfind(EMIT_SCHEMA_CHANGE_CALL) {
            Some(call) if call > alter => None,
            _ => Some("must call pgstream.emit_events_schema_change() after altering the table"),
        }
    }

    #[test]
    fn migrations_altering_events_announce_the_schema_change() {
        let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");

        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_none_or(|extension| extension != "sql") {
                continue;
            }
            let file_name = path.file_name().unwrap().to_string_lossy().into_owned();
            let version: u64 = file_name
                .split('_')
                .next()
                .and_then(|version| version.parse().ok())
                .unwrap_or_else(|| panic!("migration {file_name} has no version prefix"));
            if version <= EMIT_SCHEMA_CHANGE_VERSION {
                continue;
            }

            let sql = std::fs::read_to_string(&path).unwrap();
            if let Some(violation) = schema_change_violation(&sql) {
                panic!(
                    "migration {file_name} alters pgstream.events and {violation}; see \
                     migrations/README.md"
                );
            }
        }
    }

    #[test]
    fn schema_change_violation_detects_unannounced_alters() {
        let announced = "alter table pgstream.events add column foo text;\n\
                         select pgstream.emit_events_schema_change();";
        assert_eq!(schema_change_violation(announced), None);

        let quoted = "ALTER TABLE IF EXISTS ONLY \"pgstream\".\"events\"\n  DROP COLUMN foo;";
        assert!(schema_change_violation(quoted).is_some());

        let call_before_alter = "select pgstream.emit_events_schema_change();\n\
                                 alter table pgstream.events add column foo text;";
        assert!(schema_change_violation(call_before_alter).is_some());

        let commented_call = "alter table pgstream.events add column foo text;\n\
                              -- select pgstream.emit_events_schema_change();";
        assert!(schema_change_violation(commented_call).is_some());

        let no_transaction = "-- no-transaction\n\
                              alter table pgstream.events add column foo text;\n\
                              select pgstream.emit_events_schema_change();";
        assert!(schema_change_violation(no_transaction).is_some());

        let other_table = "alter table pgstream.events_20240101 add column foo text;\n\
                           alter table pgstream.streams add column foo text;";
        assert_eq!(schema_change_violation(other_table), None);
    }
}
