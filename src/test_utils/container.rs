use ctor::dtor;
use etl::config::{PgConnectionConfig, TlsConfig};
use std::sync::{Mutex, OnceLock};
use testcontainers::{ContainerRequest, ImageExt, runners::SyncRunner};
use testcontainers_modules::meilisearch::Meilisearch;
use testcontainers_modules::postgres::Postgres;
use uuid::Uuid;

static POSTGRES_PORT: OnceLock<u16> = OnceLock::new();
// Using Mutex<Option<...>> so we can take ownership for cleanup
static POSTGRES_CONTAINER: OnceLock<Mutex<Option<testcontainers::Container<Postgres>>>> =
    OnceLock::new();

/// Cleanup function that runs at program exit to stop and remove the postgres container
#[dtor]
fn cleanup_postgres_container() {
    if let Some(mutex) = POSTGRES_CONTAINER.get() {
        if let Ok(mut guard) = mutex.lock() {
            if let Some(container) = guard.take() {
                // rm() stops and removes the container
                let _ = container.rm();
            }
        }
    }
}

pub async fn ensure_postgres() -> u16 {
    // Use get_or_init to handle concurrent initialization attempts
    *POSTGRES_PORT.get_or_init(|| {
        // Run container startup in a separate thread to avoid runtime-in-runtime panic
        std::thread::spawn(|| {
            // Configure postgres with wal_level=logical for replication support
            let container: ContainerRequest<Postgres> =
                Postgres::default().with_tag("16-alpine").with_cmd([
                    "postgres",
                    "-c",
                    "wal_level=logical",
                    "-c",
                    "max_replication_slots=10",
                    "-c",
                    "max_wal_senders=10",
                ]);

            let container = container
                .start()
                .expect("Failed to start postgres container");

            let port = container
                .get_host_port_ipv4(5432)
                .expect("Failed to get container port");

            // Store the container in a Mutex<Option<...>> so we can take ownership later for cleanup
            let _ = POSTGRES_CONTAINER.set(Mutex::new(Some(container)));

            port
        })
        .join()
        .expect("Failed to join container startup thread")
    })
}

pub async fn test_pg_config() -> PgConnectionConfig {
    let port = (ensure_postgres()).await;

    PgConnectionConfig {
        host: "127.0.0.1".to_owned(),
        port,
        name: format!("pgstream_test_{}", Uuid::new_v4()),
        username: "postgres".to_owned(),
        password: Some("postgres".into()),
        tls: TlsConfig {
            trusted_root_certs: "".into(),
            enabled: false,
        },
        keepalive: None,
    }
}

static MEILISEARCH_PORT: OnceLock<u16> = OnceLock::new();
/// Using Mutex<Option<...>> so we can take ownership for cleanup.
static MEILISEARCH_CONTAINER: OnceLock<Mutex<Option<testcontainers::Container<Meilisearch>>>> =
    OnceLock::new();

/// Cleanup function that runs at program exit to stop and remove the Meilisearch container.
#[dtor]
fn cleanup_meilisearch_container() {
    if let Some(mutex) = MEILISEARCH_CONTAINER.get() {
        if let Ok(mut guard) = mutex.lock() {
            if let Some(container) = guard.take() {
                // rm() stops and removes the container.
                let _ = container.rm();
            }
        }
    }
}

/// Ensures a Meilisearch container is running and returns its HTTP API port.
///
/// The container is reused across tests. Used for testing Meilisearch sink.
pub async fn ensure_meilisearch() -> u16 {
    *MEILISEARCH_PORT.get_or_init(|| {
        std::thread::spawn(|| {
            let container: ContainerRequest<Meilisearch> = Meilisearch::default().into();

            let container = container
                .start()
                .expect("Failed to start Meilisearch container");

            let port = container
                .get_host_port_ipv4(7700)
                .expect("Failed to get Meilisearch port");

            let _ = MEILISEARCH_CONTAINER.set(Mutex::new(Some(container)));

            port
        })
        .join()
        .expect("Failed to join container startup thread")
    })
}
