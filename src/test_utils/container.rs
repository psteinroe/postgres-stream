use etl::config::{PgConnectionConfig, TlsConfig};
use testcontainers::{ContainerAsync, ImageExt, runners::AsyncRunner};
use testcontainers_modules::postgres::Postgres;
use tokio::sync::OnceCell;
use uuid::Uuid;

static POSTGRES_PORT: OnceCell<u16> = OnceCell::const_new();
static POSTGRES_CONTAINER: OnceCell<ContainerAsync<Postgres>> = OnceCell::const_new();

pub async fn ensure_postgres() -> u16 {
    // Use get_or_init to handle concurrent initialization attempts
    let port = POSTGRES_PORT
        .get_or_init(|| async {
            let container = Postgres::default()
                .with_tag("16-alpine")
                .start()
                .await
                .expect("Failed to start postgres container");

            let port = container
                .get_host_port_ipv4(5432)
                .await
                .expect("Failed to get container port");

            // Store the container (ignore if already set by another thread)
            let _ = POSTGRES_CONTAINER.set(container);

            port
        })
        .await;

    *port
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
    }
}
