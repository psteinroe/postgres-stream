use ctor::dtor;
use etl::config::{PgConnectionConfig, TlsConfig};
use std::sync::{Mutex, OnceLock};
use testcontainers::{ContainerRequest, ImageExt, runners::SyncRunner};
use testcontainers_modules::elasticmq::ElasticMq;
use testcontainers_modules::kafka::Kafka;
use testcontainers_modules::nats::Nats;
use testcontainers_modules::postgres::Postgres;
use testcontainers_modules::rabbitmq::RabbitMq;
use testcontainers_modules::redis::Redis;
use uuid::Uuid;

static POSTGRES_PORT: OnceLock<u16> = OnceLock::new();
static REDIS_PORT: OnceLock<u16> = OnceLock::new();
static NATS_PORT: OnceLock<u16> = OnceLock::new();
static RABBITMQ_PORT: OnceLock<u16> = OnceLock::new();
static KAFKA_PORT: OnceLock<u16> = OnceLock::new();
static ELASTICMQ_PORT: OnceLock<u16> = OnceLock::new();

// Using Mutex<Option<...>> so we can take ownership for cleanup.
static POSTGRES_CONTAINER: OnceLock<Mutex<Option<testcontainers::Container<Postgres>>>> =
    OnceLock::new();
static REDIS_CONTAINER: OnceLock<Mutex<Option<testcontainers::Container<Redis>>>> = OnceLock::new();
static NATS_CONTAINER: OnceLock<Mutex<Option<testcontainers::Container<Nats>>>> = OnceLock::new();
static RABBITMQ_CONTAINER: OnceLock<Mutex<Option<testcontainers::Container<RabbitMq>>>> =
    OnceLock::new();
static KAFKA_CONTAINER: OnceLock<Mutex<Option<testcontainers::Container<Kafka>>>> = OnceLock::new();
static ELASTICMQ_CONTAINER: OnceLock<Mutex<Option<testcontainers::Container<ElasticMq>>>> =
    OnceLock::new();

/// Cleanup function that runs at program exit to stop and remove the postgres container.
#[dtor]
fn cleanup_postgres_container() {
    if let Some(mutex) = POSTGRES_CONTAINER.get() {
        if let Ok(mut guard) = mutex.lock() {
            if let Some(container) = guard.take() {
                let _ = container.rm();
            }
        }
    }
}

/// Cleanup function that runs at program exit to stop and remove the redis container.
#[dtor]
fn cleanup_redis_container() {
    if let Some(mutex) = REDIS_CONTAINER.get() {
        if let Ok(mut guard) = mutex.lock() {
            if let Some(container) = guard.take() {
                let _ = container.rm();
            }
        }
    }
}

/// Cleanup function that runs at program exit to stop and remove the NATS container.
#[dtor]
fn cleanup_nats_container() {
    if let Some(mutex) = NATS_CONTAINER.get() {
        if let Ok(mut guard) = mutex.lock() {
            if let Some(container) = guard.take() {
                let _ = container.rm();
            }
        }
    }
}

/// Cleanup function that runs at program exit to stop and remove the RabbitMQ container.
#[dtor]
fn cleanup_rabbitmq_container() {
    if let Some(mutex) = RABBITMQ_CONTAINER.get() {
        if let Ok(mut guard) = mutex.lock() {
            if let Some(container) = guard.take() {
                let _ = container.rm();
            }
        }
    }
}

/// Cleanup function that runs at program exit to stop and remove the Kafka container.
#[dtor]
fn cleanup_kafka_container() {
    if let Some(mutex) = KAFKA_CONTAINER.get() {
        if let Ok(mut guard) = mutex.lock() {
            if let Some(container) = guard.take() {
                let _ = container.rm();
            }
        }
    }
}

/// Cleanup function that runs at program exit to stop and remove the ElasticMQ container.
#[dtor]
fn cleanup_elasticmq_container() {
    if let Some(mutex) = ELASTICMQ_CONTAINER.get() {
        if let Ok(mut guard) = mutex.lock() {
            if let Some(container) = guard.take() {
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

/// Ensures a Redis container is running and returns its port.
///
/// Uses singleton pattern to reuse the same container across tests.
pub async fn ensure_redis() -> u16 {
    *REDIS_PORT.get_or_init(|| {
        std::thread::spawn(|| {
            let container: ContainerRequest<Redis> = Redis::default().with_tag("7-alpine");

            let container = container.start().expect("Failed to start redis container");

            let port = container
                .get_host_port_ipv4(6379)
                .expect("Failed to get redis container port");

            let _ = REDIS_CONTAINER.set(Mutex::new(Some(container)));

            port
        })
        .join()
        .expect("Failed to join redis container startup thread")
    })
}

/// Ensures a NATS container is running and returns its port.
///
/// Uses singleton pattern to reuse the same container across tests.
pub async fn ensure_nats() -> u16 {
    *NATS_PORT.get_or_init(|| {
        std::thread::spawn(|| {
            let container: ContainerRequest<Nats> = Nats::default().into();

            let container = container.start().expect("Failed to start nats container");

            let port = container
                .get_host_port_ipv4(4222)
                .expect("Failed to get nats container port");

            let _ = NATS_CONTAINER.set(Mutex::new(Some(container)));

            port
        })
        .join()
        .expect("Failed to join nats container startup thread")
    })
}

/// Ensures a RabbitMQ container is running and returns its port.
///
/// Uses singleton pattern to reuse the same container across tests.
pub async fn ensure_rabbitmq() -> u16 {
    *RABBITMQ_PORT.get_or_init(|| {
        std::thread::spawn(|| {
            let container: ContainerRequest<RabbitMq> = RabbitMq::default().into();

            let container = container
                .start()
                .expect("Failed to start rabbitmq container");

            let port = container
                .get_host_port_ipv4(5672)
                .expect("Failed to get rabbitmq container port");

            let _ = RABBITMQ_CONTAINER.set(Mutex::new(Some(container)));

            port
        })
        .join()
        .expect("Failed to join rabbitmq container startup thread")
    })
}

/// Ensures a Kafka container is running and returns its port.
///
/// Uses singleton pattern to reuse the same container across tests.
pub async fn ensure_kafka() -> u16 {
    *KAFKA_PORT.get_or_init(|| {
        std::thread::spawn(|| {
            let container: ContainerRequest<Kafka> = Kafka::default().into();

            let container = container.start().expect("Failed to start kafka container");

            let port = container
                .get_host_port_ipv4(9093)
                .expect("Failed to get kafka container port");

            let _ = KAFKA_CONTAINER.set(Mutex::new(Some(container)));

            port
        })
        .join()
        .expect("Failed to join kafka container startup thread")
    })
}

/// Ensures an ElasticMQ container is running and returns its port.
///
/// Uses singleton pattern to reuse the same container across tests.
pub async fn ensure_elasticmq() -> u16 {
    *ELASTICMQ_PORT.get_or_init(|| {
        std::thread::spawn(|| {
            let container: ContainerRequest<ElasticMq> = ElasticMq::default().into();

            let container = container
                .start()
                .expect("Failed to start elasticmq container");

            let port = container
                .get_host_port_ipv4(9324)
                .expect("Failed to get elasticmq container port");

            let _ = ELASTICMQ_CONTAINER.set(Mutex::new(Some(container)));

            port
        })
        .join()
        .expect("Failed to join elasticmq container startup thread")
    })
}
