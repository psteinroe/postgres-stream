//! End-to-end tests for Postgres connections over TLS.
//!
//! Every test starts its own Postgres container that only accepts `hostssl` connections with
//! `scram-sha-256` authentication, using a CA and server certificate generated for the test.

use std::time::Duration;

use etl::config::{IntoConnectOptions, PgConnectionConfig, TcpKeepaliveConfig, TlsConfig};
use etl::error::ErrorKind;
use etl::pipeline::Pipeline;
use etl::store::PostgresStore;
use etl_postgres::tokio::tls::MakeRustlsConnect;
use postgres_stream::config::{BatchConfig, PipelineConfig, SinkConfig, StreamConfig};
use postgres_stream::core::start_pipeline_with_config;
use postgres_stream::migrations::migrate_etl;
use postgres_stream::replay_client::ReplayClient;
use postgres_stream::stream::PgStream;
use postgres_stream::test_utils::{
    FailableSink, create_today_partition, test_postgres_version, unique_pipeline_id,
};
use postgres_stream::utils::crypto::init_crypto_provider;
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
use sqlx::PgPool;
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, ImageExt};
use testcontainers_modules::postgres::Postgres;
use tokio_postgres::config::ChannelBinding;
use tokio_rustls::rustls::pki_types::pem::PemObject;
use tokio_rustls::rustls::{ClientConfig, RootCertStore, pki_types::CertificateDer};

/// Enables TLS with the generated server certificate and only allows `hostssl` connections.
const TLS_INIT_SCRIPT: &str = r#"
cp /tls/server.crt /tls/server.key "$PGDATA"/
chmod 600 "$PGDATA/server.key"
cat >> "$PGDATA/postgresql.conf" <<EOF
ssl = on
ssl_cert_file = 'server.crt'
ssl_key_file = 'server.key'
EOF
cat > "$PGDATA/pg_hba.conf" <<EOF
local all all trust
hostssl all all all scram-sha-256
EOF
"#;

/// A test certificate authority.
struct TestCa {
    issuer: Issuer<'static, KeyPair>,
    pem: String,
}

impl TestCa {
    fn generate(name: &str) -> Self {
        let mut params = CertificateParams::default();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name.push(DnType::CommonName, name);
        let key = KeyPair::generate().expect("Failed to generate CA key");
        let pem = params
            .self_signed(&key)
            .expect("Failed to self-sign CA certificate")
            .pem();

        Self {
            issuer: Issuer::new(params, key),
            pem,
        }
    }

    /// Issues a server certificate for `localhost` and `127.0.0.1`, returning the certificate and
    /// key as PEM.
    fn issue_server_cert(&self) -> (String, String) {
        let params = CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
            .expect("Failed to create server certificate params");
        let key = KeyPair::generate().expect("Failed to generate server key");
        let cert = params
            .signed_by(&key, &self.issuer)
            .expect("Failed to sign server certificate");

        (cert.pem(), key.serialize_pem())
    }
}

/// A Postgres container that requires TLS, with the CA that signed its certificate.
struct TlsPostgres {
    _container: ContainerAsync<Postgres>,
    port: u16,
    ca: TestCa,
}

impl TlsPostgres {
    async fn start() -> Self {
        init_crypto_provider();

        let ca = TestCa::generate("pgstream test CA");
        let (cert_pem, key_pem) = ca.issue_server_cert();

        let container = Postgres::default()
            .with_tag(format!("{}-alpine", test_postgres_version()))
            .with_cmd([
                "postgres",
                "-c",
                "wal_level=logical",
                "-c",
                "max_replication_slots=10",
                "-c",
                "max_wal_senders=10",
            ])
            .with_copy_to("/tls/server.crt", cert_pem.into_bytes())
            .with_copy_to("/tls/server.key", key_pem.into_bytes())
            .with_copy_to(
                "/docker-entrypoint-initdb.d/tls.sh",
                TLS_INIT_SCRIPT.as_bytes().to_vec(),
            )
            .start()
            .await
            .expect("Failed to start postgres container");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("Failed to get container port");

        Self {
            _container: container,
            port,
            ca,
        }
    }

    /// Connection config with TLS enabled, trusting `trusted_root_certs`.
    fn config(&self, trusted_root_certs: &str) -> PgConnectionConfig {
        PgConnectionConfig {
            host: "127.0.0.1".to_owned(),
            hostaddr: None,
            port: self.port,
            name: "postgres".to_owned(),
            username: "postgres".to_owned(),
            password: Some("postgres".into()),
            tls: TlsConfig {
                trusted_root_certs: trusted_root_certs.to_owned(),
                enabled: true,
            },
            keepalive: TcpKeepaliveConfig::default(),
        }
    }

    /// Connection config with TLS enabled, trusting the CA that signed the server certificate.
    fn trusted_config(&self) -> PgConnectionConfig {
        self.config(&self.ca.pem)
    }
}

fn stream_config(pg_connection: PgConnectionConfig) -> StreamConfig {
    StreamConfig {
        id: unique_pipeline_id(),
        pg_connection,
        batch: BatchConfig {
            max_size: 100,
            max_fill_ms: 200,
        },
        failover: false,
    }
}

/// Polls `condition` until it holds or the timeout elapses.
async fn wait_until<F, Fut>(description: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let result = tokio::time::timeout(Duration::from_secs(30), async {
        while !condition().await {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await;
    assert!(result.is_ok(), "timed out waiting until {description}");
}

/// Inserts `count` events in a single transaction and returns their ids in order.
async fn insert_events(pool: &PgPool, stream_config: &StreamConfig, count: i32) -> Vec<String> {
    sqlx::query_scalar(
        "insert into pgstream.events (payload, stream_id) \
         select jsonb_build_object('seq', seq), $2 from generate_series(1, $1) as seq \
         returning id::text",
    )
    .bind(count)
    .bind(stream_config.id as i64)
    .fetch_all(pool)
    .await
    .expect("Failed to insert events")
}

async fn failover_checkpoint(pool: &PgPool, stream_config: &StreamConfig) -> Option<String> {
    sqlx::query_scalar("select failover_checkpoint_id from pgstream.streams where id = $1")
        .bind(stream_config.id as i64)
        .fetch_optional(pool)
        .await
        .expect("Failed to read stream state")
        .flatten()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pipeline_streams_and_replays_over_tls() {
    let postgres = TlsPostgres::start().await;
    let stream_config = stream_config(postgres.trusted_config());

    // pgstream's sqlx connections: etl's state store and pgstream's migrations.
    let state_store = PostgresStore::new(stream_config.id, stream_config.pg_connection.clone())
        .await
        .expect("Failed to create PostgresStore over TLS");
    let sink = FailableSink::new();
    let pgstream = PgStream::create(stream_config.clone(), sink.clone(), state_store.clone())
        .await
        .expect("Failed to create PgStream over TLS");
    let pool = PgPool::connect_with(stream_config.pg_connection.with_db(None))
        .await
        .expect("Failed to connect over TLS");
    create_today_partition(&pool)
        .await
        .expect("Failed to create partition");

    // etl's replication connections.
    let mut pipeline = Pipeline::new(stream_config.clone().into(), state_store, pgstream);
    pipeline.start().await.expect("Failed to start pipeline");
    wait_until("the events table is synced", || async {
        let states: Vec<String> = sqlx::query_scalar(
            "select state::text from etl.replication_state where pipeline_id = $1 and is_current",
        )
        .bind(stream_config.id as i64)
        .fetch_all(&pool)
        .await
        .unwrap();
        !states.is_empty()
            && states
                .iter()
                .all(|state| state == "sync_done" || state == "ready")
    })
    .await;

    // The first batch fails, so the stream enters failover with the batch's first event as
    // checkpoint.
    sink.fail_on_call(0);
    let missed = insert_events(&pool, &stream_config, 3).await;
    wait_until("the stream enters failover", || async {
        failover_checkpoint(&pool, &stream_config).await.is_some()
    })
    .await;

    // The next batch publishes the checkpoint event and replays the rest of the missed events
    // through the replay client's own TLS connections.
    let next = insert_events(&pool, &stream_config, 1).await;
    wait_until("the stream recovers from failover", || async {
        failover_checkpoint(&pool, &stream_config).await.is_none()
    })
    .await;

    let walsenders_over_tls: i64 = sqlx::query_scalar(
        "select count(*) from pg_stat_activity a join pg_stat_ssl s using (pid) \
         where a.backend_type = 'walsender' and s.ssl",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(walsenders_over_tls > 0, "replication should use TLS");

    pipeline.shutdown();
    pipeline.wait().await.expect("Pipeline should shut down");

    let delivered: Vec<String> = sink
        .events()
        .await
        .into_iter()
        .map(|event| event.id.id)
        .collect();
    let expected: Vec<String> = missed.into_iter().chain(next).collect();
    assert_eq!(delivered, expected);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_untrusted_server_certificate_is_rejected() {
    let postgres = TlsPostgres::start().await;
    let wrong_ca = TestCa::generate("untrusted test CA");
    let config = postgres.config(&wrong_ca.pem);

    // pgstream's sqlx connections.
    let err = migrate_etl(&config)
        .await
        .expect_err("state store should not connect with an untrusted certificate");
    assert!(
        format!("{err:?}").contains("UnknownIssuer"),
        "unexpected error: {err:?}"
    );

    // The replay client.
    let err = ReplayClient::connect(1, config.clone())
        .await
        .expect_err("replay client should not connect with an untrusted certificate");
    assert!(
        format!("{err:?}").contains("UnknownIssuer"),
        "unexpected error: {err:?}"
    );

    // etl's replication connection, with a state store that trusts the server.
    let stream_config = stream_config(config);
    let state_store = PostgresStore::new(stream_config.id, postgres.trusted_config())
        .await
        .expect("Failed to create PostgresStore");
    let mut trusted_stream_config = stream_config.clone();
    trusted_stream_config.pg_connection = postgres.trusted_config();
    let pgstream = PgStream::create(
        trusted_stream_config,
        FailableSink::new(),
        state_store.clone(),
    )
    .await
    .expect("Failed to create PgStream");
    let mut pipeline = Pipeline::new(stream_config.into(), state_store, pgstream);
    let err = pipeline
        .start()
        .await
        .expect_err("replication should not connect with an untrusted certificate");
    assert!(
        format!("{err:?}").contains("UnknownIssuer"),
        "unexpected error: {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_tls_without_trusted_root_certs_is_a_config_error() {
    let postgres = TlsPostgres::start().await;
    let blank_config = postgres.config(" \n");

    // pgstream rejects it before connecting for migrations.
    let config = PipelineConfig {
        stream: stream_config(blank_config.clone()),
        sink: SinkConfig::Memory,
    };
    let err = tokio::time::timeout(Duration::from_secs(30), start_pipeline_with_config(config))
        .await
        .expect("startup should fail instead of running")
        .expect_err("blank trusted_root_certs should be rejected");
    assert_eq!(
        err.kind(),
        ErrorKind::ConfigError,
        "unexpected error: {err:?}"
    );
    assert!(
        format!("{err:?}").contains("trusted_root_certs"),
        "unexpected error: {err:?}"
    );

    // etl rejects it when the pipeline starts.
    let stream_config = stream_config(blank_config);
    let state_store = PostgresStore::new(stream_config.id, postgres.trusted_config())
        .await
        .expect("Failed to create PostgresStore");
    let mut trusted_stream_config = stream_config.clone();
    trusted_stream_config.pg_connection = postgres.trusted_config();
    let pgstream = PgStream::create(
        trusted_stream_config,
        FailableSink::new(),
        state_store.clone(),
    )
    .await
    .expect("Failed to create PgStream");
    let mut pipeline = Pipeline::new(stream_config.into(), state_store, pgstream);
    let err = pipeline
        .start()
        .await
        .expect_err("blank trusted_root_certs should be rejected");
    assert_eq!(
        err.kind(),
        ErrorKind::ConfigError,
        "unexpected error: {err:?}"
    );
    assert!(
        format!("{err:?}").contains("trusted_root_certs"),
        "unexpected error: {err:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn test_replay_tls_connector_supports_scram_channel_binding() {
    let postgres = TlsPostgres::start().await;
    let config = postgres.trusted_config();

    let mut root_store = RootCertStore::empty();
    for cert in CertificateDer::pem_slice_iter(config.tls.trusted_root_certs.as_bytes()) {
        root_store.add(cert.unwrap()).unwrap();
    }
    let tls_config = ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    // Fails unless the server accepts SCRAM-SHA-256-PLUS with the TLS channel binding.
    let mut pg_config: tokio_postgres::Config = config.with_db(None);
    pg_config.channel_binding(ChannelBinding::Require);
    let (client, connection) = pg_config
        .connect(MakeRustlsConnect::new(tls_config))
        .await
        .expect("Failed to connect with channel binding");
    tokio::spawn(connection);

    let ssl: bool = client
        .query_one(
            "select ssl from pg_stat_ssl where pid = pg_backend_pid()",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(ssl);
}
