use std::io::BufReader;
use std::num::NonZeroI32;
use std::sync::Arc;

use etl::config::{IntoConnectOptions, PgConnectionConfig};
use etl::error::EtlResult;
use etl_postgres::replication::extract_server_version;
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::{Client, Config, Connection, CopyOutStream, NoTls, Socket};
use tokio_rustls::rustls::{self, ClientConfig};
use tracing::{Instrument, error, info};

use crate::types::{EventIdentifier, StreamId};
use crate::utils::tokio::MakeRustlsConnect;

/// Spawns a background task to monitor a Postgres connection until it terminates.
fn spawn_postgres_connection<T>(connection: Connection<Socket, T::Stream>)
where
    T: MakeTlsConnect<Socket>,
    T::Stream: Send + 'static,
{
    let span = tracing::Span::current();
    let task = async move {
        let result = connection.await;

        match result {
            Err(err) => error!("an error occurred during the postgres connection: {}", err),
            Ok(()) => info!("postgres connection terminated successfully"),
        }
    }
    .instrument(span);

    // There is no need to track the connection task via the `JoinHandle` since the `Client`, which
    // returned the connection, will automatically terminate the connection when dropped.
    tokio::spawn(task);
}

#[derive(Debug, Clone)]
pub struct FailoverClient {
    stream_id: StreamId,
    client: Arc<Client>,
    /// Server version extracted from connection - reserved for future version-specific logic
    #[allow(dead_code)]
    server_version: Option<NonZeroI32>,
}

/// Failover client uses tokio and a persistent connection to
/// - update failover status in the pgstream.streams table
/// - copy events stream
///
/// It is created when failover is initiated, and dropped when all events are replayed and the failover is complete.
impl FailoverClient {
    /// Establishes a connection to Postgres. The connection uses TLS if configured in the
    /// supplied [`PgConnectionConfig`].
    pub async fn connect(
        stream_id: StreamId,
        pg_connection_config: PgConnectionConfig,
    ) -> EtlResult<Self> {
        match pg_connection_config.tls.enabled {
            true => FailoverClient::connect_tls(stream_id, pg_connection_config).await,
            false => FailoverClient::connect_no_tls(stream_id, pg_connection_config).await,
        }
    }

    /// Establishes a connection to Postgres without TLS encryption.
    async fn connect_no_tls(
        stream_id: StreamId,
        pg_connection_config: PgConnectionConfig,
    ) -> EtlResult<Self> {
        let config: Config = pg_connection_config.clone().with_db();

        let (client, connection) = config.connect(NoTls).await?;

        let server_version = connection
            .parameter("server_version")
            .and_then(extract_server_version);

        spawn_postgres_connection::<NoTls>(connection);

        info!("successfully connected to postgres without tls");

        Ok(FailoverClient {
            client: Arc::new(client),
            server_version,
            stream_id,
        })
    }

    /// Establishes a TLS-encrypted connection to Postgres.
    ///
    /// The connection is configured for logical replication mode
    async fn connect_tls(
        stream_id: StreamId,
        pg_connection_config: PgConnectionConfig,
    ) -> EtlResult<Self> {
        let config: Config = pg_connection_config.clone().with_db();

        let mut root_store = rustls::RootCertStore::empty();
        if pg_connection_config.tls.enabled {
            let mut root_certs_reader =
                BufReader::new(pg_connection_config.tls.trusted_root_certs.as_bytes());
            for cert in rustls_pemfile::certs(&mut root_certs_reader) {
                let cert = cert?;
                root_store.add(cert)?;
            }
        };

        let tls_config = ClientConfig::builder()
            .with_root_certificates(root_store)
            .with_no_client_auth();

        let (client, connection) = config.connect(MakeRustlsConnect::new(tls_config)).await?;

        let server_version = connection
            .parameter("server_version")
            .and_then(extract_server_version);

        spawn_postgres_connection::<MakeRustlsConnect>(connection);

        info!("successfully connected to postgres with tls");

        Ok(FailoverClient {
            client: Arc::new(client),
            server_version,
            stream_id,
        })
    }

    /// Checks if the underlying connection is closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.client.is_closed()
    }

    /// Gets events between two checkpoints (exclusive on both ends).
    pub async fn get_events_copy_stream(
        &self,
        from: &EventIdentifier,
        to: &EventIdentifier,
    ) -> EtlResult<CopyOutStream> {
        let copy_query = format!(
            r#"
                copy (
                  select id, payload, metadata, stream_id, created_at, lsn
                  from pgstream.events
                  where (created_at, id) > ('{}'::timestamptz, '{}'::uuid)
                    and (created_at, id) < ('{}'::timestamptz, '{}'::uuid)
                    and stream_id = {}
                ) to stdout with (format text);
            "#,
            from.created_at, from.id, to.created_at, to.id, self.stream_id as i64
        );

        let stream = self.client.copy_out_simple(&copy_query).await?;

        Ok(stream)
    }

    /// Updates the failover checkpoint for the given stream.
    ///
    /// This is duplicated from the [`StreamStore`] because we want to use a persistent connection during failover.
    pub async fn update_checkpoint(&self, checkpoint: &EventIdentifier) -> EtlResult<()> {
        self.client
            .execute(
                r#"
            update pgstream.streams
            set failover_checkpoint_id = $1,
                failover_checkpoint_ts = $2
            where id = $3
            "#,
                &[
                    &checkpoint.id,
                    &checkpoint.created_at,
                    &(self.stream_id as i64),
                ],
            )
            .await?;
        Ok(())
    }
}
