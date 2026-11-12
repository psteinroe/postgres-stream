use std::io::BufReader;
use std::num::NonZeroI32;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use etl::config::{IntoConnectOptions, PgConnectionConfig};
use etl::error::EtlResult;
use etl_postgres::source::extract_server_version;
use etl_postgres::tokio::tls::MakeRustlsConnect;
use futures::{Stream, StreamExt};
use tokio_postgres::tls::MakeTlsConnect;
use tokio_postgres::types::ToSql;
use tokio_postgres::{Client, Config, Connection, NoTls, Row, Socket};
use tokio_rustls::rustls::{self, ClientConfig};
use tracing::{Instrument, error, info};

use crate::config::{REPLICATION_OPTIONS, STATE_MANAGEMENT_OPTIONS};
use crate::types::{EventIdentifier, StreamId, TriggeredEvent};

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
pub struct ReplayClient {
    stream_id: StreamId,
    /// Connection used for streaming replay events.
    copy_client: Arc<Client>,
    /// Separate connection used for checkpoint updates during replay.
    checkpoint_client: Arc<Client>,
    /// Server version extracted from connection - reserved for future version-specific logic
    #[allow(dead_code)]
    server_version: Option<NonZeroI32>,
}

/// Replay client uses tokio and a persistent connection to:
/// - stream events from the events table
/// - update replay checkpoint in the pgstream.streams table
///
/// It is created when event replay is initiated, and dropped when all events are replayed.
impl ReplayClient {
    /// Establishes a connection to Postgres. The connection uses TLS if configured in the
    /// supplied [`PgConnectionConfig`].
    pub async fn connect(
        stream_id: StreamId,
        pg_connection_config: PgConnectionConfig,
    ) -> EtlResult<Self> {
        match pg_connection_config.tls.enabled {
            true => ReplayClient::connect_tls(stream_id, pg_connection_config).await,
            false => ReplayClient::connect_no_tls(stream_id, pg_connection_config).await,
        }
    }

    /// Establishes a connection to Postgres without TLS encryption.
    async fn connect_no_tls(
        stream_id: StreamId,
        pg_connection_config: PgConnectionConfig,
    ) -> EtlResult<Self> {
        let copy_config: Config = pg_connection_config
            .clone()
            .with_db(Some(&REPLICATION_OPTIONS));

        let (copy_client, copy_connection) = copy_config.connect(NoTls).await?;

        let server_version = copy_connection
            .parameter("server_version")
            .and_then(extract_server_version);

        spawn_postgres_connection::<NoTls>(copy_connection);

        let checkpoint_config: Config =
            pg_connection_config.with_db(Some(&STATE_MANAGEMENT_OPTIONS));
        let (checkpoint_client, checkpoint_connection) = checkpoint_config.connect(NoTls).await?;
        spawn_postgres_connection::<NoTls>(checkpoint_connection);

        info!("successfully connected to postgres without tls");

        Ok(ReplayClient {
            copy_client: Arc::new(copy_client),
            checkpoint_client: Arc::new(checkpoint_client),
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
        let copy_config: Config = pg_connection_config
            .clone()
            .with_db(Some(&REPLICATION_OPTIONS));

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

        let (copy_client, copy_connection) = copy_config
            .connect(MakeRustlsConnect::new(tls_config.clone()))
            .await?;

        let server_version = copy_connection
            .parameter("server_version")
            .and_then(extract_server_version);

        spawn_postgres_connection::<MakeRustlsConnect>(copy_connection);

        let checkpoint_config: Config =
            pg_connection_config.with_db(Some(&STATE_MANAGEMENT_OPTIONS));
        let (checkpoint_client, checkpoint_connection) = checkpoint_config
            .connect(MakeRustlsConnect::new(tls_config))
            .await?;
        spawn_postgres_connection::<MakeRustlsConnect>(checkpoint_connection);

        info!("successfully connected to postgres with tls");

        Ok(ReplayClient {
            copy_client: Arc::new(copy_client),
            checkpoint_client: Arc::new(checkpoint_client),
            server_version,
            stream_id,
        })
    }

    /// Checks if the underlying connection is closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.copy_client.is_closed() || self.checkpoint_client.is_closed()
    }

    /// Streams events between two checkpoints (exclusive on both ends), ordered by
    /// `(created_at, id)`.
    ///
    /// Rows are read lazily from the server as the stream is polled.
    pub async fn get_events_stream(
        &self,
        from: &EventIdentifier,
        to: &EventIdentifier,
    ) -> EtlResult<impl Stream<Item = EtlResult<TriggeredEvent>> + use<>> {
        let stream_id = self.stream_id as i64;
        let params: [&(dyn ToSql + Sync); 5] = [
            &from.created_at,
            &from.id,
            &to.created_at,
            &to.id,
            &stream_id,
        ];

        let rows = self
            .copy_client
            .query_raw(
                r#"
                  select id::text as id, payload, metadata, stream_id, created_at, lsn::text as lsn
                  from pgstream.events
                  where (created_at, id) > ($1::timestamptz, $2::text::uuid)
                    and (created_at, id) < ($3::timestamptz, $4::text::uuid)
                    and stream_id = $5
                  order by created_at, id
                "#,
                params,
            )
            .await?;

        Ok(rows.map(|row| event_from_row(&row?)))
    }

    /// Updates the failover checkpoint for the given stream.
    ///
    /// This is duplicated from the [`StreamStore`] because we want to use a persistent connection during failover.
    pub async fn update_checkpoint(&self, checkpoint: &EventIdentifier) -> EtlResult<()> {
        self.checkpoint_client
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

/// Converts a row of the replay query into a [`TriggeredEvent`].
fn event_from_row(row: &Row) -> EtlResult<TriggeredEvent> {
    let id: String = row.try_get("id")?;
    let created_at: DateTime<Utc> = row.try_get("created_at")?;
    let stream_id: i64 = row.try_get("stream_id")?;
    let lsn: Option<String> = row.try_get("lsn")?;

    Ok(TriggeredEvent {
        id: EventIdentifier::new(id, created_at),
        payload: row.try_get("payload")?,
        metadata: row.try_get("metadata")?,
        stream_id: StreamId::from(stream_id as u64),
        commit_lsn: None,
        lsn: lsn.and_then(|lsn| lsn.parse().ok()),
    })
}
