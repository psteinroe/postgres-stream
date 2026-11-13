//! Core stream daemon logic.
//!
//! Initializes and manages the lifecycle of the pgstream replication process.

use crate::config::REPLICATION_OPTIONS;
use crate::{
    config::{PipelineConfig, SinkConfig},
    migrations::migrate_etl,
    sink::{AnySink, memory::MemorySink},
    slot_guard::SlotGuardStore,
    slot_recovery::{detect_slot_loss, handle_slot_recovery, record_slot_restart_lsn},
    stream::PgStream,
};
use etl::config::{IntoConnectOptions, TlsConfig};
use etl::destination::PipelineDestination;
use etl::error::{ErrorKind, EtlResult};
use etl::pipeline::Pipeline;
use etl::store::{PipelineStore, PostgresStore};
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use std::time::Duration;
use tokio::signal::unix::{SignalKind, signal};
use tracing::{error, info, warn};

/// Interval at which the slot's `restart_lsn` is recorded for slot recovery.
const SLOT_RESTART_LSN_RECORD_INTERVAL: Duration = Duration::from_secs(10);

/// Starts the pipeline daemon with the provided configuration.
///
/// Initializes the state store, creates PgStream as a destination,
/// and starts the ETL pipeline. Handles graceful shutdown via
/// SIGTERM and SIGINT signals.
///
/// If the replication slot is lost (invalidated, or missing after the events table finished its
/// initial sync), this function will automatically recover by setting a failover checkpoint and
/// restarting the pipeline. See [`crate::slot_recovery`].
pub async fn start_pipeline_with_config(config: PipelineConfig) -> EtlResult<()> {
    log_config(&config);
    validate_tls_config(&config.stream.pg_connection.tls)?;

    // Run etl migrations before starting the pipeline
    migrate_etl(&config.stream.pg_connection).await?;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect_with(
            config
                .stream
                .pg_connection
                .with_db(Some(&REPLICATION_OPTIONS)),
        )
        .await?;

    // Recovery loop - restarts the pipeline if the slot is lost
    loop {
        recover_lost_slot(&pool, config.stream.id).await?;

        let result = run_pipeline(&config, &pool).await;

        match result {
            Ok(()) => {
                info!("pgstream daemon completed");
                return Ok(());
            }
            Err(e) => match detect_slot_loss(&pool, config.stream.id).await {
                Ok(Some(loss)) => {
                    warn!(error = %e, ?loss, "replication slot lost, attempting recovery");
                    // Loop continues to recover the slot and restart the pipeline
                }
                Ok(None) => return Err(e),
                Err(detect_err) => {
                    error!(error = %detect_err, "failed to check replication slot state");
                    return Err(e);
                }
            },
        }
    }
}

/// Recovers the stream's replication slot if it was lost.
///
/// Runs before every pipeline start: etl would otherwise silently create a new slot at the
/// current WAL position for a missing slot and skip every event committed in between.
pub async fn recover_lost_slot(pool: &PgPool, stream_id: u64) -> EtlResult<()> {
    let Some(loss) = detect_slot_loss(pool, stream_id).await? else {
        return Ok(());
    };

    warn!(?loss, "replication slot lost, attempting recovery");
    if let Err(recovery_err) = handle_slot_recovery(pool, stream_id).await {
        error!(error = %recovery_err, "slot recovery failed");
        return Err(recovery_err);
    }

    Ok(())
}

/// Periodically records the `restart_lsn` of the stream's slot for slot recovery.
async fn record_slot_restart_lsn_periodically(pool: PgPool, stream_id: u64) {
    let mut interval = tokio::time::interval(SLOT_RESTART_LSN_RECORD_INTERVAL);
    loop {
        interval.tick().await;
        if let Err(err) = record_slot_restart_lsn(&pool, stream_id).await {
            warn!(error = %err, "failed to record replication slot restart_lsn");
        }
    }
}

/// Runs the pipeline once. Returns when the pipeline completes or fails.
async fn run_pipeline(config: &PipelineConfig, pool: &PgPool) -> EtlResult<()> {
    // Initialize state store for ETL pipeline state tracking. The guard turns a lost apply slot
    // into a pipeline error instead of a silently replaced slot.
    let state_store = SlotGuardStore::new(
        PostgresStore::new(config.stream.id, config.stream.pg_connection.clone()).await?,
    );

    // Create sink based on configuration.
    let sink = match &config.sink {
        SinkConfig::Memory => AnySink::Memory(MemorySink::new()),

        #[cfg(feature = "sink-elasticsearch")]
        SinkConfig::Elasticsearch(cfg) => {
            use crate::sink::elasticsearch::ElasticsearchSink;
            let s = ElasticsearchSink::new(cfg.clone()).await.map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::InvalidData,
                    "Failed to create Elasticsearch sink",
                    e.to_string()
                )
            })?;
            AnySink::Elasticsearch(s)
        }

        #[cfg(feature = "sink-redis-strings")]
        SinkConfig::RedisStrings(cfg) => {
            use crate::sink::redis_strings::RedisStringsSink;
            let sink = RedisStringsSink::new(cfg.clone()).await?;
            AnySink::RedisStrings(sink)
        }

        #[cfg(feature = "sink-redis-streams")]
        SinkConfig::RedisStreams(cfg) => {
            use crate::sink::redis_streams::RedisStreamsSink;
            let sink = RedisStreamsSink::new(cfg.clone()).await?;
            AnySink::RedisStreams(sink)
        }

        #[cfg(feature = "sink-nats")]
        SinkConfig::Nats(cfg) => {
            use crate::sink::nats::NatsSink;
            let s = NatsSink::new(cfg.clone()).await.map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::InvalidData,
                    "Failed to create NATS sink",
                    e.to_string()
                )
            })?;
            AnySink::Nats(s)
        }

        #[cfg(feature = "sink-rabbitmq")]
        SinkConfig::Rabbitmq(cfg) => {
            use crate::sink::rabbitmq::RabbitmqSink;
            let s = RabbitmqSink::new(cfg.clone()).await.map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::InvalidData,
                    "Failed to create RabbitMQ sink",
                    e.to_string()
                )
            })?;
            AnySink::Rabbitmq(s)
        }

        #[cfg(feature = "sink-webhook")]
        SinkConfig::Webhook(cfg) => {
            use crate::sink::webhook::WebhookSink;
            let s = WebhookSink::new(cfg.clone()).map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::InvalidData,
                    "Failed to create Webhook sink",
                    e.to_string()
                )
            })?;
            AnySink::Webhook(s)
        }

        #[cfg(feature = "sink-kafka")]
        SinkConfig::Kafka(cfg) => {
            use crate::sink::kafka::KafkaSink;
            let s = KafkaSink::new(cfg.clone()).map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::InvalidData,
                    "Failed to create Kafka sink",
                    e.to_string()
                )
            })?;
            AnySink::Kafka(s)
        }

        #[cfg(feature = "sink-sqs")]
        SinkConfig::Sqs(cfg) => {
            use crate::sink::sqs::SqsSink;
            let s = SqsSink::new(cfg.clone()).await.map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::InvalidData,
                    "Failed to create SQS sink",
                    e.to_string()
                )
            })?;
            AnySink::Sqs(s)
        }

        #[cfg(feature = "sink-sns")]
        SinkConfig::Sns(cfg) => {
            use crate::sink::sns::SnsSink;
            let s = SnsSink::new(cfg.clone()).await.map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::InvalidData,
                    "Failed to create SNS sink",
                    e.to_string()
                )
            })?;
            AnySink::Sns(s)
        }

        #[cfg(feature = "sink-kinesis")]
        SinkConfig::Kinesis(cfg) => {
            use crate::sink::kinesis::KinesisSink;
            let s = KinesisSink::new(cfg.clone()).await.map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::InvalidData,
                    "Failed to create Kinesis sink",
                    e.to_string()
                )
            })?;
            AnySink::Kinesis(s)
        }

        #[cfg(feature = "sink-meilisearch")]
        SinkConfig::Meilisearch(cfg) => {
            use crate::sink::meilisearch::MeilisearchSink;
            let s = MeilisearchSink::new(cfg.clone()).await.map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::InvalidData,
                    "Failed to create Meilisearch sink",
                    e.to_string()
                )
            })?;
            AnySink::Meilisearch(s)
        }

        #[cfg(feature = "sink-gcp-pubsub")]
        SinkConfig::GcpPubsub(cfg) => {
            use crate::sink::gcp_pubsub::GcpPubsubSink;
            let s = GcpPubsubSink::new(cfg.clone()).await.map_err(|e| {
                etl::etl_error!(
                    etl::error::ErrorKind::InvalidData,
                    "Failed to create GCP Pub/Sub sink",
                    e.to_string()
                )
            })?;
            AnySink::GcpPubsub(s)
        }
    };

    // Create PgStream as an ETL destination
    // It uses the same state_store for schema tracking
    let pgstream_destination =
        PgStream::create(config.stream.clone(), sink, state_store.clone()).await?;

    info!("pgstream destination created successfully");

    // Convert StreamConfig to PipelineConfig
    let pipeline_config = config.stream.clone().into();

    // Create ETL pipeline with PgStream as destination
    let pipeline = Pipeline::new(pipeline_config, state_store, pgstream_destination);

    // Record the slot's restart_lsn while the pipeline runs
    let recorder = tokio::spawn(record_slot_restart_lsn_periodically(
        pool.clone(),
        config.stream.id,
    ));

    // Start the pipeline with signal handling
    let result = start_pipeline_with_shutdown(pipeline).await;
    recorder.abort();

    result
}

/// Rejects TLS without trusted root certificates.
///
/// etl rejects this when the pipeline starts, but pgstream connects for migrations before that,
/// where a blank trust bundle fails with a certificate error instead.
fn validate_tls_config(tls: &TlsConfig) -> EtlResult<()> {
    if tls.enabled && tls.trusted_root_certs.trim().is_empty() {
        return Err(etl::etl_error!(
            ErrorKind::ConfigError,
            "Invalid source TLS configuration",
            "pg_connection.tls.trusted_root_certs must not be blank when TLS is enabled"
        ));
    }

    Ok(())
}

/// Logs an allowlist of safe operational configuration fields.
fn log_config(config: &PipelineConfig) {
    let stream = &config.stream;
    info!(
        stream_id = stream.id,
        sink_type = config.sink.kind(),
        max_batch_size = stream.batch.max_size,
        max_batch_fill_ms = stream.batch.max_fill_ms,
        failover = stream.failover,
        tls_enabled = stream.pg_connection.tls.enabled,
        "pgstream daemon starting"
    );
}

/// Starts a pipeline and handles graceful shutdown signals.
///
/// Launches the pipeline, sets up signal handlers for SIGTERM and SIGINT,
/// and ensures proper cleanup on shutdown. The pipeline will attempt to
/// finish processing current batches before terminating.
#[tracing::instrument(skip(pipeline))]
async fn start_pipeline_with_shutdown<S, D>(mut pipeline: Pipeline<S, D>) -> EtlResult<()>
where
    S: PipelineStore,
    D: PipelineDestination,
{
    // Start the pipeline
    pipeline.start().await?;

    let pipeline = Arc::new(pipeline);

    // Spawn a task to listen for shutdown signals and trigger shutdown
    let shutdown_pipeline = Arc::clone(&pipeline);
    let shutdown_handle = tokio::spawn(async move {
        // Listen for SIGTERM, sent by Kubernetes before SIGKILL during pod termination.
        //
        // If the process is killed before shutdown completes, the pipeline may become corrupted,
        // depending on the state store and destination implementations.
        let mut sigterm =
            signal(SignalKind::terminate()).expect("failed to register SIGTERM handler");

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                info!("SIGINT (Ctrl+C) received, shutting down pipeline");
            }
            _ = sigterm.recv() => {
                info!("SIGTERM received, shutting down pipeline");
            }
        }

        shutdown_pipeline.shutdown();

        info!("pipeline shutdown successfully")
    });

    // Wait for the pipeline to finish (either normally or via shutdown)
    let result = pipeline.wait().await;

    // Ensure the shutdown task is finished before returning.
    // If the pipeline finished before Ctrl+C, we want to abort the shutdown task.
    // If Ctrl+C was pressed, the shutdown task will have already triggered shutdown.
    // We don't care about the result of the shutdown_handle, but we should abort it if it's still running.
    shutdown_handle.abort();
    let _ = shutdown_handle.await;

    // Propagate any pipeline error as anyhow error
    result?;

    Ok(())
}
