//! Core stream daemon logic.
//!
//! Initializes and manages the lifecycle of the pgstream replication process.

use crate::{
    config::{PipelineConfig, SinkConfig},
    migrations::migrate_etl,
    sink::memory::MemorySink,
    stream::PgStream,
};
use etl::error::EtlResult;
use etl::pipeline::Pipeline;
use etl::store::both::postgres::PostgresStore;
use tokio::signal::unix::{SignalKind, signal};
use tracing::{debug, info, warn};

/// Starts the pipeline daemon with the provided configuration.
///
/// Initializes the state store, creates PgStream as a destination,
/// and starts the ETL pipeline. Handles graceful shutdown via
/// SIGTERM and SIGINT signals.
pub async fn start_pipeline_with_config(config: PipelineConfig) -> EtlResult<()> {
    info!("starting pgstream daemon");

    log_config(&config);

    // Initialize state store for ETL pipeline state tracking
    let state_store = PostgresStore::new(config.stream.id, config.stream.pg_connection.clone());

    // Run etl migrations before starting the pipeline
    migrate_etl(&config.stream.pg_connection).await?;

    // Create sink based on configuration
    let sink = match &config.sink {
        SinkConfig::Memory => MemorySink::new(),
    };

    // Create PgStream as an ETL destination
    // It uses the same state_store for schema tracking
    let pgstream_destination =
        PgStream::create(config.stream.clone(), sink, state_store.clone()).await?;

    info!("pgstream destination created successfully");

    // Convert StreamConfig to PipelineConfig
    let pipeline_config = config.stream.into();

    // Create ETL pipeline with PgStream as destination
    let pipeline = Pipeline::new(pipeline_config, state_store, pgstream_destination);

    // Start the pipeline with signal handling
    start_pipeline_with_shutdown(pipeline).await?;

    info!("pgstream daemon completed");

    Ok(())
}

/// Logs the daemon configuration (without secrets).
fn log_config(config: &PipelineConfig) {
    log_stream_config(config);
    log_sink_config(&config.sink);
}

fn log_stream_config(config: &PipelineConfig) {
    let stream = &config.stream;
    debug!(
        stream_id = stream.id,
        host = stream.pg_connection.host,
        port = stream.pg_connection.port,
        dbname = stream.pg_connection.name,
        username = stream.pg_connection.username,
        tls_enabled = stream.pg_connection.tls.enabled,
        max_batch_size = stream.batch.max_size,
        max_batch_fill_ms = stream.batch.max_fill_ms,
        "stream configuration"
    );
}

fn log_sink_config(config: &SinkConfig) {
    match config {
        SinkConfig::Memory => {
            debug!("using memory sink");
        }
    }
}

/// Starts a pipeline and handles graceful shutdown signals.
///
/// Launches the pipeline, sets up signal handlers for SIGTERM and SIGINT,
/// and ensures proper cleanup on shutdown. The pipeline will attempt to
/// finish processing current batches before terminating.
#[tracing::instrument(skip(pipeline))]
async fn start_pipeline_with_shutdown<S, D>(mut pipeline: Pipeline<S, D>) -> EtlResult<()>
where
    S: etl::store::state::StateStore
        + etl::store::schema::SchemaStore
        + etl::store::cleanup::CleanupStore
        + Clone
        + Send
        + Sync
        + 'static,
    D: etl::destination::Destination + Clone + Send + Sync + 'static,
{
    // Start the pipeline
    pipeline.start().await?;

    // Spawn a task to listen for shutdown signals and trigger shutdown
    let shutdown_tx = pipeline.shutdown_tx();
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

        if let Err(e) = shutdown_tx.shutdown() {
            warn!("failed to send shutdown signal: {:?}", e);
            return;
        }

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
