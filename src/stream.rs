use chrono::{DateTime, Utc};
use etl::{
    destination::Destination,
    error::EtlResult,
    replication::stream::TableCopyStream,
    store::schema::SchemaStore as SchemaStoreTrait,
    types::{Event, TableId, TableRow},
};
use futures::StreamExt;
use tokio::pin;
use tracing::info;

use crate::{
    concurrency::TimeoutBatchStream,
    config::StreamConfig,
    maintenance::run_maintenance,
    metrics,
    migrations::migrate_pgstream,
    replay_client::ReplayClient,
    sink::Sink as SinkTrait,
    store::StreamStore,
    types::{
        EventIdentifier, StreamStatus, TriggeredEvent, convert_events_from_table_rows,
        convert_stream_events_from_events,
    },
    utils::task::TaskHandle,
};

#[derive(Debug, Clone)]
pub struct PgStream<Sink, SchemaStore> {
    store: StreamStore<SchemaStore>,
    sink: Sink,
    config: StreamConfig,
    maintenance_handle: TaskHandle<DateTime<Utc>>,
}

impl<Sink, SchemaStore> PgStream<Sink, SchemaStore>
where
    Sink: SinkTrait + Sync,
    SchemaStore: SchemaStoreTrait + Sync + Send + 'static,
{
    pub async fn create(config: StreamConfig, sink: Sink, store: SchemaStore) -> EtlResult<Self> {
        // run migrations needed for pgstream
        migrate_pgstream(&config.pg_connection).await?;

        // create stream store
        let store = StreamStore::create(config.clone(), store).await?;

        // run initial maintenance synchronously during startup if due
        let (_, next_maintenance_at) = store.get_stream_state().await?;
        if Utc::now() >= next_maintenance_at {
            run_maintenance(&store).await?;
            let next = next_maintenance_at + chrono::Duration::hours(24);
            store.store_next_maintenance_at(next).await?;
        }

        Ok(Self {
            store,
            sink,
            config,
            maintenance_handle: TaskHandle::default(),
        })
    }

    /// Main processing tick for a batch of events.
    ///
    /// This will
    /// - fetch the current stream status
    /// - handle maintenance task if due
    /// - handle failover if the stream is in failover status
    /// - publish the events to the sink
    /// - enter failover if publishing fails
    ///
    /// * `events`: Batch of stream events to process.
    async fn tick(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        let (status, next_maintenance_at) = self.store.get_stream_state().await?;

        // Handle maintenance in background (non-blocking)
        self.handle_maintenance(next_maintenance_at).await?;

        // Handle failover if needed
        if let StreamStatus::Failover {
            checkpoint_event_id,
        } = status
        {
            let current_batch_event_id = events.first().unwrap().id.clone();
            self.handle_failover(&checkpoint_event_id, &current_batch_event_id)
                .await?;
        }

        // Publish events
        let checkpoint_id = match events.first() {
            Some(event) => event.id.clone(),
            None => return Ok(()),
        };

        let last_event_timestamp = events.last().map(|e| e.id.created_at);
        let event_count = events.len();

        let result = self.sink.publish_events(events).await;
        if result.is_err() {
            info!(
                "Publishing events failed, entering failover at checkpoint event id: {:?}",
                checkpoint_id
            );
            metrics::record_failover_entered(self.config.id);
            self.store
                .store_stream_status(StreamStatus::Failover {
                    checkpoint_event_id: checkpoint_id,
                })
                .await?;
            return Ok(());
        }

        // Record events processed after successful publish
        metrics::record_events_processed(self.config.id, event_count);

        // Record processing lag based on the last event's timestamp
        if let Some(timestamp) = last_event_timestamp {
            let lag_milliseconds = (Utc::now() - timestamp).num_milliseconds();
            metrics::record_processing_lag(self.config.id, lag_milliseconds);
        }

        Ok(())
    }

    /// Handles background maintenance task lifecycle
    async fn handle_maintenance(&self, next_maintenance_at: DateTime<Utc>) -> EtlResult<()> {
        // Check if previous maintenance completed and update schedule
        if let Some(completed_at) = self.maintenance_handle.take_result().await {
            // Schedule next run 24h after the SCHEDULED time, not execution time
            let next_run = next_maintenance_at + chrono::Duration::hours(24);
            self.store.store_next_maintenance_at(next_run).await?;
            info!(
                "Maintenance completed at {}, next scheduled: {}",
                completed_at, next_run
            );
        }

        // Try to start maintenance if due (won't start if already running)
        if Utc::now() >= next_maintenance_at {
            if let Some(tx) = self.maintenance_handle.try_start().await {
                let store = self.store.clone();

                tokio::spawn(async move {
                    info!("Starting background maintenance task");
                    match run_maintenance(&store).await {
                        Ok(ts) => {
                            let _ = tx.send(ts);
                        }
                        Err(e) => {
                            // Don't send result - task will appear as cancelled/idle
                            // Next tick will retry since next_maintenance_at wasn't updated
                            tracing::error!("Maintenance task failed: {:?}", e);
                        }
                    }
                });
            } else {
                // Task already running, skip
                info!("Maintenance already running, skipping");
            }
        }

        Ok(())
    }

    async fn handle_failover(
        &self,
        checkpoint_event_id: &EventIdentifier,
        current_batch_event_id: &EventIdentifier,
    ) -> EtlResult<()> {
        let failover_start = Utc::now();

        let checkpoint_event = self.store.get_checkpoint_event(checkpoint_event_id).await?;

        // Record processing lag for checkpoint event
        let lag_milliseconds = (Utc::now() - checkpoint_event.id.created_at).num_milliseconds();
        metrics::record_processing_lag(self.config.id, lag_milliseconds);

        let result = self
            .sink
            .publish_events(vec![(*checkpoint_event).clone()])
            .await;

        if result.is_err() {
            return Ok(());
        }

        info!(
            "Sink recovered, starting failover replay from checkpoint event id: {:?}",
            checkpoint_event.id
        );

        let replay_client =
            ReplayClient::connect(self.config.id, self.config.pg_connection.clone()).await?;
        let table_schema = self.store.get_events_table_schema().await?;

        let replay_stream = replay_client
            .get_events_copy_stream(&checkpoint_event.id, current_batch_event_id)
            .await?;
        let replay_stream = TableCopyStream::wrap(replay_stream, &table_schema.column_schemas, 1);
        let replay_stream = TimeoutBatchStream::wrap(replay_stream, self.config.batch.clone());
        pin!(replay_stream);

        while let Some(table_rows) = replay_stream.next().await {
            if table_rows.is_empty() {
                continue;
            }

            let table_rows = table_rows.into_iter().collect::<Result<Vec<_>, _>>()?;
            let events = convert_events_from_table_rows(table_rows, &table_schema.column_schemas)?;

            let last_event_id = events.last().unwrap().id.clone();
            let last_event_timestamp = events.last().unwrap().id.created_at;

            self.sink.publish_events(events).await?;

            // Record processing lag during failover replay
            let lag_milliseconds = (Utc::now() - last_event_timestamp).num_milliseconds();
            metrics::record_processing_lag(self.config.id, lag_milliseconds);

            replay_client.update_checkpoint(&last_event_id).await?;
        }

        // Record successful failover recovery
        let failover_duration_seconds = (Utc::now() - failover_start).num_seconds() as f64;
        metrics::record_failover_recovered(self.config.id, failover_duration_seconds);

        self.store
            .store_stream_status(StreamStatus::Healthy)
            .await?;

        info!("Failover recovery completed");

        Ok(())
    }
}

impl<Sink, SchemaStore> Destination for PgStream<Sink, SchemaStore>
where
    Sink: SinkTrait + Sync,
    SchemaStore: SchemaStoreTrait + Sync + Send + 'static,
{
    fn name() -> &'static str {
        "pgstream"
    }

    async fn truncate_table(&self, _table_id: TableId) -> EtlResult<()> {
        Ok(())
    }

    async fn write_table_rows(
        &self,
        _table_id: TableId,
        _table_rows: Vec<TableRow>,
    ) -> EtlResult<()> {
        // DataSync copies historical events - we don't need to process them.
        // We only process new events from the replication stream via write_events.
        // In failover mode, handle_failover() uses COPY to get missed events.
        Ok(())
    }

    async fn write_events(&self, events: Vec<Event>) -> EtlResult<()> {
        let schema = self.store.get_events_table_schema().await?;
        let stream_events = convert_stream_events_from_events(events, &schema.column_schemas)?;

        if stream_events.is_empty() {
            return Ok(());
        }

        self.tick(stream_events).await?;

        Ok(())
    }
}
