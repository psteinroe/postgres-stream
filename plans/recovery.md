 Replay & Slot Recovery Implementation Plan

 Overview

 Two failure scenarios requiring replay mechanisms:

 | Scenario          | Cause                               | Slot Status | Recovery
        |
 |-------------------|-------------------------------------|-------------|-----------------------------
 -------|
 | Sink Data Loss    | Consumer bug, sink failure          | Healthy     | User triggers replay via SQL
        |
 | Slot Invalidation | WAL exceeded max_slot_wal_keep_size | Dead        | Daemon auto-recovers with
 new slot |

 ---
 Scenario 1: Sink Lost Data (User-Initiated Replay)

 Trigger Mechanism

 User sets replay_from_ts via SQL UPDATE:

 -- Replay from specific timestamp
 UPDATE pgstream.streams
 SET replay_from_ts = '2025-01-15 10:00:00'
 WHERE id = 1;

 -- Replay all events in retention window
 UPDATE pgstream.streams
 SET replay_from_ts = (SELECT MIN(created_at) FROM pgstream.events WHERE stream_id = 1)
 WHERE id = 1;

 Detecting Replay Request

 For the daemon to detect replay_from_ts changes, we need to add pgstream.streams to the publication:

 -- Modify publication to include streams table for control messages
 ALTER PUBLICATION pgstream_stream_1 ADD TABLE pgstream.streams;

 -- Or create a separate publication for control messages
 CREATE PUBLICATION pgstream_control FOR TABLE pgstream.streams;

 Alternative approach: Add a trigger on pgstream.streams that inserts a "control event" into
 pgstream.events when replay_from_ts is set:

 CREATE OR REPLACE FUNCTION pgstream.notify_replay_request()
 RETURNS trigger AS $$
 BEGIN
     IF NEW.replay_from_ts IS NOT NULL AND (OLD.replay_from_ts IS NULL OR OLD.replay_from_ts !=
 NEW.replay_from_ts) THEN
         INSERT INTO pgstream.events (payload, stream_id, lsn)
         VALUES (
             jsonb_build_object(
                 'type', 'control',
                 'action', 'replay',
                 'replay_from_ts', NEW.replay_from_ts,
                 'replay_to_ts', NEW.replay_to_ts
             ),
             NEW.id,
             pg_current_wal_lsn()
         );
     END IF;
     RETURN NEW;
 END;
 $$ LANGUAGE plpgsql;

 CREATE TRIGGER on_replay_request
 AFTER UPDATE OF replay_from_ts ON pgstream.streams
 FOR EACH ROW EXECUTE FUNCTION pgstream.notify_replay_request();

 This approach keeps the existing publication simple (events only) and uses a control event to signal
 replay requests.

 Database Schema Changes

 -- Migration: Add replay support to streams table
 ALTER TABLE pgstream.streams
 ADD COLUMN replay_from_ts timestamptz,
 ADD COLUMN replay_to_ts timestamptz;           -- Optional: limit replay range

 -- Add LSN column to events table for precise slot recovery
 ALTER TABLE pgstream.events
 ADD COLUMN lsn pg_lsn;

 -- Index for efficient LSN lookups during recovery
 CREATE INDEX events_lsn_stream_idx ON pgstream.events (stream_id, lsn);

 Stream Status Extension

 // src/types/stream.rs
 pub enum StreamStatus {
     Healthy,
     Failover { checkpoint_event_id: EventIdentifier },
     Replay { from_ts: DateTime<Utc>, to_ts: Option<DateTime<Utc>> },  // NEW
 }

 Processing Logic

 // src/stream.rs - Modified tick()
 async fn tick(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
     let state = self.store.get_stream_state().await?;

     // Check for pending replay
     if let Some(replay_from_ts) = state.replay_from_ts {
         return self.handle_user_replay(replay_from_ts, state.replay_to_ts).await;
     }

     // Existing failover handling...
     if let StreamStatus::Failover { checkpoint_event_id } = state.status {
         // ... existing code
     }

     // Normal event processing...
 }

 async fn handle_user_replay(
     &self,
     from_ts: DateTime<Utc>,
     to_ts: Option<DateTime<Utc>>,
 ) -> EtlResult<()> {
     info!("Starting user-initiated replay from {}", from_ts);

     let client = FailoverClient::connect(self.config.id, self.config.pg_connection.clone()).await?;

     let to_ts = to_ts.unwrap_or_else(Utc::now);
     let stream = client.get_events_by_timestamp_range(&from_ts, &to_ts).await?;

     let stream = TableCopyStream::wrap(stream, &schema.column_schemas, 1);
     let stream = TimeoutBatchStream::wrap(stream, self.config.batch.clone());
     pin!(stream);

     while let Some(batch) = stream.next().await {
         let events = convert_events_from_table_rows(batch?, &schema.column_schemas)?;
         self.sink.publish_events(events).await?;
     }

     // Clear replay request
     self.store.clear_replay_request().await?;
     info!("User-initiated replay completed");

     Ok(())
 }

 FailoverClient Extension

 // src/failover_client.rs - New method
 pub async fn get_events_by_timestamp_range(
     &self,
     from: &DateTime<Utc>,
     to: &DateTime<Utc>,
 ) -> EtlResult<CopyOutStream> {
     let query = format!(
         r#"
         COPY (
             SELECT id, payload, metadata, stream_id, created_at
             FROM pgstream.events
             WHERE created_at >= '{}'::timestamptz
               AND created_at <= '{}'::timestamptz
               AND stream_id = {}
             ORDER BY created_at, id
         ) TO STDOUT WITH (FORMAT text)
         "#,
         from, to, self.stream_id as i64
     );

     Ok(self.client.copy_out_simple(&query).await?)
 }

 Files to Modify

 | File                           | Changes                                          |
 |--------------------------------|--------------------------------------------------|
 | migrations/NNNN_add_replay.sql | Add replay_from_ts, replay_to_ts columns         |
 | src/types/stream.rs            | Add Replay variant to StreamStatus               |
 | src/store.rs                   | Add get_replay_request(), clear_replay_request() |
 | src/stream.rs                  | Add handle_user_replay(), check in tick()        |
 | src/failover_client.rs         | Add get_events_by_timestamp_range()              |

 ---
 Scenario 2: Slot Invalidation (Automatic Recovery)

 Key Insight: Store LSN in Events Table for Precise Recovery

 When a slot is invalidated, PostgreSQL retains the confirmed_flush_lsn in pg_replication_slots. By
 storing the LSN in each event row, we can precisely find the replay point:

 -- Query invalidated slot's LSN
 SELECT confirmed_flush_lsn FROM pg_replication_slots
 WHERE slot_name = 'pgstream_stream_1_slot' AND wal_status = 'lost';
 -- Returns: 0/1234567

 -- Find first event not yet confirmed by the slot
 SELECT MIN(created_at), MIN(id) FROM pgstream.events
 WHERE lsn > '0/1234567'::pg_lsn AND stream_id = 1;
 -- Returns: exact replay starting point

 Schema Change: Add LSN to Events Table

 -- Add LSN column to events table
 ALTER TABLE pgstream.events
 ADD COLUMN lsn pg_lsn DEFAULT pg_current_wal_lsn();

 -- Create index for efficient LSN lookups
 CREATE INDEX events_lsn_idx ON pgstream.events (lsn);

 Trigger Modification

 Update the trigger function to capture LSN at insert time:

 -- In sync_database_trigger(), modify the INSERT statement:
 INSERT INTO pgstream.events (payload, stream_id, lsn)
 SELECT elem, %L, pg_current_wal_lsn()
 FROM jsonb_array_elements(v_jsonb_output) AS t(elem);

 Recovery Flow with LSN

 ┌─────────────────────────────────────────────────────────────────┐
 │                    Slot Invalidation Detected                   │
 └─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
 ┌─────────────────────────────────────────────────────────────────┐
 │   Get confirmed_flush_lsn from pg_replication_slots             │
 │   (slot still exists with wal_status='lost')                    │
 └─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
 ┌─────────────────────────────────────────────────────────────────┐
 │   Query: SELECT MIN(created_at) FROM pgstream.events            │
 │          WHERE lsn > confirmed_flush_lsn AND stream_id = ?      │
 └─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
 ┌─────────────────────────────────────────────────────────────────┐
 │   1. Drop old invalidated slot                                  │
 │   2. Set replay_from_ts to the found timestamp                  │
 │   3. Create new slot and resume pipeline                        │
 │   4. Replay from exact point, then continue normally            │
 └─────────────────────────────────────────────────────────────────┘

 This approach gives us exact recovery without any data loss or duplicate processing!

 Detection

 When the etl Pipeline encounters an invalid slot, it returns an error. We need to:
 1. Catch the error from Pipeline::start() or during runtime
 2. Detect if it's a slot invalidation error (pattern match on error message)
 3. Query pg_replication_slots for confirmed_flush_lsn before dropping the slot
 4. Trigger recovery

 Stream Status Extension

 // src/types/stream.rs
 pub enum StreamStatus {
     Healthy,
     Failover { checkpoint_event_id: EventIdentifier },
     Replay { from_ts: DateTime<Utc>, to_ts: Option<DateTime<Utc>> },
     SlotRecovery { needs_manual_checkpoint: bool },  // NEW
 }

 Recovery Flow

 ┌─────────────────────────────────────────────────────────────────┐
 │                    Slot Invalidation Detected                   │
 └─────────────────────────────────────────────────────────────────┘
                                 │
                                 ▼
 ┌─────────────────────────────────────────────────────────────────┐
 │                 Check: Was there a checkpoint?                   │
 │     (failover_checkpoint_ts IS NOT NULL in pgstream.streams)    │
 └─────────────────────────────────────────────────────────────────┘
                     │                           │
                     │ YES                       │ NO
                     ▼                           ▼
 ┌───────────────────────────┐   ┌─────────────────────────────────┐
 │  Automatic Recovery       │   │  Manual Intervention Required   │
 │  1. Drop old slot         │   │  1. Set status = SlotRecovery   │
 │  2. Create new slot       │   │  2. Log warning                 │
 │  3. Set replay_from_ts    │   │  3. Wait for operator to set    │
 │     from checkpoint       │   │     replay_from_ts              │
 │  4. Resume pipeline       │   │  4. Then auto-recover           │
 └───────────────────────────┘   └─────────────────────────────────┘

 Implementation

 // src/core.rs - Modified main loop with recovery

 pub async fn start_pipeline_with_config(config: PipelineConfig) -> EtlResult<()> {
     loop {
         let result = run_pipeline(&config).await;

         match result {
             Ok(()) => break Ok(()),  // Clean shutdown
             Err(e) if is_slot_invalidation_error(&e) => {
                 warn!("Replication slot invalidated: {}", e);
                 handle_slot_recovery(&config).await?;
                 // Loop continues, will recreate pipeline
             }
             Err(e) => break Err(e),  // Other error, propagate
         }
     }
 }

 async fn run_pipeline(config: &PipelineConfig) -> EtlResult<()> {
     let state_store = PostgresStore::new(config.stream.id, config.stream.pg_connection.clone());
     migrate_etl(&config.stream.pg_connection).await?;

     let sink = match &config.sink {
         SinkConfig::Memory => MemorySink::new(),
     };

     let pgstream = PgStream::create(config.stream.clone(), sink, state_store.clone()).await?;
     let pipeline = Pipeline::new(config.stream.clone().into(), state_store, pgstream);

     start_pipeline_with_shutdown(pipeline).await
 }

 fn is_slot_invalidation_error(error: &EtlError) -> bool {
     let msg = error.to_string().to_lowercase();
     msg.contains("replication slot") && (
         msg.contains("does not exist") ||
         msg.contains("invalidated") ||
         msg.contains("wal removed")
     )
 }

 async fn handle_slot_recovery(config: &PipelineConfig) -> EtlResult<()> {
     let pool = create_pool(&config.stream.pg_connection).await?;
     let slot_name = format!("pgstream_stream_{}_slot", config.stream.id);

     // Step 1: Try to get confirmed_flush_lsn from the invalidated slot BEFORE dropping
     let slot_lsn = sqlx::query_scalar!(
         r#"
         SELECT confirmed_flush_lsn::text
         FROM pg_replication_slots
         WHERE slot_name = $1 AND wal_status = 'lost'
         "#,
         slot_name
     )
     .fetch_optional(&pool)
     .await?
     .flatten();

     // Step 2: Check if we have a checkpoint from previous failover
     let checkpoint = sqlx::query!(
         r#"
         SELECT failover_checkpoint_ts
         FROM pgstream.streams
         WHERE id = $1 AND failover_checkpoint_ts IS NOT NULL
         "#,
         config.stream.id as i64
     )
     .fetch_optional(&pool)
     .await?;

     // Step 3: Drop the old slot (it's invalid anyway)
     sqlx::query(&format!(
         "SELECT pg_drop_replication_slot('{}') WHERE EXISTS (
             SELECT 1 FROM pg_replication_slots WHERE slot_name = '{}'
         )", slot_name, slot_name
     ))
     .execute(&pool)
     .await
     .ok();  // Ignore error if slot doesn't exist

     // Step 4: Determine recovery point
     // Recovery priority:
     // 1. LSN-based recovery from slot's confirmed_flush_lsn (most precise)
     // 2. Failover checkpoint (if we were in failover when slot died)
     // 3. Manual intervention required

     let recovery_ts: Option<DateTime<Utc>> = if let Some(lsn) = &slot_lsn {
         // Best case: Use LSN to find exact replay point
         let replay_point = sqlx::query_scalar!(
             r#"
             SELECT MIN(created_at)
             FROM pgstream.events
             WHERE lsn > $1::pg_lsn AND stream_id = $2
             "#,
             lsn,
             config.stream.id as i64
         )
         .fetch_one(&pool)
         .await?;

         if let Some(ts) = replay_point {
             info!("Found replay point from LSN {}: {:?}", lsn, ts);
             Some(ts)
         } else {
             // No events after this LSN - we're caught up
             info!("No events after LSN {}, slot was up to date", lsn);
             Some(Utc::now())  // Start from now
         }
     } else if let Some(checkpoint) = checkpoint {
         // Fallback: Use failover checkpoint
         info!("No slot LSN available, using failover checkpoint: {:?}",
 checkpoint.failover_checkpoint_ts);
         checkpoint.failover_checkpoint_ts
     } else {
         None
     };

     if let Some(recovery_ts) = recovery_ts {
         // Automatic recovery
         sqlx::query!(
             r#"
             UPDATE pgstream.streams
             SET replay_from_ts = $1,
                 failover_checkpoint_ts = NULL,
                 failover_checkpoint_id = NULL
             WHERE id = $2
             "#,
             recovery_ts,
             config.stream.id as i64
         )
         .execute(&pool)
         .await?;

         info!("Slot recovery initiated, will replay from {:?}", recovery_ts);
     } else {
         // No recovery point - require manual intervention
         warn!(
             "Slot invalidated without recovery point! Manual intervention required. \
              Set replay_from_ts in pgstream.streams to recover: \
              UPDATE pgstream.streams SET replay_from_ts = '<timestamp>' WHERE id = {}",
             config.stream.id
         );

         // Wait for operator to set replay_from_ts
         loop {
             tokio::time::sleep(Duration::from_secs(5)).await;

             let has_replay = sqlx::query_scalar!(
                 "SELECT replay_from_ts IS NOT NULL FROM pgstream.streams WHERE id = $1",
                 config.stream.id as i64
             )
             .fetch_one(&pool)
             .await?;

             if has_replay.unwrap_or(false) {
                 info!("Operator set replay_from_ts, proceeding with recovery");
                 break;
             }

             warn!("Waiting for operator to set replay_from_ts...");
         }
     }

     Ok(())
 }

 New Metrics

 // src/metrics/slot.rs
 const SLOT_RECOVERY_TOTAL: &str = "stream_slot_recovery_total";
 const SLOT_RECOVERY_MANUAL_REQUIRED: &str = "stream_slot_recovery_manual_required_total";

 Files to Modify

 | File                | Changes                                   |
 |---------------------|-------------------------------------------|
 | src/core.rs         | Add recovery loop, handle_slot_recovery() |
 | src/types/stream.rs | Add SlotRecovery status                   |
 | src/store.rs        | Add slot recovery state methods           |
 | src/metrics/mod.rs  | Add slot recovery metrics                 |

 ---
 Implementation Order

 Phase 1: Schema Foundation

 1. Add lsn column to pgstream.events table
 2. Add index on (stream_id, lsn) for recovery queries
 3. Modify trigger function to capture pg_current_wal_lsn() on insert
 4. Add replay_from_ts, replay_to_ts columns to streams table
 5. Add trigger on streams to insert control event when replay_from_ts is set

 Phase 2: User-Initiated Replay (Scenario 1)

 1. Handle control events in daemon (detect replay requests)
 2. Add get_events_by_timestamp_range() to FailoverClient
 3. Add replay handling in tick() or after processing control event
 4. Add clear_replay_request() to store

 Phase 3: Automatic Slot Recovery (Scenario 2)

 1. Add recovery loop to start_pipeline_with_config()
 2. Implement is_slot_invalidation_error() detection
 3. Query confirmed_flush_lsn from invalidated slot
 4. Implement LSN-based recovery point lookup
 5. Add manual intervention fallback for edge cases

 ---
 Critical Files

 | File                                   | Purpose
                 |
 |----------------------------------------|------------------------------------------------------------
 ----------------|
 | migrations/NNNN_add_lsn_and_replay.sql | Add LSN column to events, replay columns to streams,
 control event trigger |
 | migrations/1765364029646_init.sql      | Modify trigger to capture LSN on event insert
                 |
 | src/core.rs                            | Pipeline lifecycle, slot recovery loop
                 |
 | src/stream.rs                          | Replay handling, control event processing
                 |
 | src/failover_client.rs                 | Event streaming methods
                 |
 | src/store.rs                           | State management for replay/recovery
                 |
 | src/types/event.rs                     | Handle control events (type: "control")
                 |
 | src/metrics/mod.rs                     | Add slot recovery metrics
                 |

 ---
 Operator Playbook

 Scenario 1: Sink Lost Data

 -- Check available events
 SELECT MIN(created_at), MAX(created_at), COUNT(*)
 FROM pgstream.events
 WHERE stream_id = 1;

 -- Trigger replay from specific timestamp
 UPDATE pgstream.streams
 SET replay_from_ts = '2025-01-15 10:00:00'
 WHERE id = 1;

 -- Monitor progress (logs will show replay activity)

 Scenario 2: Slot Invalidated (with checkpoint)

 # Daemon automatically:
 # 1. Detects slot error
 # 2. Drops invalid slot
 # 3. Sets replay_from_ts from checkpoint
 # 4. Creates new slot and resumes

 # Monitor via logs and metrics:
 # - stream_slot_recovery_total counter increases

 Scenario 3: Slot Invalidated (no checkpoint)

 # Daemon logs warning and waits for operator

 # Operator must decide replay point:
 SELECT MIN(created_at) FROM pgstream.events WHERE stream_id = 1;

 # Then set replay point:
 UPDATE pgstream.streams
 SET replay_from_ts = '2025-01-01 00:00:00'  -- Your chosen timestamp
 WHERE id = 1;

 # Daemon will automatically continue recovery
