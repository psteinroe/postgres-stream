use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use etl::destination::Destination;
use etl::error::{ErrorKind, EtlResult};
use etl::store::both::postgres::PostgresStore;
use postgres_stream::sink::Sink;
use postgres_stream::stream::PgStream;
use postgres_stream::test_utils::{
    TestDatabase, create_postgres_store_with_table_id, insert_events_to_db, make_event_with_id,
    test_stream_config,
};
use postgres_stream::types::{StreamStatus, TriggeredEvent};

#[derive(Clone)]
struct FailFirstNSink {
    fail_first_n: usize,
    call_count: Arc<AtomicUsize>,
}

impl FailFirstNSink {
    fn new(fail_first_n: usize) -> Self {
        Self {
            fail_first_n,
            call_count: Arc::new(AtomicUsize::new(0)),
        }
    }
}

impl Sink for FailFirstNSink {
    fn name() -> &'static str {
        "fail_first_n"
    }

    fn publish_events(
        &self,
        _events: Vec<TriggeredEvent>,
    ) -> impl std::future::Future<Output = EtlResult<()>> + Send {
        let call_num = self.call_count.fetch_add(1, Ordering::SeqCst);
        let fail_first_n = self.fail_first_n;

        async move {
            if call_num < fail_first_n {
                return Err(etl::etl_error!(
                    ErrorKind::InvalidData,
                    "Simulated sink failure",
                    "Test failure"
                ));
            }

            Ok(())
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_failover_does_not_overwrite_checkpoint_on_repeated_failures() {
    let db = TestDatabase::spawn().await;
    let config = test_stream_config(&db);
    let sink = FailFirstNSink::new(3);
    let (store, table_id) =
        create_postgres_store_with_table_id(config.id, &db.config, &db.pool).await;

    let stream: PgStream<FailFirstNSink, PostgresStore> =
        PgStream::create(config.clone(), sink, store)
            .await
            .expect("Failed to create PgStream");

    let event_ids = insert_events_to_db(&db, 3).await;

    // First write fails and enters failover at event 0.
    stream
        .write_events(vec![make_event_with_id(
            table_id,
            event_ids.first().expect("Should have event 0"),
            serde_json::json!({"seq": 1}),
        )])
        .await
        .expect("write_events should succeed even if sink fails");

    // During failover, both checkpoint re-publish and current event publish fail.
    // The checkpoint should remain at event 0 instead of being overwritten.
    stream
        .write_events(vec![make_event_with_id(
            table_id,
            event_ids.get(2).expect("Should have event 2"),
            serde_json::json!({"seq": 3}),
        )])
        .await
        .expect("write_events should succeed even if sink fails");

    let (status, _) = stream.store().get_stream_state().await.unwrap();

    match status {
        StreamStatus::Failover {
            checkpoint_event_id,
        } => {
            let expected = event_ids.first().expect("Should have event 0");
            assert_eq!(checkpoint_event_id.id, expected.id);
            assert_eq!(
                checkpoint_event_id.created_at.timestamp_micros(),
                expected.created_at.timestamp_micros()
            );
        }
        StreamStatus::Healthy => panic!("Expected Failover status, got Healthy"),
    }
}
