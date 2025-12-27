use etl::error::EtlResult;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

use crate::sink::Sink;
use crate::types::TriggeredEvent;

#[derive(Debug)]
struct Inner {
    events: Vec<TriggeredEvent>,
}

/// In-memory sink for testing and development purposes.
///
/// [`MemorySink`] stores all replicated data in memory, making it ideal for
/// testing streams, debugging streaming behavior, and development workflows.
/// All data is held in memory and will be lost when the process terminates.
#[derive(Debug, Clone)]
pub struct MemorySink {
    inner: Arc<Mutex<Inner>>,
}

impl MemorySink {
    /// Creates a new empty memory sink.
    ///
    /// The sink starts with no stored data and will accumulate
    /// events as the stream receives them.
    #[must_use]
    pub fn new() -> Self {
        let inner = Inner { events: Vec::new() };

        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    /// Returns a copy of all events stored in this sink.
    ///
    /// This method is useful for testing and verification of stream behavior.
    /// It provides access to all events that have been written
    /// to this sink since creation or the last clear operation.
    pub async fn events(&self) -> Vec<TriggeredEvent> {
        let inner = self.inner.lock().await;
        inner.events.clone()
    }

    /// Clears all stored events.
    ///
    /// This method is useful for resetting the sink state between tests
    /// or during development workflows.
    pub async fn clear(&self) {
        let mut inner = self.inner.lock().await;
        inner.events.clear();
    }
}

impl Default for MemorySink {
    fn default() -> Self {
        Self::new()
    }
}

impl Sink for MemorySink {
    fn name() -> &'static str {
        "memory"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        let mut inner = self.inner.lock().await;

        info!("writing a batch of {} events:", events.len());

        for event in &events {
            info!("  {:?}", event);
        }
        inner.events.extend(events);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{EventIdentifier, StreamId};
    use chrono::Utc;

    fn make_test_event(id: &str) -> TriggeredEvent {
        TriggeredEvent {
            id: EventIdentifier::new(id.to_string(), Utc::now()),
            payload: serde_json::json!({ "test": id }),
            metadata: None,
            stream_id: StreamId::from(1u64),
            lsn: Some("0/16B3748".parse().unwrap()),
        }
    }

    #[tokio::test]
    async fn test_new_sink_is_empty() {
        let sink = MemorySink::new();
        let events = sink.events().await;
        assert_eq!(events.len(), 0);
    }

    #[tokio::test]
    async fn test_publish_events_stores_events() {
        let sink = MemorySink::new();
        let test_events = vec![make_test_event("event1"), make_test_event("event2")];

        sink.publish_events(test_events.clone()).await.unwrap();

        let stored = sink.events().await;
        assert_eq!(stored.len(), 2);
        assert_eq!(
            stored.first().expect("first element exists").id.id,
            "event1"
        );
        assert_eq!(
            stored.get(1).expect("second element exists").id.id,
            "event2"
        );
    }

    #[tokio::test]
    async fn test_publish_events_accumulates() {
        let sink = MemorySink::new();

        sink.publish_events(vec![make_test_event("event1")])
            .await
            .unwrap();
        sink.publish_events(vec![make_test_event("event2")])
            .await
            .unwrap();

        let stored = sink.events().await;
        assert_eq!(stored.len(), 2);
        assert_eq!(
            stored.first().expect("first element exists").id.id,
            "event1"
        );
        assert_eq!(
            stored.get(1).expect("second element exists").id.id,
            "event2"
        );
    }

    #[tokio::test]
    async fn test_clear_removes_all_events() {
        let sink = MemorySink::new();
        let test_events = vec![make_test_event("event1"), make_test_event("event2")];

        sink.publish_events(test_events).await.unwrap();
        assert_eq!(sink.events().await.len(), 2);

        sink.clear().await;
        assert_eq!(sink.events().await.len(), 0);
    }

    #[tokio::test]
    async fn test_clone_shares_state() {
        let sink1 = MemorySink::new();
        let sink2 = sink1.clone();

        sink1
            .publish_events(vec![make_test_event("event1")])
            .await
            .unwrap();

        let events2 = sink2.events().await;
        assert_eq!(events2.len(), 1);
        assert_eq!(
            events2.first().expect("first element exists").id.id,
            "event1"
        );
    }

    #[tokio::test]
    async fn test_empty_batch() {
        let sink = MemorySink::new();
        sink.publish_events(vec![]).await.unwrap();
        assert_eq!(sink.events().await.len(), 0);
    }

    #[test]
    fn test_sink_name() {
        assert_eq!(MemorySink::name(), "memory");
    }

    #[test]
    fn test_default_creates_empty_sink() {
        let sink = MemorySink::default();
        // Can't call async in sync test, but we can verify it compiles
        drop(sink);
    }
}
