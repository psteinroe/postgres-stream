use etl::types::PipelineId;

use crate::types::EventIdentifier;

pub type StreamId = PipelineId;

pub trait SlotName {
    fn slot_name(&self) -> String;
}

impl SlotName for StreamId {
    /// Returns the slot name for the stream that `etl` uses under the hood.
    fn slot_name(&self) -> String {
        format!("supabase_etl_apply_{self}")
    }
}

pub trait PublicationName {
    fn publication_name(&self) -> String;
}

impl PublicationName for StreamId {
    fn publication_name(&self) -> String {
        format!("pgstream_stream_{self}")
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum StreamStatus {
    /// Everything is good.
    Healthy,

    /// The target is not reachable, and we are in failover mode.
    Failover {
        /// The event at which failover was initiated.
        checkpoint_event_id: EventIdentifier,
    },
}
#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn test_stream_status_equality() {
        let status1 = StreamStatus::Healthy;
        let status2 = StreamStatus::Healthy;
        assert_eq!(status1, status2);

        let ts = chrono::Utc.with_ymd_and_hms(2024, 3, 15, 12, 0, 0).unwrap();
        let checkpoint_id = EventIdentifier::new("event1".to_string(), ts);
        let status3 = StreamStatus::Failover {
            checkpoint_event_id: checkpoint_id.clone(),
        };
        let status4 = StreamStatus::Failover {
            checkpoint_event_id: checkpoint_id,
        };
        assert_eq!(status3, status4);

        assert_ne!(status1, status3);
    }
}
