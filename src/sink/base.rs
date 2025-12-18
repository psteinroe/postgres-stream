use std::future::Future;

use etl::error::EtlResult;

use crate::types::TriggeredEvent;

/// Trait for systems that can receive events data from streams.
///
/// [`Sink`] implementations define how events are sent to target systems.
///
/// Implementations should ensure idempotent operations where possible, as the stream system
/// may retry failed operations. The sink should handle concurrent writes safely.
pub trait Sink {
    /// Returns the name of the destination.
    fn name() -> &'static str;

    /// Publishs triggered events to the destination.
    fn publish_events(
        &self,
        events: Vec<TriggeredEvent>,
    ) -> impl Future<Output = EtlResult<()>> + Send;
}
