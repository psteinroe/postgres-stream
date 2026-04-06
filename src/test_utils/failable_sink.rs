use etl::error::{ErrorKind, EtlResult};
use etl::etl_error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use tokio::sync::Mutex;

use crate::sink::Sink;
use crate::types::TriggeredEvent;

/// A sink that can be configured to fail on specific calls
#[derive(Clone)]
pub struct FailableSink {
    events: Arc<Mutex<Vec<TriggeredEvent>>>,
    fail_on_call: Arc<AtomicUsize>,
    /// Inclusive start of the range of calls to fail on.
    fail_range_start: Arc<AtomicUsize>,
    /// Exclusive end of the range of calls to fail on.
    fail_range_end: Arc<AtomicUsize>,
    call_count: Arc<AtomicUsize>,
    should_fail: Arc<AtomicBool>,
}

impl Default for FailableSink {
    fn default() -> Self {
        Self::new()
    }
}

impl FailableSink {
    #[must_use]
    pub fn new() -> Self {
        Self {
            events: Arc::new(Mutex::new(Vec::new())),
            fail_on_call: Arc::new(AtomicUsize::new(usize::MAX)),
            fail_range_start: Arc::new(AtomicUsize::new(usize::MAX)),
            fail_range_end: Arc::new(AtomicUsize::new(usize::MAX)),
            call_count: Arc::new(AtomicUsize::new(0)),
            should_fail: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Configure sink to fail on the Nth call (0-indexed)
    pub fn fail_on_call(&self, n: usize) {
        self.fail_on_call.store(n, Ordering::SeqCst);
    }

    /// Configure sink to fail on calls in the range `[start, end)` (0-indexed)
    pub fn fail_on_calls(&self, start: usize, end: usize) {
        self.fail_range_start.store(start, Ordering::SeqCst);
        self.fail_range_end.store(end, Ordering::SeqCst);
    }

    /// Configure sink to succeed on all calls
    pub fn succeed_always(&self) {
        self.should_fail.store(false, Ordering::SeqCst);
        self.fail_on_call.store(usize::MAX, Ordering::SeqCst);
        self.fail_range_start.store(usize::MAX, Ordering::SeqCst);
        self.fail_range_end.store(usize::MAX, Ordering::SeqCst);
    }

    pub async fn events(&self) -> Vec<TriggeredEvent> {
        self.events.lock().await.clone()
    }

    #[must_use]
    pub fn call_count(&self) -> usize {
        self.call_count.load(Ordering::SeqCst)
    }
}

impl Sink for FailableSink {
    fn name() -> &'static str {
        "failable_sink"
    }

    async fn publish_events(&self, events: Vec<TriggeredEvent>) -> EtlResult<()> {
        let call_num = self.call_count.fetch_add(1, Ordering::SeqCst);

        let range_start = self.fail_range_start.load(Ordering::SeqCst);
        let range_end = self.fail_range_end.load(Ordering::SeqCst);

        // Check if we should fail on this specific call
        if call_num == self.fail_on_call.load(Ordering::SeqCst)
            || (call_num >= range_start && call_num < range_end)
            || self.should_fail.load(Ordering::SeqCst)
        {
            return Err(etl_error!(
                ErrorKind::InvalidData,
                "Simulated sink failure",
                "Test failure"
            ));
        }

        // Success - store events
        self.events.lock().await.extend(events);
        Ok(())
    }
}
