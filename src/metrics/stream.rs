use metrics::{Unit, counter, describe_counter, describe_gauge, gauge};

/// Metric name for counting the number of milliseconds the stream is lagging behind real-time processing.
const STREAM_PROCESSING_LAG_MILLISECONDS: &str = "stream_processing_lag_milliseconds";

/// Metric name for counting the total number of events processed by the stream.
const STREAM_EVENTS_PROCESSED_TOTAL: &str = "stream_events_processed_total";

/// Label key for stream identifier.
const STREAM_ID_LABEL: &str = "stream_id";

/// Registers stream metric descriptions with the global metrics recorder.
pub(crate) fn register_stream_metrics() {
    describe_gauge!(
        STREAM_PROCESSING_LAG_MILLISECONDS,
        Unit::Milliseconds,
        "Processing lag (difference between now and last event timestamp)"
    );
    describe_counter!(
        STREAM_EVENTS_PROCESSED_TOTAL,
        Unit::Count,
        "Total number of events processed by the stream"
    );
}

/// Records the processing lag in milliseconds.
pub fn record_processing_lag(stream_id: u64, lag_milliseconds: i64) {
    gauge!(
        STREAM_PROCESSING_LAG_MILLISECONDS,
        STREAM_ID_LABEL => stream_id.to_string()
    )
    .set(lag_milliseconds as f64);
}

/// Records the number of events processed.
pub fn record_events_processed(stream_id: u64, count: usize) {
    counter!(
        STREAM_EVENTS_PROCESSED_TOTAL,
        STREAM_ID_LABEL => stream_id.to_string()
    )
    .increment(count as u64);
}
