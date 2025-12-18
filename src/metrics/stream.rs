use metrics::{Unit, describe_gauge, gauge};

/// Metric name for counting the number of seconds the stream is lagging behind real-time processing.
const STREAM_PROCESSING_LAG_SECONDS: &str = "stream_processing_lag_seconds";

/// Label key for stream identifier.
const STREAM_ID_LABEL: &str = "stream_id";

/// Registers stream metric descriptions with the global metrics recorder.
pub(crate) fn register_stream_metrics() {
    describe_gauge!(
        STREAM_PROCESSING_LAG_SECONDS,
        Unit::Seconds,
        "Processing lag (difference between now and last event timestamp)"
    );
}

/// Records the processing lag in seconds.
pub fn record_processing_lag(stream_id: u64, lag_seconds: f64) {
    gauge!(
        STREAM_PROCESSING_LAG_SECONDS,
        STREAM_ID_LABEL => stream_id.to_string()
    )
    .set(lag_seconds);
}
