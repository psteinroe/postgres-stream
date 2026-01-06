use metrics::{Unit, describe_gauge, describe_histogram, gauge, histogram};

/// Metric name for tracking if a stream is currently in failover mode.
///
/// This gauge is set to 1 when the stream is in failover mode and 0 otherwise.
const STREAM_FAILOVER_ACTIVE: &str = "stream_failover_active";

/// Metric name for recording the time spent in failover mode.
const STREAM_FAILOVER_DURATION_MILLISECONDS: &str = "stream_failover_duration_milliseconds";

/// Label key for stream identifier.
const STREAM_ID_LABEL: &str = "stream_id";

/// Registers failover metric descriptions with the global metrics recorder.
pub(crate) fn register_failover_metrics() {
    describe_gauge!(
        STREAM_FAILOVER_ACTIVE,
        Unit::Count,
        "Whether the stream is currently in failover mode (1 = failover, 0 = healthy)"
    );
    describe_histogram!(
        STREAM_FAILOVER_DURATION_MILLISECONDS,
        Unit::Seconds,
        "Time spent in failover mode before recovery"
    );
}

/// Records that the stream has entered failover mode.
pub fn record_failover_entered(stream_id: u64) {
    gauge!(
        STREAM_FAILOVER_ACTIVE,
        STREAM_ID_LABEL => stream_id.to_string()
    )
    .set(1.0);
}

/// Records that the stream has recovered from failover mode.
pub fn record_failover_recovered(stream_id: u64, duration_milliseconds: f64) {
    gauge!(
        STREAM_FAILOVER_ACTIVE,
        STREAM_ID_LABEL => stream_id.to_string()
    )
    .set(0.0);
    histogram!(
        STREAM_FAILOVER_DURATION_MILLISECONDS,
        STREAM_ID_LABEL => stream_id.to_string()
    )
    .record(duration_milliseconds);
}
