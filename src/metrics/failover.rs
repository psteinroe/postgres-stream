use metrics::{
    Unit, counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram,
};

/// Metric name for tracking if a stream is currently in failover mode.
///
/// This gauge is set to 1 when the stream is in failover mode and 0 otherwise.
const STREAM_FAILOVER_ACTIVE: &str = "stream_failover_active";

/// Metric name for counting the total number of times a stream has entered failover mode.
const STREAM_FAILOVER_ENTERED_TOTAL: &str = "stream_failover_entered_total";

/// Metric name for counting the total number of times a stream has successfully recovered from failover.
const STREAM_FAILOVER_RECOVERED_TOTAL: &str = "stream_failover_recovered_total";

/// Metric name for recording the time spent in failover mode.
const STREAM_FAILOVER_DURATION_SECONDS: &str = "stream_failover_duration_seconds";

/// Metric name for tracking the age of the checkpoint event being retried during failover.
const STREAM_FAILOVER_CHECKPOINT_AGE_SECONDS: &str = "stream_failover_checkpoint_age_seconds";

/// Label key for stream identifier.
const STREAM_ID_LABEL: &str = "stream_id";

/// Registers failover metric descriptions with the global metrics recorder.
pub(crate) fn register_failover_metrics() {
    describe_gauge!(
        STREAM_FAILOVER_ACTIVE,
        Unit::Count,
        "Whether the stream is currently in failover mode (1 = failover, 0 = healthy)"
    );
    describe_counter!(
        STREAM_FAILOVER_ENTERED_TOTAL,
        Unit::Count,
        "Total number of times the stream has entered failover mode"
    );
    describe_counter!(
        STREAM_FAILOVER_RECOVERED_TOTAL,
        Unit::Count,
        "Total number of times the stream has successfully recovered from failover"
    );
    describe_histogram!(
        STREAM_FAILOVER_DURATION_SECONDS,
        Unit::Seconds,
        "Time spent in failover mode before recovery"
    );
    describe_gauge!(
        STREAM_FAILOVER_CHECKPOINT_AGE_SECONDS,
        Unit::Seconds,
        "Age of the checkpoint event currently being retried during failover"
    );
}

/// Records that the stream has entered failover mode.
pub fn record_failover_entered(stream_id: u64) {
    counter!(
        STREAM_FAILOVER_ENTERED_TOTAL,
        STREAM_ID_LABEL => stream_id.to_string()
    )
    .increment(1);
    gauge!(
        STREAM_FAILOVER_ACTIVE,
        STREAM_ID_LABEL => stream_id.to_string()
    )
    .set(1.0);
}

/// Records that the stream has recovered from failover mode.
pub fn record_failover_recovered(stream_id: u64, duration_seconds: f64) {
    counter!(
        STREAM_FAILOVER_RECOVERED_TOTAL,
        STREAM_ID_LABEL => stream_id.to_string()
    )
    .increment(1);
    gauge!(
        STREAM_FAILOVER_ACTIVE,
        STREAM_ID_LABEL => stream_id.to_string()
    )
    .set(0.0);
    histogram!(
        STREAM_FAILOVER_DURATION_SECONDS,
        STREAM_ID_LABEL => stream_id.to_string()
    )
    .record(duration_seconds);
}

/// Records the age of the checkpoint event during failover.
pub fn record_checkpoint_age(stream_id: u64, age_seconds: f64) {
    gauge!(
        STREAM_FAILOVER_CHECKPOINT_AGE_SECONDS,
        STREAM_ID_LABEL => stream_id.to_string()
    )
    .set(age_seconds);
}
