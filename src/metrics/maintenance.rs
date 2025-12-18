use metrics::{Unit, counter, describe_counter, describe_histogram, histogram};

/// Metric name for counting the total number of maintenance runs executed.
const MAINTENANCE_RUNS_TOTAL: &str = "maintenance_runs_total";

/// Metric name for tracking the duration of maintenance operations in milliseconds.
const MAINTENANCE_DURATION_MILLISECONDS: &str = "maintenance_duration_milliseconds";

/// Label key for stream identifier.
const STREAM_ID_LABEL: &str = "stream_id";

/// Label key for maintenance result (success/failure).
const RESULT_LABEL: &str = "result";

/// Registers maintenance metric descriptions with the global metrics recorder.
pub(crate) fn register_maintenance_metrics() {
    describe_counter!(
        MAINTENANCE_RUNS_TOTAL,
        Unit::Count,
        "Total number of maintenance runs executed"
    );
    describe_histogram!(
        MAINTENANCE_DURATION_MILLISECONDS,
        Unit::Milliseconds,
        "Duration of maintenance operations"
    );
}

/// Records a maintenance run with its result.
pub fn record_maintenance_run(stream_id: u64, duration_milliseconds: f64, success: bool) {
    let result = if success { "success" } else { "failure" };
    counter!(
        MAINTENANCE_RUNS_TOTAL,
        STREAM_ID_LABEL => stream_id.to_string(),
        RESULT_LABEL => result
    )
    .increment(1);

    if success {
        histogram!(
            MAINTENANCE_DURATION_MILLISECONDS,
            STREAM_ID_LABEL => stream_id.to_string()
        )
        .record(duration_milliseconds);
    }
}
