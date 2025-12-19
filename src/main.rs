use etl::error::EtlResult;
use postgres_stream::config::{PipelineConfig, load_config};
use postgres_stream::core::start_pipeline_with_config;
use postgres_stream::metrics::init_metrics;
use tracing::{error, info};
use tracing_subscriber::{EnvFilter, fmt};

/// Jemalloc allocator for better memory management in high-throughput async workloads.
#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Jemalloc configuration optimized for high-throughput async CDC workloads.
///
/// - `narenas:8`: Fixed arena count for predictable memory behavior in containers.
/// - `background_thread:true`: Offloads memory purging to background threads (Linux only).
/// - `metadata_thp:auto`: Enables transparent huge pages for jemalloc metadata, reducing TLB misses.
/// - `dirty_decay_ms:10000`: Returns unused dirty pages to the OS after 10 seconds.
/// - `muzzy_decay_ms:10000`: Returns unused muzzy pages to the OS after 10 seconds.
/// - `tcache_max:8192`: Reduces thread-local cache size for better container memory efficiency.
/// - `abort_conf:true`: Aborts on invalid configuration for fail-fast behavior.
///
/// On Linux, this can be overridden via `MALLOC_CONF` env var.
/// On macOS, use `_RJEM_MALLOC_CONF` (unprefixed symbols not supported).
#[cfg(all(target_os = "linux", not(target_env = "msvc")))]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "malloc_conf")]
pub static malloc_conf: &[u8] =
    b"narenas:8,background_thread:true,metadata_thp:auto,dirty_decay_ms:10000,muzzy_decay_ms:10000,tcache_max:8192,abort_conf:true\0";

/// Jemalloc configuration for macOS (uses prefixed symbol since unprefixed not supported).
#[cfg(all(target_os = "macos", not(target_env = "msvc")))]
#[allow(non_upper_case_globals)]
#[unsafe(export_name = "_rjem_malloc_conf")]
pub static malloc_conf: &[u8] =
    b"narenas:8,background_thread:true,metadata_thp:auto,dirty_decay_ms:10000,muzzy_decay_ms:10000,tcache_max:8192,abort_conf:true\0";

/// Entry point for the pgstream daemon.
///
/// Loads configuration, initializes tracing, starts the async runtime,
/// and launches the replication stream. Handles all errors and ensures
/// proper service initialization sequence.
fn main() -> anyhow::Result<()> {
    // Initialize tracing subscriber for logging
    init_tracing();

    // Load daemon configuration
    let config = load_config::<PipelineConfig>().map_err(|c| {
        etl::etl_error!(
            etl::error::ErrorKind::ConfigError,
            "failed to load configuration: {}",
            c
        )
    })?;

    // Initialize metrics collection
    init_metrics()?;

    info!(stream_id = config.stream.id, "pgstream daemon starting");

    // Start the tokio runtime
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async_main(config))?;

    Ok(())
}

/// Main async entry point that starts the pipeline.
///
/// Launches the stream with the provided configuration and captures
/// any errors for logging and error handling.
async fn async_main(config: PipelineConfig) -> EtlResult<()> {
    // Start the jemalloc metrics collection background task.
    #[cfg(not(target_env = "msvc"))]
    postgres_stream::metrics::spawn_jemalloc_metrics_task(config.stream.id);

    // Start the daemon with error handling
    if let Err(err) = start_pipeline_with_config(config).await {
        error!("an error occurred in the stream daemon: {err}");
        return Err(err);
    }

    Ok(())
}

/// Initializes the tracing subscriber for logging.
///
/// Sets up a basic console-based logger with INFO level by default.
/// Can be configured via RUST_LOG environment variable.
fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    fmt()
        .with_env_filter(filter)
        .with_target(false)
        .with_thread_ids(false)
        .with_file(false)
        .with_line_number(false)
        .init();
}
