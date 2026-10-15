use std::net::{AddrParseError, IpAddr, Ipv6Addr, SocketAddr};

use metrics_exporter_prometheus::{BuildError, PrometheusBuilder};

use super::{register_failover_metrics, register_maintenance_metrics, register_stream_metrics};

/// Environment variable that overrides the metrics listener address.
pub const METRICS_LISTEN_ADDRESS_ENV: &str = "PGSTREAM_METRICS_ADDR";

/// Default metrics listener address: all interfaces, port 9000.
const DEFAULT_METRICS_LISTEN_ADDRESS: SocketAddr =
    SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 9000);

/// Errors raised while starting the metrics exporter.
#[derive(Debug, thiserror::Error)]
pub enum MetricsError {
    /// `PGSTREAM_METRICS_ADDR` is not a socket address.
    #[error(
        "{METRICS_LISTEN_ADDRESS_ENV} must be a socket address such as 127.0.0.1:9000 (got {value:?})"
    )]
    InvalidListenAddress {
        value: String,
        #[source]
        source: AddrParseError,
    },
    /// The Prometheus exporter could not be installed.
    #[error(transparent)]
    Build(#[from] BuildError),
}

/// Resolves the metrics listener address from an optional override.
pub fn metrics_listen_address(value: Option<&str>) -> Result<SocketAddr, MetricsError> {
    match value {
        None => Ok(DEFAULT_METRICS_LISTEN_ADDRESS),
        Some(value) => value
            .parse()
            .map_err(|source| MetricsError::InvalidListenAddress {
                value: value.to_owned(),
                source,
            }),
    }
}

/// Initializes metrics with an automatic HTTP server.
///
/// This function is designed for standalone services where metrics should be exposed
/// automatically without manual endpoint management. It installs a global metrics recorder
/// and starts an HTTP server that listens on `[::]:9000/metrics` by default, or on the
/// address in `PGSTREAM_METRICS_ADDR`, making metrics available for Prometheus scraping.
///
/// # Use Case
///
/// Use this when you want to:
/// - Expose metrics from a standalone service.
/// - Automatically start a dedicated metrics endpoint without custom routing.
/// - Let Prometheus scrape metrics directly from a fixed port.
/// - Run several instances on one host network, each on its own port.
pub fn init_metrics() -> Result<(), MetricsError> {
    let address =
        metrics_listen_address(std::env::var(METRICS_LISTEN_ADDRESS_ENV).ok().as_deref())?;
    let builder = PrometheusBuilder::new().with_http_listener(address);

    builder.install()?;

    // Register all metric descriptions
    register_failover_metrics();
    register_maintenance_metrics();
    register_stream_metrics();

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_all_interfaces_on_port_9000() {
        assert_eq!(
            metrics_listen_address(None).unwrap(),
            "[::]:9000".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn accepts_an_override() {
        assert_eq!(
            metrics_listen_address(Some("127.0.0.1:9187")).unwrap(),
            "127.0.0.1:9187".parse::<SocketAddr>().unwrap()
        );
    }

    #[test]
    fn rejects_an_invalid_override() {
        let error = metrics_listen_address(Some("9187")).unwrap_err();
        assert!(error.to_string().contains("PGSTREAM_METRICS_ADDR"));
    }
}
