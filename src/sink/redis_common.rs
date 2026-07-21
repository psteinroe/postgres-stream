//! Shared Redis connection and error handling for Redis-backed sinks.

use etl::error::{ErrorKind, EtlError};
use redis::{
    RedisError,
    aio::{ConnectionManager, ConnectionManagerConfig},
};
use std::time::Duration;

/// Optional Redis connection-manager settings.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct RedisConnectionSettings {
    pub(crate) connection_timeout_ms: Option<u64>,
    pub(crate) response_timeout_ms: Option<u64>,
    pub(crate) connection_retries: Option<usize>,
    pub(crate) connection_max_delay_ms: Option<u64>,
}

/// Creates a Redis connection manager with the configured timeout and retry behavior.
pub(crate) async fn connect(
    url: String,
    settings: RedisConnectionSettings,
) -> redis::RedisResult<ConnectionManager> {
    let client = redis::Client::open(url)?;
    let mut config = ConnectionManagerConfig::new();

    if let Some(timeout_ms) = settings.connection_timeout_ms {
        config = config.set_connection_timeout(Duration::from_millis(timeout_ms));
    }
    if let Some(timeout_ms) = settings.response_timeout_ms {
        config = config.set_response_timeout(Duration::from_millis(timeout_ms));
    }
    if let Some(retries) = settings.connection_retries {
        config = config.set_number_of_retries(retries);
    }
    if let Some(max_delay_ms) = settings.connection_max_delay_ms {
        config = config.set_max_delay(max_delay_ms);
    }

    ConnectionManager::new_with_config(client, config).await
}

/// Maps a Redis failure to the ETL retry classification while retaining its source.
pub(crate) fn map_error(error: RedisError, description: &'static str) -> EtlError {
    let kind = classify_error(&error);
    etl::etl_error!(kind, description, source: error)
}

fn classify_error(error: &RedisError) -> ErrorKind {
    if error.kind() == redis::ErrorKind::AuthenticationFailed {
        return ErrorKind::DestinationAuthenticationError;
    }

    if is_transient(error) {
        ErrorKind::DestinationConnectionFailed
    } else {
        ErrorKind::InvalidData
    }
}

fn is_transient(error: &RedisError) -> bool {
    if error.is_io_error() {
        return true;
    }

    if error.kind() == redis::ErrorKind::ExtensionError {
        return error.code() == Some("OOM");
    }

    matches!(
        error.kind(),
        redis::ErrorKind::BusyLoadingError
            | redis::ErrorKind::Moved
            | redis::ErrorKind::Ask
            | redis::ErrorKind::TryAgain
            | redis::ErrorKind::ClusterDown
            | redis::ErrorKind::MasterDown
            | redis::ErrorKind::ReadOnly
            | redis::ErrorKind::ClusterConnectionNotFound
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{error::Error, io, net::TcpListener, thread, time::Instant};

    #[test]
    fn classifies_transient_io_errors_for_timed_retry() {
        let error = RedisError::from(io::Error::new(io::ErrorKind::TimedOut, "timed out"));

        assert_eq!(
            classify_error(&error),
            ErrorKind::DestinationConnectionFailed
        );
    }

    #[test]
    fn classifies_authentication_errors_for_manual_intervention() {
        let error = RedisError::from((
            redis::ErrorKind::AuthenticationFailed,
            "authentication failed",
        ));

        assert_eq!(
            classify_error(&error),
            ErrorKind::DestinationAuthenticationError
        );
    }

    #[test]
    fn classifies_client_errors_as_invalid_data() {
        for kind in [
            redis::ErrorKind::InvalidClientConfig,
            redis::ErrorKind::ParseError,
            redis::ErrorKind::ResponseError,
        ] {
            let error = RedisError::from((kind, "permanent error"));
            assert_eq!(classify_error(&error), ErrorKind::InvalidData);
        }
    }

    #[test]
    fn classifies_temporary_server_errors_for_timed_retry() {
        let error = RedisError::from((redis::ErrorKind::TryAgain, "try again"));

        assert_eq!(
            classify_error(&error),
            ErrorKind::DestinationConnectionFailed
        );
    }

    #[test]
    fn classifies_redis_oom_as_transient() {
        let error = redis::parse_redis_value(b"-OOM command not allowed\r\n")
            .unwrap()
            .extract_error()
            .unwrap_err();

        assert_eq!(
            classify_error(&error),
            ErrorKind::DestinationConnectionFailed
        );
    }

    #[tokio::test]
    async fn response_timeout_bounds_stalled_redis_responses() {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let _connection = listener.accept().unwrap();
            thread::sleep(Duration::from_secs(1));
        });

        let settings = RedisConnectionSettings {
            response_timeout_ms: Some(50),
            connection_retries: Some(0),
            ..RedisConnectionSettings::default()
        };
        let started = Instant::now();
        let error = match connect(format!("redis://{address}"), settings).await {
            Ok(_) => panic!("stalled Redis response should time out"),
            Err(error) => error,
        };

        assert!(error.is_timeout());
        assert!(started.elapsed() < Duration::from_millis(500));
        assert_eq!(
            classify_error(&error),
            ErrorKind::DestinationConnectionFailed
        );

        server.join().unwrap();
    }

    #[test]
    fn mapped_errors_retain_the_redis_error_as_their_source() {
        let error = RedisError::from((redis::ErrorKind::TypeError, "wrong type"));
        let mapped = map_error(error, "failed to publish to Redis");

        assert_eq!(mapped.kind(), ErrorKind::InvalidData);
        assert!(mapped.source().is_some());
    }
}
