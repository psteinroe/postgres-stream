use etl::error::EtlError;
use std::{backtrace::Backtrace, error::Error, fmt};

pub type PgStreamResult<T> = Result<T, PgStreamError>;

/// Captured backtrace wrapper matching etl-replicator's stable error-reporting pattern.
pub struct CapturedBacktrace(Backtrace);

impl CapturedBacktrace {
    fn capture() -> Self {
        Self(Backtrace::capture())
    }
}

impl fmt::Debug for CapturedBacktrace {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// Top-level daemon error used to render one consistent failure report.
#[derive(Debug)]
pub enum PgStreamError {
    Etl(EtlError),
    Config(Box<dyn Error + Send + Sync>, CapturedBacktrace),
    Io(std::io::Error, CapturedBacktrace),
}

impl PgStreamError {
    pub fn config<E: Error + Send + Sync + 'static>(error: E) -> Self {
        Self::Config(Box::new(error), CapturedBacktrace::capture())
    }

    fn category(&self) -> &'static str {
        match self {
            Self::Etl(_) => "daemon error",
            Self::Config(_, _) => "configuration error",
            Self::Io(_, _) => "i/o error",
        }
    }

    fn backtrace(&self) -> Option<&Backtrace> {
        match self {
            Self::Etl(error) => error.backtrace(),
            Self::Config(_, backtrace) | Self::Io(_, backtrace) => Some(&backtrace.0),
        }
    }

    pub fn render_report(&self) -> String {
        let mut report = String::new();
        report.push_str("postgres-stream failed\n");
        report.push_str(&format!("category: {}\n", self.category()));
        report.push_str(&format!("error: {self}\n"));

        if !matches!(self, Self::Etl(error) if error.errors().is_some()) {
            let mut source = Error::source(self);
            let mut index = 1usize;
            while let Some(error) = source {
                report.push_str(&format!("cause {index}: {error}\n"));
                source = error.source();
                index += 1;
            }
        }

        if should_render_backtrace()
            && let Some(backtrace) = self.backtrace()
        {
            report.push_str("backtrace:\n");
            report.push_str(&backtrace.to_string());
            if !report.ends_with('\n') {
                report.push('\n');
            }
        }

        report
    }
}

impl fmt::Display for PgStreamError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Etl(error) => write!(formatter, "{error}"),
            Self::Config(source, _) => write!(formatter, "configuration error: {source}"),
            Self::Io(source, _) => write!(formatter, "i/o error: {source}"),
        }
    }
}

impl Error for PgStreamError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Etl(error) => error.source(),
            Self::Config(source, _) => Some(source.as_ref()),
            Self::Io(source, _) => Some(source),
        }
    }
}

impl From<EtlError> for PgStreamError {
    fn from(error: EtlError) -> Self {
        Self::Etl(error)
    }
}

impl From<std::io::Error> for PgStreamError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error, CapturedBacktrace::capture())
    }
}

fn should_render_backtrace() -> bool {
    matches!(
        std::env::var("RUST_BACKTRACE").as_deref(),
        Ok("1") | Ok("full")
    )
}
