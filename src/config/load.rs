use std::{
    borrow::Cow,
    io,
    path::{Path, PathBuf},
};

use serde::de::DeserializeOwned;
use thiserror::Error;

/// Directory containing configuration files relative to the application root.
const CONFIGURATION_DIR: &str = "configuration";

/// Environment variable for specifying an absolute path to the configuration directory.
const CONFIG_DIR_ENV_VAR: &str = "APP_CONFIG_DIR";

/// Supported extensions for base and environment configuration files.
const CONFIG_FILE_EXTENSIONS: &[&str] = &["yaml", "yml", "json"];

/// Prefix for environment variable configuration overrides.
const ENV_PREFIX: &str = "APP";

/// Separator between environment variable prefix and key segments.
const ENV_PREFIX_SEPARATOR: &str = "_";

/// Separator for nested configuration keys in environment variables.
const ENV_SEPARATOR: &str = "__";

/// Separator for list elements in environment variables.
const LIST_SEPARATOR: &str = ",";

/// Environment variable name containing the environment identifier.
const APP_ENVIRONMENT_ENV_NAME: &str = "APP_ENVIRONMENT";

/// Trait implemented by configuration structures that require list parsing help.
pub trait Config {
    /// Keys whose values should be parsed as lists when loading the configuration.
    const LIST_PARSE_KEYS: &'static [&'static str];
}

/// Runtime environment for the application.
#[derive(Debug, Clone, Copy)]
enum Environment {
    Prod,
    Staging,
    Dev,
}

impl Environment {
    /// Loads the environment from the `APP_ENVIRONMENT` environment variable.
    ///
    /// Defaults to [`Environment::Prod`] if the variable is not set.
    fn load() -> Result<Environment, io::Error> {
        std::env::var(APP_ENVIRONMENT_ENV_NAME)
            .unwrap_or_else(|_| "prod".into())
            .try_into()
    }
}

impl std::fmt::Display for Environment {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Environment::Prod => write!(f, "prod"),
            Environment::Staging => write!(f, "staging"),
            Environment::Dev => write!(f, "dev"),
        }
    }
}

impl TryFrom<String> for Environment {
    type Error = io::Error;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        match s.to_lowercase().as_str() {
            "prod" => Ok(Self::Prod),
            "staging" => Ok(Self::Staging),
            "dev" => Ok(Self::Dev),
            other => Err(io::Error::other(format!(
                "{other} is not a supported environment. Use either `prod`/`staging`/`dev`.",
            ))),
        }
    }
}

/// Identifies which configuration file is currently being loaded.
#[derive(Debug, Clone, Copy)]
enum ConfigFileKind {
    /// Always-present base configuration that every service loads.
    Base,
    /// Environment-specific overrides (dev/staging/prod).
    Environment(Environment),
}

impl ConfigFileKind {
    fn stem(&self) -> Cow<'static, str> {
        match self {
            ConfigFileKind::Base => Cow::Borrowed("base"),
            ConfigFileKind::Environment(env) => Cow::Owned(env.to_string()),
        }
    }

    /// Returns a static string describing this configuration file kind for error messages.
    fn as_str(&self) -> &'static str {
        match self {
            ConfigFileKind::Base => "base",
            ConfigFileKind::Environment(Environment::Dev) => "dev",
            ConfigFileKind::Environment(Environment::Staging) => "staging",
            ConfigFileKind::Environment(Environment::Prod) => "prod",
        }
    }
}

/// Errors that can occur while loading configuration files and overrides.
#[derive(Debug, Error)]
pub enum LoadConfigError {
    /// Failed to determine the current working directory.
    #[error("failed to determine the current directory")]
    CurrentDir(#[source] io::Error),

    /// The configured `configuration` directory does not exist.
    #[error("configuration directory `{0}` does not exist")]
    MissingConfigurationDirectory(PathBuf),

    /// Could not locate one of the required configuration files.
    #[error("could not locate {kind} configuration in `{directory}`; attempted: {attempted}")]
    ConfigurationFileMissing {
        kind: &'static str,
        directory: PathBuf,
        attempted: String,
    },

    /// Environment variable overrides failed to merge into the configuration.
    #[error("failed to load configuration from environment variables")]
    EnvironmentVariables(#[source] config::ConfigError),

    /// The configuration files were parsed but deserialization failed.
    #[error("failed to deserialize configuration")]
    Deserialization(#[source] config::ConfigError),

    /// Failed to determine the runtime environment (`APP_ENVIRONMENT`).
    #[error("failed to determine runtime environment")]
    Environment(#[source] io::Error),

    /// Failed to initialize the configuration builder.
    #[error("failed to initialize configuration builder")]
    Builder(#[source] config::ConfigError),
}

/// Loads hierarchical configuration from base, environment, and environment-variable sources.
///
/// The configuration directory is determined by:
/// - First checking the `APP_CONFIG_DIR` environment variable for an absolute path
/// - If not set, using `<current_dir>/configuration`
///
/// Loads files from `base.(yaml|yml|json)` (required) and `{environment}.(yaml|yml|json)` (optional)
/// before applying overrides from `APP_`-prefixed environment variables.
///
/// Nested keys use double underscores (`APP_SERVICE__HOST`), and list values are comma-separated.
pub fn load_config<T>() -> Result<T, LoadConfigError>
where
    T: Config + DeserializeOwned,
{
    let configuration_directory = if let Ok(config_dir) = std::env::var(CONFIG_DIR_ENV_VAR) {
        // Use the absolute path provided by APP_CONFIG_DIR
        PathBuf::from(config_dir)
    } else {
        // Fallback to <current_dir>/configuration
        let base_path = std::env::current_dir().map_err(LoadConfigError::CurrentDir)?;
        base_path.join(CONFIGURATION_DIR)
    };

    if !configuration_directory.is_dir() {
        return Err(LoadConfigError::MissingConfigurationDirectory(
            configuration_directory,
        ));
    }

    let environment = Environment::load().map_err(LoadConfigError::Environment)?;

    // Base file is required
    let base_file = find_configuration_file(&configuration_directory, ConfigFileKind::Base)
        .ok_or_else(|| {
            let stem = ConfigFileKind::Base.stem();
            let attempted = CONFIG_FILE_EXTENSIONS
                .iter()
                .map(|ext| {
                    format!(
                        "`{}`",
                        configuration_directory
                            .join(format!("{stem}.{ext}"))
                            .display()
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");

            LoadConfigError::ConfigurationFileMissing {
                kind: ConfigFileKind::Base.as_str(),
                directory: configuration_directory.clone(),
                attempted,
            }
        })?;

    // Environment-specific file is optional
    let environment_file = find_configuration_file(
        &configuration_directory,
        ConfigFileKind::Environment(environment),
    );

    let mut environment_source = config::Environment::with_prefix(ENV_PREFIX)
        .prefix_separator(ENV_PREFIX_SEPARATOR)
        .separator(ENV_SEPARATOR);

    if !T::LIST_PARSE_KEYS.is_empty() {
        environment_source = environment_source
            .try_parsing(true)
            .list_separator(LIST_SEPARATOR);

        for key in <T as Config>::LIST_PARSE_KEYS {
            environment_source = environment_source.with_list_parse_key(key);
        }
    }

    let mut builder = config::Config::builder();

    // Always add base file (required)
    builder = builder.add_source(config::File::from(base_file));

    // Optionally add environment file if it exists
    if let Some(env_file) = environment_file {
        builder = builder.add_source(config::File::from(env_file));
    }

    // Always add environment variables (highest priority)
    builder = builder.add_source(environment_source);

    let settings = builder.build().map_err(LoadConfigError::Builder)?;

    settings
        .try_deserialize::<T>()
        .map_err(LoadConfigError::Deserialization)
}

/// Finds the configuration file that matches the requested kind and supported extensions.
/// Returns `Some(PathBuf)` if found, `None` otherwise.
fn find_configuration_file(directory: &Path, kind: ConfigFileKind) -> Option<PathBuf> {
    let stem = kind.stem();

    for extension in CONFIG_FILE_EXTENSIONS {
        let path = directory.join(format!("{stem}.{extension}"));
        if path.is_file() {
            return Some(path);
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::fs;
    use temp_env::with_vars;
    use tempfile::TempDir;

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    struct TestConfig {
        value: String,
        number: i32,
    }

    impl Config for TestConfig {
        const LIST_PARSE_KEYS: &'static [&'static str] = &[];
    }

    #[test]
    fn test_load_with_base_only() {
        let temp_dir = TempDir::new().unwrap();
        let config_dir = temp_dir.path().join("configuration");
        fs::create_dir(&config_dir).unwrap();

        let base_content = "value: \"from_base\"\nnumber: 42\n";
        fs::write(config_dir.join("base.yml"), base_content).unwrap();

        with_vars(
            vec![
                ("APP_ENVIRONMENT", Some("prod")),
                ("APP_CONFIG_DIR", Some(config_dir.to_str().unwrap())),
                ("APP_VALUE", None),
                ("APP_NUMBER", None),
            ],
            || {
                let config: TestConfig = load_config().unwrap();
                assert_eq!(config.value, "from_base");
                assert_eq!(config.number, 42);
            },
        );
    }

    #[test]
    fn test_load_with_base_and_env_file() {
        let temp_dir = TempDir::new().unwrap();
        let config_dir = temp_dir.path().join("configuration");
        fs::create_dir(&config_dir).unwrap();

        let base_content = "value: \"from_base\"\nnumber: 42\n";
        fs::write(config_dir.join("base.yml"), base_content).unwrap();

        let dev_content = "value: \"from_dev\"\nnumber: 99\n";
        fs::write(config_dir.join("dev.yml"), dev_content).unwrap();

        with_vars(
            vec![
                ("APP_ENVIRONMENT", Some("dev")),
                ("APP_CONFIG_DIR", Some(config_dir.to_str().unwrap())),
                ("APP_VALUE", None),
                ("APP_NUMBER", None),
            ],
            || {
                let config: TestConfig = load_config().unwrap();
                assert_eq!(config.value, "from_dev");
                assert_eq!(config.number, 99);
            },
        );
    }

    #[test]
    fn test_env_vars_override_files() {
        let temp_dir = TempDir::new().unwrap();
        let config_dir = temp_dir.path().join("configuration");
        fs::create_dir(&config_dir).unwrap();

        let base_content = "value: \"from_base\"\nnumber: 42\n";
        fs::write(config_dir.join("base.yml"), base_content).unwrap();

        with_vars(
            vec![
                ("APP_ENVIRONMENT", Some("prod")),
                ("APP_CONFIG_DIR", Some(config_dir.to_str().unwrap())),
                ("APP_VALUE", Some("from_env")),
                ("APP_NUMBER", Some("123")),
            ],
            || {
                let config: TestConfig = load_config().unwrap();
                assert_eq!(config.value, "from_env");
                assert_eq!(config.number, 123);
            },
        );
    }

    #[test]
    fn test_missing_base_file_fails() {
        let temp_dir = TempDir::new().unwrap();
        let config_dir = temp_dir.path().join("configuration");
        fs::create_dir(&config_dir).unwrap();

        with_vars(
            vec![
                ("APP_ENVIRONMENT", Some("prod")),
                ("APP_CONFIG_DIR", Some(config_dir.to_str().unwrap())),
            ],
            || {
                let result = load_config::<TestConfig>();
                assert!(result.is_err());
                assert!(matches!(
                    result.unwrap_err(),
                    LoadConfigError::ConfigurationFileMissing { .. }
                ));
            },
        );
    }

    #[test]
    fn test_missing_env_file_succeeds() {
        let temp_dir = TempDir::new().unwrap();
        let config_dir = temp_dir.path().join("configuration");
        fs::create_dir(&config_dir).unwrap();

        let base_content = "value: \"from_base\"\nnumber: 42\n";
        fs::write(config_dir.join("base.yml"), base_content).unwrap();

        with_vars(
            vec![
                ("APP_ENVIRONMENT", Some("staging")),
                ("APP_CONFIG_DIR", Some(config_dir.to_str().unwrap())),
                ("APP_VALUE", None),
                ("APP_NUMBER", None),
            ],
            || {
                let config: TestConfig = load_config().unwrap();
                assert_eq!(config.value, "from_base");
                assert_eq!(config.number, 42);
            },
        );
    }

    #[test]
    fn test_app_config_dir_env_var() {
        let temp_dir = TempDir::new().unwrap();
        let custom_dir = temp_dir.path().join("my-config");
        fs::create_dir(&custom_dir).unwrap();

        let base_content = "value: \"custom_dir\"\nnumber: 777\n";
        fs::write(custom_dir.join("base.yml"), base_content).unwrap();

        with_vars(
            vec![
                ("APP_ENVIRONMENT", Some("prod")),
                ("APP_CONFIG_DIR", Some(custom_dir.to_str().unwrap())),
                ("APP_VALUE", None),
                ("APP_NUMBER", None),
            ],
            || {
                let config: TestConfig = load_config().unwrap();
                assert_eq!(config.value, "custom_dir");
                assert_eq!(config.number, 777);
            },
        );
    }

    #[test]
    fn test_environment_defaults_to_prod() {
        let temp_dir = TempDir::new().unwrap();
        let config_dir = temp_dir.path().join("configuration");
        fs::create_dir(&config_dir).unwrap();

        let base_content = "value: \"base\"\nnumber: 1\n";
        fs::write(config_dir.join("base.yml"), base_content).unwrap();

        let prod_content = "value: \"prod\"\nnumber: 2\n";
        fs::write(config_dir.join("prod.yml"), prod_content).unwrap();

        with_vars(
            [
                ("APP_ENVIRONMENT", None::<&str>),
                ("APP_CONFIG_DIR", Some(config_dir.to_str().unwrap())),
                ("APP_VALUE", None::<&str>),
                ("APP_NUMBER", None::<&str>),
            ],
            || {
                let config: TestConfig = load_config().unwrap();
                assert_eq!(config.value, "prod");
                assert_eq!(config.number, 2);
            },
        );
    }
}
