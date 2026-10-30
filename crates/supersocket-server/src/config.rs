use std::{
    io,
    path::{Path, PathBuf},
};

use serde_json::Value;

use crate::ServerOptions;

/// Why server options could not be loaded.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// The config file could not be read.
    #[error("cannot read {path}: {source}")]
    Read {
        /// The config file.
        path: PathBuf,
        /// The underlying I/O error.
        source: io::Error,
    },

    /// The file extension is neither `.toml` nor `.json`.
    #[error("unsupported config file {0}: use .toml or .json")]
    UnsupportedFormat(PathBuf),

    /// The file is not valid TOML or JSON.
    #[error("cannot parse {path}: {message}")]
    Parse {
        /// The config file.
        path: PathBuf,
        /// What the parser reported.
        message: String,
    },

    /// An environment override has an invalid name.
    #[error("invalid environment override {name}: {message}")]
    Env {
        /// The variable name.
        name: String,
        /// What is wrong with it.
        message: String,
    },

    /// The values do not form valid options, such as an unknown field or a malformed duration.
    #[error("invalid server options: {0}")]
    Invalid(String),
}

impl ServerOptions {
    /// Loads options from a TOML or JSON file, chosen by extension, then applies overrides from
    /// environment variables prefixed with `SUPERSOCKET`; see [`load_with_env`](Self::load_with_env).
    ///
    /// ```toml
    /// name = "echo"
    /// package_handling_timeout = "10s"
    ///
    /// [[listeners]]
    /// address = "0.0.0.0:4040"
    /// ```
    pub fn load(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        Self::load_with_env(path, "SUPERSOCKET", std::env::vars())
    }

    /// Loads options from a TOML or JSON file, then applies the variables in `vars` whose name
    /// starts with `{prefix}__`.
    ///
    /// The rest of a variable's name is the path of the field it sets, separated by `__`, with
    /// array indexes as numbers: `SUPERSOCKET__LISTENERS__0__ADDRESS=0.0.0.0:4041`. Names are
    /// case-insensitive. A value that parses as a TOML value (a number, a boolean or a quoted
    /// string) is used as such; anything else is a string.
    pub fn load_with_env(
        path: impl AsRef<Path>,
        prefix: &str,
        vars: impl IntoIterator<Item = (String, String)>,
    ) -> Result<Self, ConfigError> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path).map_err(|source| ConfigError::Read {
            path: path.to_owned(),
            source,
        })?;
        let parse_error = |message: String| ConfigError::Parse {
            path: path.to_owned(),
            message,
        };
        let mut value: Value = match path.extension().and_then(|extension| extension.to_str()) {
            Some("toml") => {
                toml::from_str(&text).map_err(|error| parse_error(error.to_string()))?
            }
            Some("json") => {
                serde_json::from_str(&text).map_err(|error| parse_error(error.to_string()))?
            }
            _ => return Err(ConfigError::UnsupportedFormat(path.to_owned())),
        };
        apply_env(&mut value, prefix, vars)?;
        serde_json::from_value(value).map_err(|error| ConfigError::Invalid(error.to_string()))
    }

    /// Parses options from TOML text.
    pub fn from_toml_str(text: &str) -> Result<Self, ConfigError> {
        toml::from_str(text).map_err(|error| ConfigError::Invalid(error.to_string()))
    }
}

fn apply_env(
    root: &mut Value,
    prefix: &str,
    vars: impl IntoIterator<Item = (String, String)>,
) -> Result<(), ConfigError> {
    let prefix = format!("{}__", prefix.to_ascii_uppercase());
    for (name, raw) in vars {
        let Some(path) = name
            .to_ascii_uppercase()
            .strip_prefix(&prefix)
            .map(str::to_ascii_lowercase)
        else {
            continue;
        };
        let segments: Vec<&str> = path.split("__").collect();
        if segments.iter().any(|segment| segment.is_empty()) {
            return Err(ConfigError::Env {
                name,
                message: "empty path segment".into(),
            });
        }
        set_path(root, &segments, parse_env_value(&raw))
            .map_err(|message| ConfigError::Env { name, message })?;
    }
    Ok(())
}

fn parse_env_value(raw: &str) -> Value {
    #[derive(serde::Deserialize)]
    struct Wrapper {
        value: Value,
    }
    toml::from_str::<Wrapper>(&format!("value = {raw}"))
        .map(|wrapper| wrapper.value)
        .unwrap_or_else(|_| Value::String(raw.to_owned()))
}

fn set_path(node: &mut Value, segments: &[&str], value: Value) -> Result<(), String> {
    let (&first, rest) = segments.split_first().expect("the path is not empty");
    let child = if let Ok(index) = first.parse::<usize>() {
        let Value::Array(items) = node else {
            return Err(format!(
                "index {index} used on a value that is not an array"
            ));
        };
        if index > items.len() {
            return Err(format!(
                "index {index} skips items, the array has {}",
                items.len()
            ));
        }
        if index == items.len() {
            items.push(Value::Null);
        }
        &mut items[index]
    } else {
        if node.is_null() {
            *node = Value::Object(Default::default());
        }
        let Value::Object(map) = node else {
            return Err(format!(
                "field `{first}` set on a value that is not a table"
            ));
        };
        map.entry(first.to_owned()).or_insert(Value::Null)
    };
    if rest.is_empty() {
        *child = value;
        return Ok(());
    }
    if child.is_null() && rest[0].parse::<usize>().is_ok() {
        *child = Value::Array(Vec::new());
    }
    set_path(child, rest, value)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::PackageHandling;

    const TOML: &str = r#"
        name = "test"
        package_handling_timeout = "10s"
        idle_session_timeout = "5m"
        package_handling = { concurrent = { max_in_flight = 8 } }

        [[listeners]]
        address = "127.0.0.1:4040"
        no_delay = false
    "#;

    fn write_temp(name: &str, text: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!("supersocket-{}-{name}", std::process::id()));
        std::fs::write(&path, text).unwrap();
        path
    }

    #[test]
    fn parses_toml_with_defaults_for_missing_fields() {
        let options = ServerOptions::from_toml_str(TOML).unwrap();
        assert_eq!(options.name, "test");
        assert_eq!(options.package_handling_timeout, Duration::from_secs(10));
        assert_eq!(options.idle_session_timeout, Some(Duration::from_secs(300)));
        assert_eq!(
            options.package_handling,
            PackageHandling::Concurrent { max_in_flight: 8 }
        );
        assert_eq!(options.listeners[0].address, "127.0.0.1:4040");
        assert!(!options.listeners[0].no_delay);
        assert_eq!(options.listeners[0].backlog, 1024);
        assert_eq!(
            options.max_package_length,
            ServerOptions::default().max_package_length
        );
    }

    #[test]
    fn rejects_unknown_fields() {
        assert!(ServerOptions::from_toml_str("nmae = \"typo\"").is_err());
    }

    #[test]
    fn environment_overrides_file_values() {
        let path = write_temp("env.toml", TOML);
        let vars = [
            ("SUPERSOCKET__NAME", "from-env"),
            ("SUPERSOCKET__MAX_PACKAGE_LENGTH", "2048"),
            ("SUPERSOCKET__SEND_TIMEOUT", "3s"),
            ("SUPERSOCKET__LISTENERS__0__NO_DELAY", "true"),
            ("SUPERSOCKET__LISTENERS__1__ADDRESS", "0.0.0.0:4041"),
            ("OTHER__NAME", "ignored"),
        ]
        .map(|(name, value)| (name.to_owned(), value.to_owned()));
        let options = ServerOptions::load_with_env(&path, "SUPERSOCKET", vars).unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(options.name, "from-env");
        assert_eq!(options.max_package_length, 2048);
        assert_eq!(options.send_timeout, Some(Duration::from_secs(3)));
        assert!(options.listeners[0].no_delay);
        assert_eq!(options.listeners[1].address, "0.0.0.0:4041");
    }

    #[test]
    fn loads_json() {
        let path = write_temp(
            "options.json",
            r#"{"name": "json", "listeners": [{"address": "[::1]:0"}]}"#,
        );
        let options = ServerOptions::load_with_env(&path, "SUPERSOCKET", []).unwrap();
        std::fs::remove_file(path).unwrap();
        assert_eq!(options.name, "json");
        assert_eq!(options.listeners[0].address, "[::1]:0");
    }

    #[test]
    fn reports_bad_overrides() {
        let path = write_temp("bad.toml", TOML);
        let vars = [(
            "SUPERSOCKET__LISTENERS__5__ADDRESS".to_owned(),
            "x".to_owned(),
        )];
        let error = ServerOptions::load_with_env(&path, "SUPERSOCKET", vars).unwrap_err();
        std::fs::remove_file(path).unwrap();
        assert!(matches!(error, ConfigError::Env { .. }), "{error}");
    }
}
