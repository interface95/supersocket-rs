//! Deserializers for the loose types that config files and environment overrides produce:
//! numbers written as strings, strings that look like numbers or booleans, and durations written
//! as `"30s"` or as a number of seconds.

use std::{path::PathBuf, time::Duration};

use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

#[derive(Deserialize)]
#[serde(untagged)]
enum Scalar {
    Bool(bool),
    Unsigned(u64),
    Signed(i64),
    Float(f64),
    Text(String),
}

impl Scalar {
    fn describe(&self) -> String {
        match self {
            Self::Bool(value) => format!("boolean `{value}`"),
            Self::Unsigned(value) => format!("integer `{value}`"),
            Self::Signed(value) => format!("integer `{value}`"),
            Self::Float(value) => format!("number `{value}`"),
            Self::Text(value) => format!("string {value:?}"),
        }
    }

    fn into_text(self) -> String {
        match self {
            Self::Bool(value) => value.to_string(),
            Self::Unsigned(value) => value.to_string(),
            Self::Signed(value) => value.to_string(),
            Self::Float(value) => value.to_string(),
            Self::Text(value) => value,
        }
    }
}

pub(crate) fn string<'de, D: Deserializer<'de>>(deserializer: D) -> Result<String, D::Error> {
    Scalar::deserialize(deserializer).map(Scalar::into_text)
}

pub(crate) fn path<'de, D: Deserializer<'de>>(deserializer: D) -> Result<PathBuf, D::Error> {
    string(deserializer).map(PathBuf::from)
}

fn unsigned<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
    unsigned_from(Scalar::deserialize(deserializer)?)
}

fn unsigned_from<E: serde::de::Error>(scalar: Scalar) -> Result<u64, E> {
    match scalar {
        Scalar::Unsigned(value) => Ok(value),
        Scalar::Text(text) => text.trim().parse().map_err(|_| {
            E::custom(format!(
                "expected a non-negative integer, found string {text:?}"
            ))
        }),
        other => Err(E::custom(format!(
            "expected a non-negative integer, found {}",
            other.describe()
        ))),
    }
}

pub(crate) fn option_path<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<PathBuf>, D::Error> {
    Ok(
        Option::<Scalar>::deserialize(deserializer)?
            .map(|scalar| PathBuf::from(scalar.into_text())),
    )
}

pub(crate) fn usize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<usize, D::Error> {
    let value = unsigned(deserializer)?;
    usize::try_from(value).map_err(|_| D::Error::custom(format!("{value} is too large")))
}

pub(crate) fn u32<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u32, D::Error> {
    let value = unsigned(deserializer)?;
    u32::try_from(value).map_err(|_| D::Error::custom(format!("{value} is too large")))
}

pub(crate) fn option_u32<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<u32>, D::Error> {
    Option::<Scalar>::deserialize(deserializer)?
        .map(|scalar| {
            let value = unsigned_from(scalar)?;
            u32::try_from(value).map_err(|_| D::Error::custom(format!("{value} is too large")))
        })
        .transpose()
}

pub(crate) fn bool<'de, D: Deserializer<'de>>(deserializer: D) -> Result<bool, D::Error> {
    match Scalar::deserialize(deserializer)? {
        Scalar::Bool(value) => Ok(value),
        Scalar::Text(text) => match text.trim().to_ascii_lowercase().as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => Err(D::Error::custom(format!(
                "expected a boolean, found string {text:?}"
            ))),
        },
        other => Err(D::Error::custom(format!(
            "expected a boolean, found {}",
            other.describe()
        ))),
    }
}

fn to_duration<E: serde::de::Error>(scalar: Scalar) -> Result<Duration, E> {
    match scalar {
        Scalar::Unsigned(seconds) => Ok(Duration::from_secs(seconds)),
        Scalar::Float(seconds) if seconds.is_finite() && seconds >= 0.0 => {
            Ok(Duration::from_secs_f64(seconds))
        }
        Scalar::Text(text) => humantime::parse_duration(text.trim())
            .map_err(|error| E::custom(format!("invalid duration {text:?}: {error}"))),
        other => Err(E::custom(format!(
            "expected a duration such as \"30s\", found {}",
            other.describe()
        ))),
    }
}

fn format_duration(duration: Duration) -> String {
    humantime::format_duration(duration).to_string()
}

pub(crate) mod duration {
    use super::*;

    pub(crate) fn serialize<S: Serializer>(
        duration: &Duration,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format_duration(*duration))
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Duration, D::Error> {
        to_duration(Scalar::deserialize(deserializer)?)
    }
}

pub(crate) mod option_duration {
    use super::*;

    pub(crate) fn serialize<S: Serializer>(
        duration: &Option<Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match duration {
            Some(duration) => serializer.serialize_some(&format_duration(*duration)),
            None => serializer.serialize_none(),
        }
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Duration>, D::Error> {
        Option::<Scalar>::deserialize(deserializer)?
            .map(to_duration)
            .transpose()
    }
}
