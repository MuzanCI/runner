use std::str::FromStr;
use std::sync::LazyLock;

use regex::Regex;
use serde::Deserialize;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tag(String);

#[derive(thiserror::Error, Debug, Clone)]
pub enum TagError {
    #[error("tag cannot be empty")]
    Empty,
    #[error("invalid tag format: {0}")]
    InvalidFormat(String),
}

impl FromStr for Tag {
    type Err = TagError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(TagError::Empty);
        }

        static TAG_REGEX: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"^[\w][\w.-]{0,127}$").unwrap());

        if !TAG_REGEX.is_match(s) {
            return Err(TagError::InvalidFormat(s.to_string()));
        }
        Ok(Tag(s.to_string()))
    }
}

impl AsRef<str> for Tag {
    fn as_ref(&self) -> &str {
        &self.0
    }
}
