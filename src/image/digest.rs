use std::fmt::Display;
use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;

#[derive(thiserror::Error, Debug, Clone)]
pub enum DigestError {
    #[error("digest cannot be empty")]
    Empty,
    #[error("unsupported digest algorithm: {0}")]
    UnsupportedAlgorithm(String),
    #[error("expected digest format <algo>:<hex>, got invalid digest format: {0}")]
    InvalidFormat(String),
    #[error("invalid sha256 hex: {0}")]
    InvalidSha256Hex(String),
    #[error("invalid sha512 hex: {0}")]
    InvalidSha512Hex(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Digest {
    Sha256(String),
    Sha512(String),
}

impl Digest {
    fn parse_sha256_hex(s: &str) -> Result<String, DigestError> {
        if s.len() != 64 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(DigestError::InvalidSha256Hex(s.to_string()));
        }
        Ok(s.to_string())
    }

    fn parse_sha512_hex(s: &str) -> Result<String, DigestError> {
        if s.len() != 128 || !s.chars().all(|c| c.is_ascii_hexdigit()) {
            return Err(DigestError::InvalidSha512Hex(s.to_string()));
        }
        Ok(s.to_string())
    }
}

impl FromStr for Digest {
    type Err = DigestError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(DigestError::Empty);
        }

        let (algo, hex) = if let Some(pos) = s.find(':') {
            s.split_at(pos)
        } else {
            return Err(DigestError::InvalidFormat(s.to_string()));
        };

        match algo {
            "sha256" => Ok(Digest::Sha256(Digest::parse_sha256_hex(&hex[1..])?)),
            "sha512" => Ok(Digest::Sha512(Digest::parse_sha512_hex(&hex[1..])?)),
            _ => Err(DigestError::UnsupportedAlgorithm(algo.to_string())),
        }
    }
}

impl From<sha2::Sha256> for Digest {
    fn from(hasher: sha2::Sha256) -> Self {
        use sha2::Digest as _;
        Digest::Sha256(hex::encode(hasher.finalize()))
    }
}

impl From<sha2::Sha512> for Digest {
    fn from(hasher: sha2::Sha512) -> Self {
        use sha2::Digest as _;
        Digest::Sha512(hex::encode(hasher.finalize()))
    }
}

impl Display for Digest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Digest::Sha256(hex) => write!(f, "sha256:{}", hex),
            Digest::Sha512(hex) => write!(f, "sha512:{}", hex),
        }
    }
}

impl Serialize for Digest {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Digest {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct DigestVisitor;

        impl serde::de::Visitor<'_> for DigestVisitor {
            type Value = Digest;

            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a digest string like \"sha256:<hex>\"")
            }

            fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Digest, E> {
                v.parse::<Digest>().map_err(E::custom)
            }
        }

        deserializer.deserialize_str(DigestVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::Digest;
    use serde::Deserialize;
    use serde::Serialize;

    #[test]
    fn test_digest_serde() {
        let sha256 = "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
        let sha512 = "sha512:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

        // Serialization
        let d = Digest::Sha256(
            "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
        );
        assert_eq!(
            serde_json::to_string(&d).unwrap(),
            format!("\"{}\"", sha256)
        );

        let d = Digest::Sha512("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string());
        assert_eq!(
            serde_json::to_string(&d).unwrap(),
            format!("\"{}\"", sha512)
        );

        // Deserialization
        let d: Digest = serde_json::from_str(&format!("\"{}\"", sha256)).unwrap();
        assert_eq!(
            d,
            Digest::Sha256(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string()
            )
        );

        let d: Digest = serde_json::from_str(&format!("\"{}\"", sha512)).unwrap();
        assert_eq!(d, Digest::Sha512("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string()));

        // Invalid digest
        assert!(serde_json::from_str::<Digest>("\"invalid\"").is_err());

        // Struct round-trip
        #[derive(Serialize, Deserialize)]
        struct Wrapper {
            digest: Digest,
        }
        let json = format!(r#"{{"digest":"{}"}}"#, sha256);
        let w: Wrapper = serde_json::from_str(&json).unwrap();
        assert_eq!(
            w.digest,
            Digest::Sha256(
                "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string()
            )
        );
        assert_eq!(serde_json::to_string(&w).unwrap(), json);
    }
}
