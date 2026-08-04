use std::str::FromStr;
use std::sync::LazyLock;

use regex::bytes::Regex;
use url::ParseError;
use url::Url;

use crate::image::digest::Digest;
use crate::image::digest::DigestError;
use crate::image::tag::Tag;
use crate::image::tag::TagError;

#[derive(thiserror::Error, Debug)]
pub enum ManifestRefError {
    #[error("manifest ref cannot be empty")]
    Empty,
    #[error("manifest ref has invalid namespace: {0}")]
    InvalidNamespace(String),
    #[error("manifest ref has invalid tag: {0}")]
    InvalidTag(TagError),
    #[error("manifest ref has invalid digest: {0}")]
    InvalidDigest(DigestError),
    #[error("manifest ref has invalid url: {0}")]
    InvalidUrl(ParseError),
}

/// https://github.com/opencontainers/image-spec/blob/main/manifest.md
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestRef {
    pub domain: String,
    pub namespace: String,
    pub tag: Option<Tag>,
    pub digest: Option<Digest>,
}

impl ManifestRef {
    pub fn to_url(&self) -> Result<Url, ManifestRefError> {
        let mut url = format!("https://{}/v2/{}/manifests/", self.domain, self.namespace);
        match (&self.tag, &self.digest) {
            (_, Some(digest)) => {
                url.push_str(&digest.to_string());
            }
            (Some(tag), None) => {
                url.push_str(tag.as_ref());
            }
            (None, None) => {
                return Err(ManifestRefError::InvalidDigest(DigestError::Empty));
            }
        };
        let url = Url::parse(&url).map_err(|e| ManifestRefError::InvalidUrl(e))?;
        Ok(url)
    }
}

impl FromStr for ManifestRef {
    type Err = ManifestRefError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.is_empty() {
            return Err(ManifestRefError::Empty);
        }

        let (domain, s) = if let Some(pos) = s.find('/') {
            let (first, second) = s.split_at(pos);
            if first.contains(':') || first.contains('.') || first == "localhost" {
                (first.to_string(), &second[1..])
            } else {
                ("docker.io".to_string(), s)
            }
        } else {
            ("docker.io".to_string(), s)
        };

        // Docker CLI will use "docker.io" as a default registry domain, but the actual registry domain for docker hub is "registry-1.docker.io".
        // We match the behavior here.
        let domain = if domain == "docker.io" {
            "registry-1.docker.io".to_string()
        } else {
            domain
        };

        let (digest, s) = if let Some(pos) = s.find('@') {
            let (first, second) = s.split_at(pos);
            // Wrap digest error with manifest ref error to provide better context
            let digest = second[1..]
                .parse::<Digest>()
                .map_err(|e| ManifestRefError::InvalidDigest(e))?;
            (Some(digest), first)
        } else {
            (None, s)
        };

        let (namespace, tag) = if let Some(pos) = s.find(':') {
            let (first, second) = s.split_at(pos);
            let tag = second[1..]
                .parse::<Tag>()
                .map_err(|e| ManifestRefError::InvalidTag(e))?;
            (first.to_string(), Some(tag))
        } else {
            (s.to_string(), None)
        };

        // TODO: Improve namespace parsing.
        static NAMESPACE_REGEX: LazyLock<Regex> = LazyLock::new(|| {
            Regex::new(r"^[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*(\/[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*)*$")
                .unwrap()
        });
        if !NAMESPACE_REGEX.is_match(&namespace.as_bytes()) {
            return Err(ManifestRefError::InvalidNamespace(namespace));
        }

        let namespace = if domain == "registry-1.docker.io" && !namespace.contains('/') {
            format!("library/{}", namespace)
        } else {
            namespace
        };

        Ok(ManifestRef {
            domain,
            namespace,
            tag,
            digest,
        })
    }
}

impl TryFrom<&str> for ManifestRef {
    type Error = ManifestRefError;

    fn try_from(s: &str) -> Result<ManifestRef, ManifestRefError> {
        ManifestRef::from_str(s)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_manifest_ref_from_str() {
        struct TestCase {
            input: String,
            expected: Result<ManifestRef, ManifestRefError>,
        }

        let test_cases = vec![
            TestCase {
                input: "name".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry-1.docker.io".to_string(),
                    namespace: "library/name".to_string(),
                    tag: None,
                    digest: None,
                }),
            },
            TestCase {
                input: "name:latest".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry-1.docker.io".to_string(),
                    namespace: "library/name".to_string(),
                    tag: Some(Tag::from_str("latest").unwrap()),
                    digest: None,
                }),
            },
            TestCase {
                input: "name@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry-1.docker.io".to_string(),
                    namespace: "library/name".to_string(),
                    tag: None,
                    digest: Some(Digest::Sha256("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string())),
                }),
            },
            TestCase {
                input: "name:latest@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry-1.docker.io".to_string(),
                    namespace: "library/name".to_string(),
                    tag: Some(Tag::from_str("latest").unwrap()),
                    digest: Some(Digest::Sha256("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string())),
                }),
            },
            TestCase {
                input: "name@sha512:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry-1.docker.io".to_string(),
                    namespace: "library/name".to_string(),
                    tag: None,
                    digest: Some(Digest::Sha512("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string())),
                }),
            },
            TestCase {
                input: "name:latest@sha512:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry-1.docker.io".to_string(),
                    namespace: "library/name".to_string(),
                    tag: Some(Tag::from_str("latest").unwrap()),
                    digest: Some(Digest::Sha512("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string())),
                }),
            },
            TestCase {
                input: "repo/name".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry-1.docker.io".to_string(),
                    namespace: "repo/name".to_string(),
                    tag: None,
                    digest: None,
                }),
            },
            TestCase {
                input: "repo/name:latest".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry-1.docker.io".to_string(),
                    namespace: "repo/name".to_string(),
                    tag: Some(Tag::from_str("latest").unwrap()),
                    digest: None,
                }),
            },
            TestCase {
                input: "repo/name@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry-1.docker.io".to_string(),
                    namespace: "repo/name".to_string(),
                    tag: None,
                    digest: Some(Digest::Sha256("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string())),
                }),
            },
            TestCase {
                input: "repo/name:latest@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry-1.docker.io".to_string(),
                    namespace: "repo/name".to_string(),
                    tag: Some(Tag::from_str("latest").unwrap()),
                    digest: Some(Digest::Sha256("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string())),
                }),
            },
            TestCase {
                input: "registry.io/name".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry.io".to_string(),
                    namespace: "name".to_string(),
                    tag: None,
                    digest: None,
                }),
            },
            TestCase {
                input: "registry.io/name:latest".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry.io".to_string(),
                    namespace: "name".to_string(),
                    tag: Some(Tag::from_str("latest").unwrap()),
                    digest: None,
                }),
            },
            TestCase {
                input: "registry.io/name@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry.io".to_string(),
                    namespace: "name".to_string(),
                    tag: None,
                    digest: Some(Digest::Sha256("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string())),
                }),
            },
            TestCase {
                input: "registry.io/name:latest@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry.io".to_string(),
                    namespace: "name".to_string(),
                    tag: Some(Tag::from_str("latest").unwrap()),
                    digest: Some(Digest::Sha256("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string())),
                }),
            },
            TestCase {
                input: "registry.io/repo/name".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry.io".to_string(),
                    namespace: "repo/name".to_string(),
                    tag: None,
                    digest: None,
                }),
            },
            TestCase {
                input: "registry.io/repo/name:latest".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry.io".to_string(),
                    namespace: "repo/name".to_string(),
                    tag: Some(Tag::from_str("latest").unwrap()),
                    digest: None,
                }),
            },
            TestCase {
                input: "registry.io/repo/name@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry.io".to_string(),
                    namespace: "repo/name".to_string(),
                    tag: None,
                    digest: Some(Digest::Sha256("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string())),
                }),
            },
            TestCase {
                input: "registry.io/repo/name:latest@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry.io".to_string(),
                    namespace: "repo/name".to_string(),
                    tag: Some(Tag::from_str("latest").unwrap()),
                    digest: Some(Digest::Sha256("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string())),
                }),
            },
            TestCase {
                input: "registry.io/repo/name:latest@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
                expected: Ok(ManifestRef {
                    domain: "registry.io".to_string(),
                    namespace: "repo/name".to_string(),
                    tag: Some(Tag::from_str("latest").unwrap()),
                    digest: Some(Digest::Sha256("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string())),
                }),
            },
            TestCase {
                input: "localhost/repo/name".to_string(),
                expected: Ok(ManifestRef {
                    domain: "localhost".to_string(),
                    namespace: "repo/name".to_string(),
                    tag: None,
                    digest: None,
                }),
            },
            TestCase {
                input: "localhost/repo/name:latest".to_string(),
                expected: Ok(ManifestRef {
                    domain: "localhost".to_string(),
                    namespace: "repo/name".to_string(),
                    tag: Some(Tag::from_str("latest").unwrap()),
                    digest: None,
                }),
            },
            TestCase {
                input: "localhost/repo/name@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
                expected: Ok(ManifestRef {
                    domain: "localhost".to_string(),
                    namespace: "repo/name".to_string(),
                    tag: None,
                    digest: Some(Digest::Sha256("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string())),
                }),
            },
            TestCase {
                input: "localhost/repo/name:latest@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
                expected: Ok(ManifestRef {
                    domain: "localhost".to_string(),
                    namespace: "repo/name".to_string(),
                    tag: Some(Tag::from_str("latest").unwrap()),
                    digest: Some(Digest::Sha256("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string())),
                }),
            },
            TestCase {
                input: "domain:port/repo/name".to_string(),
                expected: Ok(ManifestRef {
                    domain: "domain:port".to_string(),
                    namespace: "repo/name".to_string(),
                    tag: None,
                    digest: None,
                }),
            },
            TestCase {
                input: "domain:port/repo/name:latest".to_string(),
                expected: Ok(ManifestRef {
                    domain: "domain:port".to_string(),
                    namespace: "repo/name".to_string(),
                    tag: Some(Tag::from_str("latest").unwrap()),
                    digest: None,
                }),
            },
            TestCase {
                input: "domain:port/repo/name@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
                expected: Ok(ManifestRef {
                    domain: "domain:port".to_string(),
                    namespace: "repo/name".to_string(),
                    tag: None,
                    digest: Some(Digest::Sha256("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string())),
                }),
            },
            TestCase {
                input: "domain:port/repo/name:latest@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string(),
                expected: Ok(ManifestRef {
                    domain: "domain:port".to_string(),
                    namespace: "repo/name".to_string(),
                    tag: Some(Tag::from_str("latest").unwrap()),
                    digest: Some(Digest::Sha256("0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef".to_string())),
                }),
            },
            TestCase {
                input: "Invalid/Path/Format/".to_string(),
                expected: Err(ManifestRefError::InvalidNamespace("Invalid/Path/Format/".to_string()).into()),
            },
            TestCase {
                input: "name:Invalid Tag Format".to_string(),
                expected: Err(ManifestRefError::InvalidTag(TagError::InvalidFormat("Invalid Tag Format".to_string())).into()),
            },
            TestCase {
                input: "name@InvalidDigestFormat".to_string(),
                expected: Err(ManifestRefError::InvalidDigest(DigestError::InvalidFormat("InvalidDigestFormat".to_string())).into()),
            },
            TestCase {
                input: "name@unknownalgo:0123456789abcdef".to_string(),
                expected: Err(ManifestRefError::InvalidDigest(DigestError::UnsupportedAlgorithm("unknownalgo".to_string()).into()).into()),
            },
            TestCase {
                input: "name@sha256:invalidsha256hex".to_string(),
                expected: Err(ManifestRefError::InvalidDigest(DigestError::InvalidSha256Hex("invalidsha256hex".to_string()).into()).into()),
            },
            TestCase {
                input: "name@sha512:invalidsha512hex".to_string(),
                expected: Err(ManifestRefError::InvalidDigest(DigestError::InvalidSha512Hex("invalidsha512hex".to_string()).into()).into()),
            },
        ];

        for test_case in test_cases {
            let got = ManifestRef::from_str(&test_case.input).map_err(|e| e.to_string());
            let want = test_case.expected.map_err(|e| e.to_string());
            if got != want {
                panic!(
                    "test case failed for input '{}'\ngot '{:?}'\nwant '{:?}'",
                    test_case.input, got, want
                );
            }
        }
    }
}
