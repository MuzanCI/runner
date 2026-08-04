use std::collections::HashMap;
use std::str::FromStr;

use serde::Deserialize;
use serde::Serialize;

use crate::image::digest::Digest;

// TODO: Improve parsing error messages.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum MediaType {
    #[serde(rename = "application/vnd.oci.image.manifest.v1+json")]
    OciImageManifestV1Json,
    #[serde(rename = "application/vnd.oci.image.index.v1+json")]
    OciImageIndexV1Json,
    #[serde(rename = "application/vnd.oci.image.config.v1+json")]
    OciImageConfigV1Json,
    #[serde(rename = "application/vnd.oci.image.layer.v1.tar+gzip")]
    OciImageLayerV1TarGzip,
}

#[derive(thiserror::Error, Debug)]
#[error("invalid media type: {0}")]
pub struct ParseMediaTypeError(String);

impl FromStr for MediaType {
    type Err = ParseMediaTypeError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "application/vnd.oci.image.manifest.v1+json" => Ok(Self::OciImageManifestV1Json),
            "application/vnd.oci.image.index.v1+json" => Ok(Self::OciImageIndexV1Json),
            "application/vnd.oci.image.config.v1+json" => Ok(Self::OciImageConfigV1Json),
            "application/vnd.oci.image.layer.v1.tar+gzip" => Ok(Self::OciImageLayerV1TarGzip),
            _ => Err(ParseMediaTypeError(s.to_string())),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Descriptor {
    #[serde(rename = "mediaType")]
    pub media_type: MediaType,
    pub digest: Digest,
    pub size: usize,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageIndex {
    #[serde(rename = "schemaVersion")]
    pub schema_version: u8,
    #[serde(rename = "mediaType")]
    pub media_type: MediaType,
    pub manifests: Option<Vec<ImageManifestDescriptor>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImagePlatform {
    pub architecture: String,
    pub os: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageManifestDescriptor {
    #[serde(rename = "mediaType")]
    pub media_type: MediaType,
    pub digest: Digest,
    pub size: u64,
    pub platform: ImagePlatform,
    pub annotations: Option<HashMap<String, String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageManifest {
    #[serde(rename = "schemaVersion")]
    pub schema_version: u8,
    #[serde(rename = "mediaType")]
    pub media_type: MediaType,
    pub annotations: Option<HashMap<String, String>>,
    pub config: Descriptor,
    pub layers: Vec<Descriptor>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImageConfig {
    pub architecture: String,
    pub os: String,
    pub rootfs: Rootfs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Rootfs {
    pub diff_ids: Vec<Digest>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_parse_image_index_json() {
        let input = r#"{
  "manifests": [
    {
      "annotations": {
        "com.docker.official-images.bashbrew.arch": "amd64",
        "org.opencontainers.image.base.name": "scratch",
        "org.opencontainers.image.created": "2026-04-15T20:01:12Z",
        "org.opencontainers.image.revision": "c68e08480b8fb053591ade7dbaffa2ea67db2f56",
        "org.opencontainers.image.source": "https://github.com/alpinelinux/docker-alpine.git#c68e08480b8fb053591ade7dbaffa2ea67db2f56:x86_64",
        "org.opencontainers.image.url": "https://hub.docker.com/_/alpine",
        "org.opencontainers.image.version": "3.23.4"
      },
      "digest": "sha256:4d889c14e7d5a73929ab00be2ef8ff22437e7cbc545931e52554a7b00e123d8b",
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "platform": {
        "architecture": "amd64",
        "os": "linux"
      },
      "size": 1022
    },
    {
      "annotations": {
        "com.docker.official-images.bashbrew.arch": "amd64",
        "vnd.docker.reference.digest": "sha256:4d889c14e7d5a73929ab00be2ef8ff22437e7cbc545931e52554a7b00e123d8b",
        "vnd.docker.reference.type": "attestation-manifest"
      },
      "digest": "sha256:452bfe804076a924cc3982dfe3a7d760a387d8332fa32b8f7050d763895f901f",
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "platform": {
        "architecture": "unknown",
        "os": "unknown"
      },
      "size": 838
    },
    {
      "annotations": {
        "com.docker.official-images.bashbrew.arch": "arm32v6",
        "org.opencontainers.image.base.name": "scratch",
        "org.opencontainers.image.created": "2026-04-15T20:02:01Z",
        "org.opencontainers.image.revision": "c68e08480b8fb053591ade7dbaffa2ea67db2f56",
        "org.opencontainers.image.source": "https://github.com/alpinelinux/docker-alpine.git#c68e08480b8fb053591ade7dbaffa2ea67db2f56:armhf",
        "org.opencontainers.image.url": "https://hub.docker.com/_/alpine",
        "org.opencontainers.image.version": "3.23.4"
      },
      "digest": "sha256:9f806c580b6a59b5f64bce6cefc061709a910008d527659d8293ef67bae44270",
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "platform": {
        "architecture": "arm",
        "os": "linux",
        "variant": "v6"
      },
      "size": 1023
    },
    {
      "annotations": {
        "com.docker.official-images.bashbrew.arch": "arm32v6",
        "vnd.docker.reference.digest": "sha256:9f806c580b6a59b5f64bce6cefc061709a910008d527659d8293ef67bae44270",
        "vnd.docker.reference.type": "attestation-manifest"
      },
      "digest": "sha256:87a847f541b77d68b0b3ee2603b84fc9ba8e29cef9b5a3eeb2de0caaba412338",
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "platform": {
        "architecture": "unknown",
        "os": "unknown"
      },
      "size": 566
    },
    {
      "annotations": {
        "com.docker.official-images.bashbrew.arch": "arm32v7",
        "org.opencontainers.image.base.name": "scratch",
        "org.opencontainers.image.created": "2026-04-15T20:00:52Z",
        "org.opencontainers.image.revision": "c68e08480b8fb053591ade7dbaffa2ea67db2f56",
        "org.opencontainers.image.source": "https://github.com/alpinelinux/docker-alpine.git#c68e08480b8fb053591ade7dbaffa2ea67db2f56:armv7",
        "org.opencontainers.image.url": "https://hub.docker.com/_/alpine",
        "org.opencontainers.image.version": "3.23.4"
      },
      "digest": "sha256:0be3c29c7b8d475f38f71ac3d25eb5eb673c68cc673576996cb2afd7a536829a",
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "platform": {
        "architecture": "arm",
        "os": "linux",
        "variant": "v7"
      },
      "size": 1023
    },
    {
      "annotations": {
        "com.docker.official-images.bashbrew.arch": "arm32v7",
        "vnd.docker.reference.digest": "sha256:0be3c29c7b8d475f38f71ac3d25eb5eb673c68cc673576996cb2afd7a536829a",
        "vnd.docker.reference.type": "attestation-manifest"
      },
      "digest": "sha256:634e2191e8084f16a8bda925c26b53f6f0558b54836b6d5fe0fe7fe45ca9cea8",
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "platform": {
        "architecture": "unknown",
        "os": "unknown"
      },
      "size": 838
    },
    {
      "annotations": {
        "com.docker.official-images.bashbrew.arch": "arm64v8",
        "org.opencontainers.image.base.name": "scratch",
        "org.opencontainers.image.created": "2026-04-15T20:01:01Z",
        "org.opencontainers.image.revision": "c68e08480b8fb053591ade7dbaffa2ea67db2f56",
        "org.opencontainers.image.source": "https://github.com/alpinelinux/docker-alpine.git#c68e08480b8fb053591ade7dbaffa2ea67db2f56:aarch64",
        "org.opencontainers.image.url": "https://hub.docker.com/_/alpine",
        "org.opencontainers.image.version": "3.23.4"
      },
      "digest": "sha256:378c4c5418f7493bd500ad21ffb43818d0689daaad43e3261859fb417d1481a0",
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "platform": {
        "architecture": "arm64",
        "os": "linux",
        "variant": "v8"
      },
      "size": 1025
    },
    {
      "annotations": {
        "com.docker.official-images.bashbrew.arch": "arm64v8",
        "vnd.docker.reference.digest": "sha256:378c4c5418f7493bd500ad21ffb43818d0689daaad43e3261859fb417d1481a0",
        "vnd.docker.reference.type": "attestation-manifest"
      },
      "digest": "sha256:175cdb0651aaf8b1fe584a0076312b70def5ba29c5750134cacf99396acd89c1",
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "platform": {
        "architecture": "unknown",
        "os": "unknown"
      },
      "size": 838
    },
    {
      "annotations": {
        "com.docker.official-images.bashbrew.arch": "i386",
        "org.opencontainers.image.base.name": "scratch",
        "org.opencontainers.image.created": "2026-04-15T21:33:16Z",
        "org.opencontainers.image.revision": "c68e08480b8fb053591ade7dbaffa2ea67db2f56",
        "org.opencontainers.image.source": "https://github.com/alpinelinux/docker-alpine.git#c68e08480b8fb053591ade7dbaffa2ea67db2f56:x86",
        "org.opencontainers.image.url": "https://hub.docker.com/_/alpine",
        "org.opencontainers.image.version": "3.23.4"
      },
      "digest": "sha256:9b9ebaba5ccb78ee301bec0b365d4d014973b05bd77a7bf59cb18f8b160a09c4",
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "platform": {
        "architecture": "386",
        "os": "linux"
      },
      "size": 1018
    },
    {
      "annotations": {
        "com.docker.official-images.bashbrew.arch": "i386",
        "vnd.docker.reference.digest": "sha256:9b9ebaba5ccb78ee301bec0b365d4d014973b05bd77a7bf59cb18f8b160a09c4",
        "vnd.docker.reference.type": "attestation-manifest"
      },
      "digest": "sha256:438f21c09d38bfbdfa7a86c1dac83a8644a0288df524e861abde1365173fc311",
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "platform": {
        "architecture": "unknown",
        "os": "unknown"
      },
      "size": 838
    },
    {
      "annotations": {
        "com.docker.official-images.bashbrew.arch": "ppc64le",
        "org.opencontainers.image.base.name": "scratch",
        "org.opencontainers.image.created": "2026-04-15T20:00:09Z",
        "org.opencontainers.image.revision": "c68e08480b8fb053591ade7dbaffa2ea67db2f56",
        "org.opencontainers.image.source": "https://github.com/alpinelinux/docker-alpine.git#c68e08480b8fb053591ade7dbaffa2ea67db2f56:ppc64le",
        "org.opencontainers.image.url": "https://hub.docker.com/_/alpine",
        "org.opencontainers.image.version": "3.23.4"
      },
      "digest": "sha256:721eb42dc505c68b5a5a4823b9faace5db351f04f688fb95c8be33c61680608d",
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "platform": {
        "architecture": "ppc64le",
        "os": "linux"
      },
      "size": 1025
    },
    {
      "annotations": {
        "com.docker.official-images.bashbrew.arch": "ppc64le",
        "vnd.docker.reference.digest": "sha256:721eb42dc505c68b5a5a4823b9faace5db351f04f688fb95c8be33c61680608d",
        "vnd.docker.reference.type": "attestation-manifest"
      },
      "digest": "sha256:3c3ace829feeaba4f26239185d04226fb450e4755310ea378fce0f93c3d03573",
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "platform": {
        "architecture": "unknown",
        "os": "unknown"
      },
      "size": 838
    },
    {
      "annotations": {
        "com.docker.official-images.bashbrew.arch": "riscv64",
        "org.opencontainers.image.base.name": "scratch",
        "org.opencontainers.image.created": "2026-04-15T20:30:25Z",
        "org.opencontainers.image.revision": "c68e08480b8fb053591ade7dbaffa2ea67db2f56",
        "org.opencontainers.image.source": "https://github.com/alpinelinux/docker-alpine.git#c68e08480b8fb053591ade7dbaffa2ea67db2f56:riscv64",
        "org.opencontainers.image.url": "https://hub.docker.com/_/alpine",
        "org.opencontainers.image.version": "3.23.4"
      },
      "digest": "sha256:667d07bf2f6239f094f64b5682c8ffbe24c9f3139b1fb854f85caf931a3d7439",
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "platform": {
        "architecture": "riscv64",
        "os": "linux"
      },
      "size": 1025
    },
    {
      "annotations": {
        "com.docker.official-images.bashbrew.arch": "riscv64",
        "vnd.docker.reference.digest": "sha256:667d07bf2f6239f094f64b5682c8ffbe24c9f3139b1fb854f85caf931a3d7439",
        "vnd.docker.reference.type": "attestation-manifest"
      },
      "digest": "sha256:580b8f0d67f41fbe7d2c1b13de29ab59c25e7ba3ed920748c4f67de2464253f9",
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "platform": {
        "architecture": "unknown",
        "os": "unknown"
      },
      "size": 838
    },
    {
      "annotations": {
        "com.docker.official-images.bashbrew.arch": "s390x",
        "org.opencontainers.image.base.name": "scratch",
        "org.opencontainers.image.created": "2026-04-15T20:00:18Z",
        "org.opencontainers.image.revision": "c68e08480b8fb053591ade7dbaffa2ea67db2f56",
        "org.opencontainers.image.source": "https://github.com/alpinelinux/docker-alpine.git#c68e08480b8fb053591ade7dbaffa2ea67db2f56:s390x",
        "org.opencontainers.image.url": "https://hub.docker.com/_/alpine",
        "org.opencontainers.image.version": "3.23.4"
      },
      "digest": "sha256:0791b04ae8a9ddcb3d5ffa6740f0b12574a101a086eb747dd78bf6d9063ded87",
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "platform": {
        "architecture": "s390x",
        "os": "linux"
      },
      "size": 1021
    },
    {
      "annotations": {
        "com.docker.official-images.bashbrew.arch": "s390x",
        "vnd.docker.reference.digest": "sha256:0791b04ae8a9ddcb3d5ffa6740f0b12574a101a086eb747dd78bf6d9063ded87",
        "vnd.docker.reference.type": "attestation-manifest"
      },
      "digest": "sha256:920bb87772e86dead9225b2a8ac9cc670005955abe1a0329f5b1ca7be3bcb331",
      "mediaType": "application/vnd.oci.image.manifest.v1+json",
      "platform": {
        "architecture": "unknown",
        "os": "unknown"
      },
      "size": 838
    }
  ],
  "mediaType": "application/vnd.oci.image.index.v1+json",
  "schemaVersion": 2
}"#;

        let _image_index = serde_json::from_str::<ImageIndex>(input).unwrap();
    }

    #[test]
    fn test_parse_image_manifest_json() {
        let input = r#"
        {
  "schemaVersion": 2,
  "mediaType": "application/vnd.oci.image.manifest.v1+json",
  "config": {
    "mediaType": "application/vnd.oci.image.config.v1+json",
    "digest": "sha256:2ffb2ff4aab36d06b7f3266bbb10e8232769cd2360613131d37abd19430cf6f1",
    "size": 627
  },
  "layers": [
    {
      "mediaType": "application/vnd.oci.image.layer.v1.tar+gzip",
      "digest": "sha256:d17f077ada118cc762df373ff803592abf2dfa3ddafaa7381e364dd27a88fca7",
      "size": 4199870
    }
  ],
  "annotations": {
    "com.docker.official-images.bashbrew.arch": "arm64v8",
    "org.opencontainers.image.base.name": "scratch",
    "org.opencontainers.image.created": "2026-04-15T20:01:01Z",
    "org.opencontainers.image.revision": "c68e08480b8fb053591ade7dbaffa2ea67db2f56",
    "org.opencontainers.image.source": "https://github.com/alpinelinux/docker-alpine.git#c68e08480b8fb053591ade7dbaffa2ea67db2f56:aarch64",
    "org.opencontainers.image.url": "https://hub.docker.com/_/alpine",
    "org.opencontainers.image.version": "3.23.4"
  }
}
        "#;
        let _image_manifest = serde_json::from_str::<ImageManifest>(input).unwrap();
    }

    #[test]
    fn test_parse_image_config_json() {
        let input = r#"
{
  "architecture": "arm64",
  "os": "freebsd",
  "rootfs": {
    "diff_ids": [
      "sha256:d17f077ada118cc762df373ff803592abf2dfa3ddafaa7381e364dd27a88fca7",
      "sha256:c6f988f4874bb0add23a778f753c65efe992244e148a1d2ec2a8b664fb66bbd1",
      "sha256:5f70bf18a086007016e948b04aed3b82103a36bea41755b6cddfaf10ace3c6ef"
    ]
  }
}
        "#;
        let _image_config = serde_json::from_str::<ImageConfig>(input).unwrap();
    }
}
