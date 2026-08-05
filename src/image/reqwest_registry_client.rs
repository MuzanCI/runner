use futures::TryStreamExt;

use crate::image::blob_ref::BlobRef;
use crate::image::image::ImageIndex;
use crate::image::image::ImageManifest;
use crate::image::image::MediaType;
use crate::image::manifest_ref::ManifestRef;
use crate::image::registry_client::RegistryClient;
use crate::image::registry_client::RegistryClientError;

pub struct ReqwestRegistryClient;

impl ReqwestRegistryClient {
    pub fn new() -> Self {
        ReqwestRegistryClient
    }
}

#[async_trait::async_trait]
impl RegistryClient for ReqwestRegistryClient {
    #[tracing::instrument(skip_all)]
    async fn resolve_image_manifest(
        &self,
        manifest_ref: &ManifestRef,
    ) -> Result<ImageManifest, RegistryClientError> {
        tracing::info!("Resolving image manifest for '{:?}'", manifest_ref);
        let auth_token = self.auth_token(&manifest_ref.namespace).await?;
        let url = manifest_ref
            .to_url()
            .map_err(|e| RegistryClientError(e.to_string()))?;
        let client = reqwest::Client::new();
        let res = client
            .get(url.clone())
            .header(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", auth_token),
            )
            .send()
            .await
            .map_err(|e| RegistryClientError(e.to_string()))?;

        if !res.status().is_success() {
            let e = format!(
                "Failed to fetch manifest from registry: '{}', status: {}",
                url,
                res.status()
            );
            tracing::error!("{e}");
            return Err(RegistryClientError(e));
        }

        let content_type = res
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .ok_or_else(|| {
                RegistryClientError(format!(
                    "HTTP response from '{}' is missing Content-Type header",
                    url
                ))
            })?
            .to_str()
            .map_err(|e| RegistryClientError(e.to_string()))?;

        tracing::info!(
            "Fetched manifest from '{}', Content-Type: '{}'",
            url,
            content_type,
        );

        let media_type = content_type
            .parse::<MediaType>()
            .map_err(|e| RegistryClientError(e.to_string()))?;

        match media_type {
            MediaType::OciImageIndexV1Json => {
                tracing::info!(
                    "Received image index for '{}', looking for linux/arm64 manifest",
                    url
                );
                // Parse response body as JSON into ImageIndex struct.
                let image_index = res
                    .json::<ImageIndex>()
                    .await
                    .map_err(|e| RegistryClientError(e.to_string()))?;

                // Find manifest descriptor matching "linux/arm64".
                let manifest_descriptor = image_index
                    .manifests
                    .ok_or_else(|| {
                        RegistryClientError(format!(
                            "Image index from '{}' does not contain any manifests",
                            url
                        ))
                    })?
                    .into_iter()
                    .find(|d| d.platform.architecture == "arm64" && d.platform.os == "freebsd")
                    .ok_or_else(|| {
                        RegistryClientError(format!(
                            "No manifest found for platform 'linux/arm64' in image index from '{}'",
                            url
                        ))
                    })?;

                let digest = manifest_descriptor.digest.clone();

                tracing::info!(
                    "Found manifest for 'linux/arm64' in image index from '{}', digest: '{:?}'",
                    url,
                    digest,
                );

                // Fetch the image manifest.
                let manifest_ref = ManifestRef {
                    domain: manifest_ref.domain.clone(),
                    namespace: manifest_ref.namespace.clone(),
                    digest: Some(digest),
                    tag: None,
                };
                self.resolve_image_manifest(&manifest_ref).await
            }
            MediaType::OciImageManifestV1Json => {
                tracing::info!("Received image manifest for '{}'", url);
                // Perform GET request to fetch manifest, and parse JSON into ImageManifest struct.
                let image_manifest = res
                    .json::<ImageManifest>()
                    .await
                    .map_err(|e| RegistryClientError(e.to_string()))?;
                Ok(image_manifest)
            }
            media_type => {
                let e = format!(
                    "Unsupported media type '{:?}' for manifest from '{}'",
                    media_type, url
                );
                tracing::error!("{e}");
                Err(RegistryClientError(e))
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn blob_reader(
        &self,
        blob_ref: &BlobRef,
        offset: usize,
    ) -> Result<Box<dyn tokio::io::AsyncRead + Unpin + Send>, RegistryClientError> {
        let auth_token = self.auth_token(&blob_ref.namespace).await?;
        tracing::info!(
            "Reading blob from registry: '{}', offset: {}",
            blob_ref.to_url(),
            offset,
        );
        // Perform GET request with Range header to read blob from offset.
        let url = blob_ref.to_url();
        let client = reqwest::Client::new();
        let res = client
            .get(url.clone())
            .header(reqwest::header::RANGE, format!("bytes={}-", offset))
            .bearer_auth(auth_token)
            .send()
            .await
            .map_err(|e| RegistryClientError(e.to_string()))?;
        if !res.status().is_success() && res.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            let e = format!(
                "Failed to read blob from registry: '{}', status: {}",
                url,
                res.status()
            );
            tracing::error!("{e}");
            return Err(RegistryClientError(e));
        }
        let stream = res
            .bytes_stream()
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e));
        let reader = tokio_util::io::StreamReader::new(stream);
        Ok(Box::new(reader))
    }

    /// Example: https://auth.docker.io/token?service=registry.docker.io&scope=repository:library/alpine:pull,push
    #[tracing::instrument(skip_all)]
    async fn auth_token(&self, namespace: &str) -> Result<String, RegistryClientError> {
        let url = format!(
            "https://auth.docker.io/token?service=registry.docker.io&scope=repository:{}:pull",
            namespace
        );
        let client = reqwest::Client::new();
        let res = client
            .get(&url)
            .send()
            .await
            .map_err(|e| RegistryClientError(e.to_string()))?;
        if !res.status().is_success() {
            let e = format!("Failed to fetch auth token from '{}'", url);
            return Err(RegistryClientError(e));
        }
        let json: serde_json::Value = res
            .json()
            .await
            .map_err(|e| RegistryClientError(e.to_string()))?;
        let token = json.get("token").and_then(|v| v.as_str()).ok_or_else(|| {
            RegistryClientError(format!(
                "Auth token response from '{}' is missing 'token' field",
                url
            ))
        })?;
        Ok(token.to_string())
    }
}
