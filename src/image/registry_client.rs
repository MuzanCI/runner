use crate::image::blob_ref::BlobRef;
use crate::image::image::ImageManifest;
use crate::image::manifest_ref::ManifestRef;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct RegistryClientError(pub String);

#[async_trait::async_trait]
pub trait RegistryClient
where
    Self: Send + Sync,
{
    async fn resolve_image_manifest(
        &self,
        manifest_ref: &ManifestRef,
    ) -> Result<ImageManifest, RegistryClientError>;
    async fn blob_reader(
        &self,
        blob_ref: &BlobRef,
        offset: usize,
    ) -> Result<Box<dyn tokio::io::AsyncRead + Unpin + Send>, RegistryClientError>;
    async fn auth_token(&self, path: &str) -> Result<String, RegistryClientError>;
}
