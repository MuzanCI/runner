use std::collections::HashMap;
use std::path::Path;
use std::process::ExitStatus;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc;

use muzanci_transport::channel::ProcessOutput;

use muzanci_image::image::ImagePlatform;
use muzanci_image::manifest_ref::ManifestRef;

pub mod jail_config;
pub mod jail_sandbox;
pub mod jail_sandboxer;
pub mod jail_slot;
pub mod zfs_image_store;

pub type NetworkInterface = String;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SandboxError(pub String);

pub struct SandboxConfig {
    pub sandbox_id: SandboxId,
    pub manifest_ref: ManifestRef,
    pub platform: ImagePlatform,
}

#[async_trait::async_trait]
pub trait Sandbox
where
    Self: Send + Sync,
{
    async fn run(
        &self,
        cmd_str: &str,
        envs: &HashMap<String, String>,
        output_tx: mpsc::Sender<ProcessOutput>,
    ) -> Result<ExitStatus, SandboxError>;

    async fn create_executable_file(&self, path: &Path, content: Bytes)
    -> Result<(), SandboxError>;

    async fn read_file(&self, path: &Path) -> Result<String, SandboxError>;

    fn config(&self) -> &SandboxConfig;
}

pub type SandboxId = uuid::Uuid;

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
pub struct SandboxerError(pub String);

#[async_trait::async_trait]
pub trait Sandboxer
where
    Self: Send + Sync,
{
    async fn create(&self, config: SandboxConfig) -> Result<Arc<dyn Sandbox>, SandboxerError>;
    fn destroy(&self, sandbox: Arc<dyn Sandbox>) -> Result<(), SandboxerError>;
}
