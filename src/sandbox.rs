use std::{
    path::{Path, PathBuf},
    process::ExitStatus,
    sync::Arc,
};

use futures::StreamExt;
use muzanci_interpreter::Secret;
use tokio::{process::Command, sync::mpsc};
use tokio_util::codec::{FramedRead, LinesCodec};

use crate::secret::SecretService;

#[async_trait::async_trait]
pub trait Sandboxer
where
    Self: Send + Sync,
{
    fn create(&self) -> anyhow::Result<Arc<dyn Sandbox>>;
}

pub struct FakeSandboxer {
    secret_service: Arc<SecretService>,
}

impl FakeSandboxer {
    pub fn new(secret_service: Arc<SecretService>) -> Self {
        Self { secret_service }
    }
}

impl Sandboxer for FakeSandboxer {
    fn create(&self) -> anyhow::Result<Arc<dyn Sandbox>> {
        let sandbox_id = uuid::Uuid::now_v7();
        let sandbox_path = PathBuf::from(format!("./sandboxes/{}", sandbox_id));
        std::fs::create_dir_all(&sandbox_path)?;

        Ok(Arc::new(FakeSandbox {
            sandbox_id,
            secret_service: self.secret_service.clone(),
            root_path: sandbox_path,
        }))
    }
}

pub type SandboxId = uuid::Uuid;

pub enum Output {
    Stdout(String),
    Stderr(String),
}

#[async_trait::async_trait]
pub trait Sandbox
where
    Self: Send + Sync,
{
    async fn run(
        &self,
        command: &str,
        secrets: Vec<Secret>,
        output_tx: mpsc::Sender<Output>,
    ) -> anyhow::Result<ExitStatus>;

    async fn create_executable_file(&self, path: &Path, content: &[u8]) -> anyhow::Result<()>;
    async fn read_file(&self, path: &Path) -> anyhow::Result<String>;
}
pub struct FakeSandbox {
    sandbox_id: SandboxId,
    secret_service: Arc<SecretService>,
    root_path: PathBuf,
}

#[async_trait::async_trait]
impl Sandbox for FakeSandbox {
    async fn run(
        &self,
        command: &str,
        secrets: Vec<Secret>,
        output_tx: mpsc::Sender<Output>,
    ) -> anyhow::Result<ExitStatus> {
        let envs = {
            let mut envs = vec![];
            for secret in secrets {
                match self.secret_service.resolve(&secret).await {
                    Ok(value) => {
                        envs.push((secret.key.clone(), value));
                    }
                    Err(e) => {
                        tracing::warn!("Unable to resolve secret with key [{}]: {}", secret.key, e);
                        tracing::warn!("Skipping secret with key [{}]: {}", secret.key, e);
                    }
                }
            }
            envs
        };

        let mut child = Command::new("sh")
            .arg("-c")
            .arg(command)
            .current_dir(self.root_path.clone())
            .envs(envs)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;

        let stdout = child.stdout.take().expect("Failed to capture stdout");
        let stderr = child.stderr.take().expect("Failed to capture stderr");

        let mut stdout_lines = FramedRead::new(stdout, LinesCodec::new());
        let mut stderr_lines = FramedRead::new(stderr, LinesCodec::new());

        let stdout_tx = output_tx.clone();
        let stdout_handle = tokio::spawn(async move {
            while let Some(result) = stdout_lines.next().await {
                match result {
                    Ok(line) => {
                        stdout_tx.send(Output::Stdout(line)).await.unwrap();
                    }
                    Err(e) => {
                        eprintln!("Error reading stdout: {}", e);
                        break;
                    }
                }
            }
        });

        let stderr_tx = output_tx;
        let stderr_handle = tokio::spawn(async move {
            while let Some(result) = stderr_lines.next().await {
                match result {
                    Ok(line) => {
                        stderr_tx.send(Output::Stderr(line)).await.unwrap();
                    }
                    Err(e) => {
                        eprintln!("Error reading stderr: {}", e);
                        break;
                    }
                }
            }
        });

        let _ = tokio::join!(stdout_handle, stderr_handle);
        let exit_status = child.wait().await?;

        Ok(exit_status)
    }

    async fn create_executable_file(&self, path: &Path, content: &[u8]) -> anyhow::Result<()> {
        let path = self.root_path.join(path);
        tokio::fs::write(&path, content).await?;
        // set permissions to read and execute for owner only
        use std::os::unix::fs::PermissionsExt;
        let mut permissions = tokio::fs::metadata(&path).await?.permissions();
        permissions.set_mode(0o700);
        tokio::fs::set_permissions(&path, permissions).await?;
        Ok(())
    }

    async fn read_file(&self, path: &Path) -> anyhow::Result<String> {
        let path = self.root_path.join(path);
        let content = tokio::fs::read_to_string(path).await?;
        Ok(content)
    }
}
