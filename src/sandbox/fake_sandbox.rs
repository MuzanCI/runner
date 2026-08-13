use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitStatus;

use futures_util::StreamExt;
use tempfile::TempDir;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_util::codec::FramedRead;
use tokio_util::codec::LinesCodec;

use muzanci_transport::message::ProcessOutput;

use crate::sandbox::Sandbox;
use crate::sandbox::SandboxConfig;
use crate::sandbox::SandboxError;

pub struct FakeSandbox {
    temp_dir: TempDir,
    config: SandboxConfig,
}

impl FakeSandbox {
    pub fn new(config: SandboxConfig) -> Self {
        let temp_dir = TempDir::new().unwrap();
        Self { temp_dir, config }
    }
}

#[async_trait::async_trait]
impl Sandbox for FakeSandbox {
    #[tracing::instrument(skip_all)]
    async fn run(
        &self,
        cmd_str: &str,
        envs: &HashMap<String, String>,
        output_tx: mpsc::Sender<ProcessOutput>,
    ) -> Result<ExitStatus, SandboxError> {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(cmd_str)
            .envs(envs)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .map_err(|e| SandboxError(e.to_string()))?;

        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| SandboxError("failed to take stdout".to_string()))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| SandboxError("failed to take stderr".to_string()))?;

        let mut stdout_lines = FramedRead::new(stdout, LinesCodec::new());
        let mut stderr_lines = FramedRead::new(stderr, LinesCodec::new());

        let stdout_tx = output_tx.clone();
        let stdout_handle = tokio::spawn(async move {
            let mut index = 0;
            while let Some(result) = stdout_lines.next().await {
                match result {
                    Ok(line) => {
                        stdout_tx
                            .send(ProcessOutput::Stdout { index, line })
                            .await
                            .unwrap();
                        index += 1;
                    }
                    Err(e) => {
                        tracing::error!("failed to read stdout: {}", e);
                        break;
                    }
                }
            }
        });

        let stderr_tx = output_tx;
        let stderr_handle = tokio::spawn(async move {
            let mut index = 0;
            while let Some(result) = stderr_lines.next().await {
                match result {
                    Ok(line) => {
                        stderr_tx
                            .send(ProcessOutput::Stderr { index, line })
                            .await
                            .unwrap();
                        index += 1;
                    }
                    Err(e) => {
                        tracing::error!("failed to read stderr: {}", e);
                        break;
                    }
                }
            }
        });

        let _ = tokio::join!(stdout_handle, stderr_handle);
        let exit_status = child
            .wait()
            .await
            .map_err(|e| SandboxError(e.to_string()))?;

        Ok(exit_status)
    }

    fn workspace_path(&self) -> PathBuf {
        self.temp_dir.path().to_path_buf()
    }

    fn config(&self) -> &SandboxConfig {
        &self.config
    }
}
