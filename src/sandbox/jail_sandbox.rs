use std::collections::HashMap;
use std::path::Path;
use std::process::ExitStatus;

use bytes::Bytes;
use futures_util::StreamExt;
use muzanci_transport::channel::ProcessOutput;
use std::os::unix::fs::PermissionsExt;
use tokio::process::Command;
use tokio::sync::mpsc;
use tokio_util::codec::FramedRead;
use tokio_util::codec::LinesCodec;

use crate::sandbox::Sandbox;
use crate::sandbox::SandboxConfig;
use crate::sandbox::SandboxError;
use crate::sandbox::jail_config::JailConfig;
use crate::sandbox::jail_slot::JailSlot;

pub struct JailSandbox {
    config: SandboxConfig,
    jail_conf: JailConfig,

    /// When dropped, the jail slot is automatically restored to [`FreeJailSlots`](crate::jail::jail_slot::FreeJailSlots).
    _slot: JailSlot,
}

impl JailSandbox {
    pub fn new(config: SandboxConfig, jail_conf: JailConfig, slot: JailSlot) -> Self {
        JailSandbox {
            config,
            jail_conf,
            _slot: slot,
        }
    }
}

#[async_trait::async_trait]
impl Sandbox for JailSandbox {
    fn config(&self) -> &SandboxConfig {
        &self.config
    }

    async fn run(
        &self,
        cmd_str: &str,
        envs: HashMap<String, String>,
        output_tx: mpsc::Sender<ProcessOutput>,
    ) -> Result<ExitStatus, SandboxError> {
        let mut child = Command::new("sh")
            .arg("-c")
            .arg(format!("jexec {} {}", self.jail_conf.name(), cmd_str))
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

    async fn create_executable_file(
        &self,
        path: &Path,
        content: Bytes,
    ) -> Result<(), SandboxError> {
        let path = self.jail_conf.path().join(path);
        tokio::fs::write(&path, content)
            .await
            .map_err(|e| SandboxError(e.to_string()))?;

        {
            let mut permissions = tokio::fs::metadata(&path)
                .await
                .map_err(|e| SandboxError(e.to_string()))?
                .permissions();
            permissions.set_mode(0o700);
            tokio::fs::set_permissions(&path, permissions)
                .await
                .map_err(|e| SandboxError(e.to_string()))?;
        }
        Ok(())
    }

    async fn read_file(&self, path: &Path) -> Result<String, SandboxError> {
        let path = self.jail_conf.path().join(path);
        let content = tokio::fs::read_to_string(path)
            .await
            .map_err(|e| SandboxError(e.to_string()))?;
        Ok(content)
    }
}
