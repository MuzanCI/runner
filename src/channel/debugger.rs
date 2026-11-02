use muzanci_config::config::ImageConfig;
use muzanci_observability::TError;
use sha2::Digest;
use sha2::Sha256;
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use tokio::sync::oneshot;
use tokio::task::AbortHandle;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use url::Url;
use uuid::Uuid;

use muzanci_config::config::DebugSessionId;
use muzanci_config::config::StepConfig;
use muzanci_git::GitBranch;
use muzanci_git::GitClient;
use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::DebugClientMessage;
use muzanci_transport::message::DebuggerMessage;
use muzanci_transport::message::Message;

use crate::app::runner_state::RunnerState;
use crate::assignment_capacity::AssignmentCapacityPermit;
use crate::channel::debugger_tunnel::DebuggerTunnel;
use crate::sandbox::Sandbox;
use crate::sandbox::SandboxConfig;
use crate::sandbox::SandboxId;

#[derive(thiserror::Error, Debug)]
#[error("{0}")]
pub struct DebuggerError(String);

pub struct Debugger {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
    debug_session_id: DebugSessionId,
    sandbox: Option<Arc<dyn Sandbox>>,
    diff_file: Option<NamedTempFile>,
    diff_hasher: Option<Sha256>,
    _permit: AssignmentCapacityPermit,
}

impl Debugger {
    pub fn spawn(
        join_set: &mut JoinSet<Result<(), TError>>,
        runner_state: Arc<RunnerState>,
        killed: CancellationToken,
        debug_session_id: DebugSessionId,
        permit: AssignmentCapacityPermit,
    ) -> AbortHandle {
        join_set.spawn(async move {
            let (channel_tx, channel_rx, channel_closed) = runner_state
                .runner_mux_handle()
                .open_channel(ChannelType::Debugger)
                .await
                .unwrap();

            Debugger {
                runner_state,
                channel_tx,
                channel_rx,
                debug_session_id,
                sandbox: None,
                diff_file: None,
                diff_hasher: None,
                _permit: permit,
            }
            .main(killed, channel_closed)
            .await
        })
    }

    #[tracing::instrument(
        skip_all,
        name = "debugger",
        fields(
            id = Uuid::now_v7().to_string(),
            server_id = self.runner_state.runner_mux_handle().runner_id().to_string(),
        )
    )]
    async fn main(
        self,
        killed: CancellationToken,
        channel_closed: CancellationToken,
    ) -> Result<(), TError> {
        tokio::select! {
            _ = killed.cancelled() => {
                tracing::info!("Killed, stopping Debugger.");
                Ok(())
            }
            _ = channel_closed.cancelled() => {
                tracing::info!("Channel closed, stopping Debugger.");
                Ok(())
            }
            result = self.run() => {
                result
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn run(mut self) -> Result<(), TError> {
        self.start_debug_session().await?;

        // Initialization. Disconnection indicates debug_session stopped.

        self.handle_create_sandbox_request().await?;
        self.handle_checkout_branch_request().await?;
        self.handle_diff_upload().await?;
        self.handle_apply_diff_request().await?;

        // Ready loop. Disconnection does not indicate debug_session stopped.

        loop {
            match self.channel_rx.recv().await {
                Some(message) => {
                    self.handle_message(message).await?;
                }
                None => {
                    tracing::info!("Channel closed, stopping Debugger.");
                    return Ok(());
                }
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn start_debug_session(&mut self) -> Result<(), TError> {
        self.channel_tx
            .send(Message::Debugger(
                DebuggerMessage::StartDebugSessionRequest {
                    runner_id: self.runner_state.runner_mux_handle().runner_id(),
                    debug_session_id: self.debug_session_id,
                },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::Debugger(DebuggerMessage::StartDebugSessionResponse { result }) => {
                    result.map_err(|e| TError::new(&e.to_string()))
                }
                _ => Err(TError::new("Unexpected message type")),
            })
    }

    #[tracing::instrument(skip_all)]
    async fn handle_message(&mut self, message: Message) -> Result<(), TError> {
        let message = match message {
            Message::DebugClient(msg) => msg,
            _ => Err(DebuggerError(format!("unexpected message: {:?}", message)))?,
        };

        match message {
            DebugClientMessage::ApplyDiffRequest => self.handle_apply_diff_request().await,
            DebugClientMessage::StartShellRequest => self.handle_start_shell_request().await,
            _ => Err(DebuggerError(format!("unexpected message: {:?}", message)))?,
        }
    }

    #[tracing::instrument(skip_all)]
    async fn handle_create_sandbox_request(&mut self) -> Result<(), TError> {
        let result = match self.channel_rx.recv().await {
            Some(Message::DebugClient(DebugClientMessage::CreateSandboxRequest { image })) => {
                self.create_sandbox(image).await.map_err(|e| e.to_string())
            }
            Some(message) => {
                let e = DebuggerError(format!("unexpected message: {:?}", message));
                return Err(e.into());
            }
            None => {
                tracing::info!("Channel closed, stopping Debugger.");
                return Ok(());
            }
        };

        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::CreateSandboxResponse { result },
            ))
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn create_sandbox(&mut self, image: ImageConfig) -> Result<(), TError> {
        let sandbox_config = SandboxConfig {
            sandbox_id: SandboxId::now_v7(),
            image,
        };

        let sandbox = self.runner_state.sandboxer().create(sandbox_config).await?;

        tracing::info!(
            "Created sandbox at [{}]",
            sandbox.workspace_path().display()
        );

        self.sandbox = Some(sandbox);

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn handle_checkout_branch_request(&mut self) -> Result<(), TError> {
        let result = match self.channel_rx.recv().await {
            Some(Message::DebugClient(DebugClientMessage::CheckoutBranchRequest {
                url,
                branch,
            })) => self
                .checkout_branch(url, branch)
                .await
                .map_err(|e| e.to_string()),
            Some(message) => {
                let e = DebuggerError(format!("unexpected message: {:?}", message));
                return Err(e.into());
            }
            None => {
                tracing::info!("Channel closed, stopping Debugger.");
                return Ok(());
            }
        };

        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::CheckoutBranchResponse { result },
            ))
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn checkout_branch(&self, url: Url, branch: GitBranch) -> Result<(), TError> {
        let sandbox = self
            .sandbox
            .as_ref()
            .ok_or_else(|| DebuggerError("sandbox is None".to_string()))?;

        {
            let git_client = GitClient::try_default()?;
            tracing::info!(
                "Checking out repo [{}] branch [{}] to [{}]",
                url,
                branch,
                sandbox.workspace_path().display()
            );
            git_client.checkout_branch(&url, &branch, &sandbox.workspace_path())?;
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn handle_diff_upload(&mut self) -> Result<(), TError> {
        let result = match self.channel_rx.recv().await {
            Some(Message::DebugClient(DebugClientMessage::StartDiffUploadRequest)) => {
                self.start_diff_upload().await.map_err(|e| e.to_string())
            }
            Some(message) => {
                let e = DebuggerError(format!("unexpected message: {:?}", message));
                return Err(e.into());
            }
            None => {
                tracing::info!("Channel closed, stopping Debugger.");
                return Ok(());
            }
        };

        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::StartDiffUploadResponse { result },
            ))
            .await?;

        loop {
            match self.channel_rx.recv().await {
                Some(Message::DebugClient(DebugClientMessage::UploadDiffChunkRequest {
                    chunk,
                })) => {
                    let result = self
                        .upload_diff_chunk(chunk)
                        .await
                        .map_err(|e| e.to_string());

                    self.channel_tx
                        .send(Message::DebugClient(
                            DebugClientMessage::UploadDiffChunkResponse { result },
                        ))
                        .await?;
                }
                Some(Message::DebugClient(DebugClientMessage::CompleteDiffUploadRequest {
                    checksum,
                })) => {
                    let result = self
                        .complete_diff_upload(checksum)
                        .await
                        .map_err(|e| e.to_string());

                    self.channel_tx
                        .send(Message::DebugClient(
                            DebugClientMessage::CompleteDiffUploadResponse { result },
                        ))
                        .await?;

                    return Ok(());
                }
                Some(message) => {
                    let e = DebuggerError(format!("unexpected message: {:?}", message));
                    return Err(e.into());
                }
                None => {
                    tracing::info!("Channel closed, stopping Debugger.");
                    return Ok(());
                }
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn start_diff_upload(&mut self) -> Result<(), TError> {
        self.diff_file = Some(NamedTempFile::new()?);
        self.diff_hasher = Some(Sha256::new());
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn upload_diff_chunk(&mut self, chunk: Vec<u8>) -> Result<(), TError> {
        {
            let diff_hasher = self
                .diff_hasher
                .as_mut()
                .ok_or_else(|| TError::new("diff upload has not been started"))?;
            diff_hasher.update(&chunk);
        }

        {
            let diff_file = self
                .diff_file
                .as_ref()
                .ok_or_else(|| TError::new("diff upload has not been started"))?;

            let file = diff_file.reopen()?;
            tokio::fs::File::from_std(file).write_all(&chunk).await?;
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn complete_diff_upload(&mut self, checksum: String) -> Result<(), TError> {
        let diff_hasher = self
            .diff_hasher
            .as_mut()
            .ok_or_else(|| TError::new("diff upload has not been started"))?;
        let digest = hex::encode(diff_hasher.finalize_reset());
        tracing::info!("Diff digest [{}]", digest);
        if digest != checksum {
            return Err(TError::new("diff file checksum mismatch"));
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn handle_apply_diff_request(&mut self) -> Result<(), TError> {
        let result = match self.channel_rx.recv().await {
            Some(Message::DebugClient(DebugClientMessage::ApplyDiffRequest)) => {
                self.apply_diff().await.map_err(|e| e.to_string())
            }
            Some(message) => {
                let e = DebuggerError(format!("unexpected message: {:?}", message));
                return Err(e.into());
            }
            None => {
                tracing::info!("Channel closed, stopping Debugger.");
                return Ok(());
            }
        };

        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::ApplyDiffResponse { result },
            ))
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn apply_diff(&mut self) -> Result<(), TError> {
        let sandbox = self
            .sandbox
            .as_ref()
            .ok_or_else(|| TError::new("sandbox not available"))?;

        let diff_file = self
            .diff_file
            .as_ref()
            .ok_or_else(|| TError::new("diff file not available"))?;

        {
            let git_client = GitClient::try_default()?;
            git_client.apply_diff(diff_file.path(), &sandbox.workspace_path())?;
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn handle_start_shell_request(&self) -> Result<(), TError> {
        let result = self.start_shell().await.map_err(|e| e.to_string());

        self.channel_tx
            .send(Message::DebugClient(
                DebugClientMessage::StartShellResponse { result },
            ))
            .await?;

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn start_shell(&self) -> Result<(), TError> {
        let (reply_tx, reply_rx) = oneshot::channel();

        DebuggerTunnel::spawn(self.runner_state.clone(), self.debug_session_id, reply_tx);

        let () = reply_rx.await?;

        Ok(())
    }
}
