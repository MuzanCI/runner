use muzanci_git::GitBranch;
use muzanci_transport::message::DebugClientMessage;
use muzanci_transport::message::DebugConfig;
use muzanci_transport::message::DebugId;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::join;
use tokio::sync::mpsc;
use tracing::instrument;
use url::Url;

use muzanci_git::GitClient;
use muzanci_image::image::ImagePlatform;
use muzanci_image::manifest_ref::ManifestRef;
use muzanci_interpreter::StepConfig;
use muzanci_interpreter::StepId;
use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::DebuggerMessage;
use muzanci_transport::message::ExitStatus;
use muzanci_transport::message::Message;
use muzanci_transport::message::ProcessOutput;
use muzanci_transport::message::TaskConfig;
use muzanci_transport::message::TaskId;

use crate::RunnerState;
use crate::assignment_capacity::AssignmentCapacityPermit;
use crate::sandbox::Sandbox;
use crate::sandbox::SandboxConfig;
use crate::sandbox::SandboxId;

#[derive(thiserror::Error, Debug)]
#[error("{0}")]
pub struct DebuggerError(String);

pub struct DebuggerHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for DebuggerHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct Debugger {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
    debug_id: DebugId,
    manifest_ref: ManifestRef,
    platform: ImagePlatform,
    sandbox: Option<Arc<dyn Sandbox>>,
    _permit: AssignmentCapacityPermit,
}

impl Debugger {
    pub fn spawn(
        runner_state: Arc<RunnerState>,
        debug_id: DebugId,
        manifest_ref: ManifestRef,
        platform: ImagePlatform,
        permit: AssignmentCapacityPermit,
    ) -> DebuggerHandle {
        let runner_state = runner_state.clone();
        let handle = tokio::spawn(async move {
            let (channel_tx, channel_rx) = runner_state
                .mux_handle
                .open_channel(ChannelType::Debugger)
                .await
                .unwrap();
            Debugger {
                runner_state,
                channel_tx,
                channel_rx,
                debug_id,
                manifest_ref,
                platform,
                sandbox: None,
                _permit: permit,
            }
            .run()
            .await
            .unwrap();
        });
        DebuggerHandle { handle }
    }

    #[instrument(skip_all)]
    async fn run(&mut self) -> anyhow::Result<()> {
        let cancellation_token = self.runner_state.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                eprintln!("Debugger received cancellation signal.");
                Ok(())
            }

            result = self.main() => {
                result
            }
        }
    }

    #[instrument(skip_all)]
    async fn main(&mut self) -> anyhow::Result<()> {
        self.connect_debug_client().await?;
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

    async fn handle_message(&mut self, message: Message) -> anyhow::Result<()> {
        let message = match message {
            Message::DebugClient(msg) => msg,
            _ => Err(DebuggerError(format!("unexpected message: {:?}", message)))?,
        };

        match message {
            DebugClientMessage::CreateSandboxRequest => self.handle_create_sandbox_request().await,
            DebugClientMessage::CheckoutBranchRequest { url, branch } => {
                self.handle_checkout_branch_request(url, branch).await
            }
            DebugClientMessage::StartDiffUploadRequest => {
                self.handle_start_diff_upload_request().await
            }
            DebugClientMessage::UploadDiffChunkRequest => {
                self.handle_upload_diff_chunk_request().await
            }
            DebugClientMessage::CompleteDiffUploadRequest => {
                self.handle_complete_diff_upload_request().await
            }
            DebugClientMessage::ApplyDiffRequest => self.handle_apply_diff_request().await,
            DebugClientMessage::StartShellRequest => self.handle_start_shell_request().await,
            DebugClientMessage::ExecuteStepRequest { step } => {
                self.handle_execute_step_request(step).await
            }
            _ => Err(DebuggerError(format!("unexpected message: {:?}", message)))?,
        }
    }

    async fn handle_create_sandbox_request(&mut self) -> anyhow::Result<()> {
        let sandbox_config = SandboxConfig {
            sandbox_id: SandboxId::now_v7(),
            manifest_ref: self.manifest_ref.clone(),
            platform: self.platform.clone(),
        };
        let sandbox = self.runner_state.sandboxer.create(sandbox_config).await?;
        self.sandbox = Some(sandbox);
        Ok(())
    }

    async fn handle_checkout_branch_request(
        &mut self,
        url: Url,
        branch: GitBranch,
    ) -> anyhow::Result<()> {
        let sandbox = self
            .sandbox
            .as_ref()
            .ok_or_else(|| DebuggerError(format!("sandbox is None")))?;
        let git_client = GitClient::try_default()?;
        git_client.checkout_branch(&url, &branch, &sandbox.workspace_path())?;
        Ok(())
    }

    async fn connect_debug_client(&mut self) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Debugger(
                DebuggerMessage::ConnectDebugClientRequest {
                    debug_id: self.debug_id,
                },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Debugger(DebuggerMessage::ConnectDebugClientResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn handle_start_diff_upload_request(&mut self) -> anyhow::Result<()> {
        unimplemented!();
    }

    async fn handle_upload_diff_chunk_request(&mut self) -> anyhow::Result<()> {
        unimplemented!();
    }

    async fn handle_complete_diff_upload_request(&mut self) -> anyhow::Result<()> {
        unimplemented!();
    }

    async fn handle_apply_diff_request(&mut self) -> anyhow::Result<()> {
        unimplemented!();
    }

    async fn handle_start_shell_request(&mut self) -> anyhow::Result<()> {
        unimplemented!();
    }

    async fn handle_execute_step_request(&mut self, step: StepConfig) -> anyhow::Result<()> {
        unimplemented!();
    }
}
