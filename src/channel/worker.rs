use muzanci_observability::TError;
use std::collections::HashMap;
use std::os::unix::process::ExitStatusExt;
use std::sync::Arc;
use tokio::join;
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tokio::task::JoinHandle;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use muzanci_config::config::StepConfig;
use muzanci_config::config::StepIndex;
use muzanci_git::GitClient;
use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::ExitStatus;
use muzanci_transport::message::Message;
use muzanci_transport::message::ProcessOutput;
use muzanci_transport::message::TaskConfig;
use muzanci_transport::message::TaskId;
use muzanci_transport::message::WorkerMessage;

use crate::app::runner_state::RunnerState;
use crate::assignment_capacity::AssignmentCapacityPermit;
use crate::sandbox::Sandbox;
use crate::sandbox::SandboxConfig;
use crate::sandbox::SandboxId;

pub struct Worker {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    task_id: TaskId,
    _permit: AssignmentCapacityPermit,
}

enum ItemResult {
    Continue,
    Halt,
}

impl Worker {
    pub fn spawn(
        join_set: &mut JoinSet<Result<(), TError>>,
        runner_state: Arc<RunnerState>,
        killed: CancellationToken,
        task_id: TaskId,
        permit: AssignmentCapacityPermit,
    ) -> AbortHandle {
        join_set.spawn(async move {
            let (channel_tx, channel_rx, channel_closed) = runner_state
                .runner_mux_handle()
                .open_channel(ChannelType::Worker)
                .await?;

            Worker {
                runner_state,
                channel_tx,
                task_id,
                _permit: permit,
            }
            .main(killed, channel_closed, channel_rx)
            .await
        })
    }

    #[tracing::instrument(
        skip_all,
        name = "worker",
        fields(
            id = Uuid::now_v7().to_string(),
            server_id = self.runner_state.runner_mux_handle().runner_id().to_string(),
        )
    )]
    async fn main(
        self,
        killed: CancellationToken,
        channel_closed: CancellationToken,
        channel_rx: ChannelReceiver,
    ) -> Result<(), TError> {
        tokio::select! {
            _ = killed.cancelled() => {
                tracing::info!("Killed, stopping Worker.");
                Ok(())
            }
            _ = channel_closed.cancelled() => {
                tracing::info!("Channel closed, stopping Worker.");
                Ok(())
            }
            result = self.run(channel_rx) => {
                result
            }
        }
    }

    async fn run(self, mut channel_rx: ChannelReceiver) -> Result<(), TError> {
        let task_config = self.start_task(&mut channel_rx).await?;

        let sandbox = {
            let sandbox_config = SandboxConfig {
                sandbox_id: SandboxId::now_v7(),
                image: task_config.image.clone(),
            };
            self.runner_state.sandboxer().create(sandbox_config).await?
        };

        {
            let git_client = GitClient::try_default()?;
            git_client.checkout_commit(
                &task_config.remote.url,
                &task_config.remote.branch,
                &sandbox.workspace_path(),
                &task_config.commit_sha,
            )?;
        }

        for (step_index, step) in task_config.steps.into_iter().enumerate() {
            let item_result = self
                .run_item(
                    &mut channel_rx,
                    sandbox.clone(),
                    (step_index as i64).into(),
                    step,
                )
                .await?;

            match item_result {
                ItemResult::Continue => {
                    continue;
                }
                ItemResult::Halt => {
                    self.fail_task(&mut channel_rx).await?;
                    // The step failed so we stop, but the worker itself
                    // is considered successful.
                    return Ok(());
                }
            };
        }

        self.complete_task(&mut channel_rx).await
    }

    async fn start_task(&self, channel_rx: &mut ChannelReceiver) -> Result<TaskConfig, TError> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::StartTaskRequest {
                runner_id: self.runner_state.runner_mux_handle().runner_id(),
                task_id: self.task_id,
            }))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::StartTaskResponse { result }) => {
                    result.map_err(|e| TError::new(&e.to_string()))
                }
                _ => Err(TError::new("Unexpected message type")),
            })
    }

    async fn run_item(
        &self,
        channel_rx: &mut ChannelReceiver,
        sandbox: Arc<dyn Sandbox>,
        step_index: StepIndex,
        step: StepConfig,
    ) -> Result<ItemResult, TError> {
        self.start_item(channel_rx, step_index).await?;

        let envs = {
            let mut envs = HashMap::new();
            // TODO: Populate envs from step config.
            envs
        };

        let exit_status = {
            let (output_tx, output_rx) = mpsc::channel(1);
            let output_handle = WorkerItemOutput::spawn(
                self.runner_state.clone(),
                self.channel_tx.clone(),
                self.task_id,
                step_index,
                output_rx,
            );
            let process_handle = sandbox.run(&step.command, &envs, output_tx);
            let (process_result, _output_result) = join!(process_handle, output_handle);

            let exit_status = process_result?;
            match (exit_status.code(), exit_status.signal()) {
                (Some(code), None) => ExitStatus::Code(code),
                (None, Some(signal)) => ExitStatus::Signal(signal),
                (code, signal) => {
                    return Err(TError::new(&format!(
                        "Unexpected exit status: code={code:?} signal={signal:?}"
                    )));
                }
            }
        };

        self.channel_tx
            .send(Message::Worker(WorkerMessage::ItemProcessExitStatus {
                runner_id: self.runner_state.runner_mux_handle().runner_id(),
                task_id: self.task_id,
                step_index,
                exit_status,
            }))
            .await?;

        match exit_status {
            ExitStatus::Code(0) => {
                self.complete_item(channel_rx, step_index).await?;
                Ok(ItemResult::Continue)
            }
            ExitStatus::Code(non_zero_code) => {
                let e = format!("Process exited with non-zero status code [{non_zero_code}]");
                self.fail_item(channel_rx, step_index, e.clone()).await?;
                Ok(ItemResult::Halt)
            }
            ExitStatus::Signal(signal) => {
                let e = format!("Process terminated by signal [{signal}]");
                self.fail_item(channel_rx, step_index, e.clone()).await?;
                Ok(ItemResult::Halt)
            }
        }
    }

    async fn complete_task(&self, channel_rx: &mut ChannelReceiver) -> Result<(), TError> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::CompleteTaskRequest {
                runner_id: self.runner_state.runner_mux_handle().runner_id(),
                task_id: self.task_id,
            }))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::CompleteTaskResponse { result }) => {
                    result.map_err(|e| TError::new(&e.to_string()))
                }
                _ => Err(TError::new("Unexpected message type")),
            })
    }

    async fn fail_task(&self, channel_rx: &mut ChannelReceiver) -> Result<(), TError> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::FailTaskRequest {
                runner_id: self.runner_state.runner_mux_handle().runner_id(),
                task_id: self.task_id,
            }))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::FailTaskResponse { result }) => {
                    result.map_err(|e| TError::new(&e.to_string()))
                }
                _ => Err(TError::new("Unexpected message type")),
            })
    }

    async fn start_item(
        &self,
        channel_rx: &mut ChannelReceiver,
        step_index: StepIndex,
    ) -> Result<(), TError> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::StartItemRequest {
                runner_id: self.runner_state.runner_mux_handle().runner_id(),
                task_id: self.task_id,
                step_index,
            }))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::StartItemResponse { result }) => {
                    result.map_err(|e| TError::new(&e.to_string()))
                }
                _ => Err(TError::new("Unexpected message type")),
            })
    }

    async fn complete_item(
        &self,
        channel_rx: &mut ChannelReceiver,
        step_index: StepIndex,
    ) -> Result<(), TError> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::CompleteItemRequest {
                runner_id: self.runner_state.runner_mux_handle().runner_id(),
                task_id: self.task_id,
                step_index,
            }))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::CompleteItemResponse { result }) => {
                    result.map_err(|e| TError::new(&e.to_string()))
                }
                _ => Err(TError::new("Unexpected message type")),
            })
    }

    async fn fail_item(
        &self,
        channel_rx: &mut ChannelReceiver,
        step_index: StepIndex,
        reason: String,
    ) -> Result<(), TError> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::FailItemRequest {
                runner_id: self.runner_state.runner_mux_handle().runner_id(),
                task_id: self.task_id,
                step_index,
                reason,
            }))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::FailItemResponse { result }) => {
                    result.map_err(|e| TError::new(&e.to_string()))
                }
                _ => Err(TError::new("Unexpected message type")),
            })
    }
}

pub struct WorkerItemOutput {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    task_id: TaskId,
    step_index: StepIndex,
    output_rx: mpsc::Receiver<ProcessOutput>,
}

impl WorkerItemOutput {
    pub fn spawn(
        runner_state: Arc<RunnerState>,
        channel_tx: ChannelSender,
        task_id: TaskId,
        step_index: StepIndex,
        output_rx: mpsc::Receiver<ProcessOutput>,
    ) -> JoinHandle<Result<(), TError>> {
        tokio::spawn({
            let runner_state = runner_state.clone();
            async move {
                WorkerItemOutput {
                    runner_state,
                    channel_tx,
                    task_id,
                    step_index,
                    output_rx,
                }
                .run()
                .await
            }
        })
    }

    async fn run(&mut self) -> Result<(), TError> {
        let runner_shutdown = self.runner_state.runner_shutdown();
        tokio::select! {
            _ = runner_shutdown.cancelled() => {
                eprintln!("WorkerStepOutput received cancellation signal.");
                Ok(())
            }

            result = self.main() => {
                result
            }
        }
    }

    async fn main(&mut self) -> Result<(), TError> {
        while let Some(output) = self.output_rx.recv().await {
            let result = self
                .channel_tx
                .send(Message::Worker(WorkerMessage::ItemProcessOutput {
                    runner_id: self.runner_state.runner_mux_handle().runner_id(),
                    task_id: self.task_id,
                    step_index: self.step_index,
                    output,
                }))
                .await;

            if let Err(e) = result {
                tracing::error!("Failed to send worker step process output: {}", e);
                return Err(TError::new(&format!(
                    "Failed to send worker step process output: {e}",
                )));
            }
        }
        Ok(())
    }
}
