use std::sync::Arc;

use muzanci_interpreter::{Step, StepId};
use muzanci_transport::channel::{
    ChannelReceiver, ChannelSender, ChannelType, Message, RepoUrl, TaskId, WorkerMessage,
};

use crate::{RunnerState, sandbox::Sandbox};

pub struct WorkerHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for WorkerHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct Worker {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
    task_id: TaskId,
}

impl Worker {
    pub fn spawn(runner_state: Arc<RunnerState>, task_id: TaskId) -> WorkerHandle {
        let runner_state = runner_state.clone();
        let handle = tokio::spawn(async move {
            let (channel_tx, channel_rx) = runner_state
                .mux_handle
                .open_channel(ChannelType::Worker)
                .await
                .unwrap();
            Worker {
                runner_state,
                channel_tx,
                channel_rx,
                task_id,
            }
            .run()
            .await
            .unwrap();
        });
        WorkerHandle { handle }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let cancellation_token = self.runner_state.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                eprintln!("Worker received cancellation signal.");
                Ok(())
            }

            result = self.main() => {
                result
            }
        }
    }

    async fn main(&mut self) -> anyhow::Result<()> {
        let steps = self.start().await?;
        match self.run_steps(steps).await {
            Ok(()) => self.complete().await,
            Err(e) => self.fail(e.to_string()).await,
        }
    }

    async fn start(&mut self) -> anyhow::Result<Vec<Step>> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::StartRequest {
                runner_id: self.runner_state.runner_id,
                task_id: self.task_id,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::StartResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn run_steps(&mut self, steps: Vec<Step>) -> anyhow::Result<()> {
        let sandbox = self.runner_state.sandboxer.create()?;
        for step in steps {
            let step_id = step.step_id;
            self.start_step(step_id).await?;
            match self.run_step(sandbox.clone(), step).await {
                Ok(()) => self.complete_step(step_id).await?,
                Err(e) => {
                    self.fail_step(step_id, e.to_string()).await?;
                    return Err(anyhow::anyhow!("Step {} failed: {}", step_id, e));
                }
            }
        }
        Ok(())
    }

    async fn complete(&mut self) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::CompleteRequest {
                runner_id: self.runner_state.runner_id,
                task_id: self.task_id,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::CompleteResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn fail(&mut self, reason: String) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::FailRequest {
                runner_id: self.runner_state.runner_id,
                task_id: self.task_id,
                reason,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::FailResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn start_step(&mut self, step_id: StepId) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::StartStepRequest {
                runner_id: self.runner_state.runner_id,
                task_id: self.task_id,
                step_id,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::StartStepResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn run_step(&mut self, sandbox: Arc<dyn Sandbox>, step: Step) -> anyhow::Result<()> {
        for secret in step.secrets {
            match self.runner_state.secrets_service.resolve(&secret).await {
                Ok(value) => sandbox.add_secret(&secret.key, &value)?,
                Err(e) => {
                    tracing::warn!("Unable to resolve secret with key [{}]: {}", secret.key, e);
                    tracing::warn!("Skipping secret with key [{}]: {}", secret.key, e);
                }
            }
        }
        sandbox.spawn(&step.command)?;

        sandbox.clear_secrets()?;
        Ok(())
    }

    async fn complete_step(&mut self, step_id: StepId) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::CompleteStepRequest {
                runner_id: self.runner_state.runner_id,
                task_id: self.task_id,
                step_id,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::CompleteStepResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn fail_step(&mut self, step_id: StepId, reason: String) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::FailStepRequest {
                runner_id: self.runner_state.runner_id,
                task_id: self.task_id,
                step_id,
                reason,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::FailStepResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }
}
