use std::{path::Path, sync::Arc};

use muzanci_interpreter::{EvalContext, EvalResult, Interpreter, Step, StepId};
use muzanci_transport::channel::{
    Assignment, AssignmentId, ChannelReceiver, ChannelSender, ChannelType, Message, RepoUrl,
    WorkerMessage,
};

use crate::{RunnerState, jail::Jail};

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
    assignment_id: AssignmentId,
}

impl Worker {
    pub fn spawn(runner_state: Arc<RunnerState>, assignment_id: AssignmentId) -> WorkerHandle {
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
                assignment_id,
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
        let assignment = self.start_assignment().await?;
        match self.run_assignment(assignment).await {
            Ok(()) => self.complete_assignment().await,
            Err(e) => self.fail_assignment(e.to_string()).await,
        }
    }

    async fn start_assignment(&mut self) -> anyhow::Result<Assignment> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::StartAssignmentRequest {
                assignment_id: self.assignment_id,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::StartAssignmentResponse { assignment }) => {
                    Ok(assignment)
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn run_assignment(&mut self, assignment: Assignment) -> anyhow::Result<()> {
        let jail = self.runner_state.jailer.create()?;
        for step_id in assignment.step_ids {
            let step = self.start_step(step_id).await?;
            match self.run_step(&jail, step).await {
                Ok(()) => self.complete_step(step_id).await?,
                Err(e) => {
                    self.fail_step(step_id, e.to_string()).await?;
                    return Err(anyhow::anyhow!("Step {} failed: {}", step_id, e));
                }
            }
        }
        Ok(())
    }

    async fn complete_assignment(&mut self) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::CompleteAssignmentRequest {
                assignment_id: self.assignment_id,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::CompleteAssignmentResponse) => Ok(()),
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn fail_assignment(&mut self, reason: String) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::FailAssignmentRequest {
                assignment_id: self.assignment_id,
                reason,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::FailAssignmentResponse) => Ok(()),
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn start_step(&mut self, step_id: StepId) -> anyhow::Result<Step> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::StartStepRequest { step_id }))
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

    async fn run_step(&mut self, jail: &Box<dyn Jail>, step: Step) -> anyhow::Result<()> {
        for secret in step.secrets {
            match self.runner_state.secrets_service.resolve(&secret).await {
                Ok(value) => jail.add_secret(&secret.key, &value)?,
                Err(e) => {
                    tracing::warn!("Unable to resolve secret with key [{}]: {}", secret.key, e);
                    tracing::warn!("Skipping secret with key [{}]: {}", secret.key, e);
                }
            }
        }
        jail.spawn(&step.command)?;

        jail.clear_secrets()?;
        Ok(())
    }

    async fn complete_step(&mut self, step_id: StepId) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::CompleteStepRequest {
                step_id,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::CompleteStepResponse) => Ok(()),
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn fail_step(&mut self, step_id: StepId, reason: String) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Worker(WorkerMessage::FailStepRequest {
                step_id,
                reason,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Worker(WorkerMessage::FailStepResponse) => Ok(()),
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }
}
