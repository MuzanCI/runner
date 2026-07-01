use std::sync::Arc;

use muzanci_interpreter::{Step, StepId};
use muzanci_transport::channel::{
    ChannelReceiver, ChannelSender, ChannelType, Message, TaskId, WorkerMessage,
};
use tokio::{join, sync::mpsc};

use crate::{
    RunnerState,
    sandbox::{Output, Sandbox},
};

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

enum StepResult {
    Continue,
    Fail(String),
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
        let sandbox = self.runner_state.sandboxer.create()?;
        for step in steps {
            match self.run_step(sandbox.clone(), step).await? {
                StepResult::Continue => {
                    continue;
                }
                StepResult::Fail(reason) => {
                    self.fail(reason).await?;
                    // The step failed so we stop, but the worker itself
                    // is considered successful.
                    return Ok(());
                }
            }
        }
        self.complete().await
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

    async fn run_step(
        &mut self,
        sandbox: Arc<dyn Sandbox>,
        step: Step,
    ) -> anyhow::Result<StepResult> {
        let step_id = step.step_id;
        self.start_step(step_id).await?;

        let exit_status = {
            let (output_tx, output_rx) = mpsc::channel(1);
            let output_handle = WorkerStepOutput::spawn(
                self.runner_state.clone(),
                self.channel_tx.clone(),
                self.task_id,
                step_id,
                output_rx,
            );
            let process_handle = sandbox.run(&step.command, step.secrets.clone(), output_tx);
            let (process_result, _output_result) = join!(process_handle, output_handle);
            process_result?
        };

        match exit_status.code() {
            Some(0) => {
                self.channel_tx
                    .send(Message::Worker(WorkerMessage::ExitCode {
                        runner_id: self.runner_state.runner_id,
                        task_id: self.task_id,
                        step_id,
                        exit_code: 0,
                    }))
                    .await?;
                self.complete_step(step_id).await?;
                Ok(StepResult::Continue)
            }
            Some(exit_code) => {
                self.channel_tx
                    .send(Message::Worker(WorkerMessage::ExitCode {
                        runner_id: self.runner_state.runner_id,
                        task_id: self.task_id,
                        step_id,
                        exit_code,
                    }))
                    .await?;
                self.fail_step(
                    step_id,
                    format!("Process exited with non-zero status code: [{}]", exit_code),
                )
                .await?;
                Ok(StepResult::Fail(format!(
                    "Process exited with non-zero status code: [{}]",
                    exit_code
                )))
            }
            None => {
                self.fail_step(step_id, "Process terminated by signal".to_string())
                    .await?;
                Ok(StepResult::Fail("Process terminated by signal".to_string()))
            }
        }
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

pub struct WorkerStepOutputHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for WorkerStepOutputHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct WorkerStepOutput {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    task_id: TaskId,
    step_id: StepId,
    output_rx: mpsc::Receiver<Output>,
}

impl WorkerStepOutput {
    pub fn spawn(
        runner_state: Arc<RunnerState>,
        channel_tx: ChannelSender,
        task_id: TaskId,
        step_id: StepId,
        output_rx: mpsc::Receiver<Output>,
    ) -> WorkerStepOutputHandle {
        let runner_state = runner_state.clone();
        let handle = tokio::spawn(async move {
            WorkerStepOutput {
                runner_state,
                channel_tx,
                task_id,
                step_id,
                output_rx,
            }
            .run()
            .await
            .unwrap();
        });
        WorkerStepOutputHandle { handle }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let cancellation_token = self.runner_state.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                eprintln!("WorkerStepOutput received cancellation signal.");
                Ok(())
            }

            result = self.main() => {
                result
            }
        }
    }

    async fn main(&mut self) -> anyhow::Result<()> {
        while let Some(output) = self.output_rx.recv().await {
            match output {
                Output::Stdout(line) => {
                    tracing::info!("Sending Worker stdout line. [{}] characters", line.len());
                    let result = self
                        .channel_tx
                        .send(Message::Worker(WorkerMessage::StdoutLine {
                            runner_id: self.runner_state.runner_id,
                            task_id: self.task_id,
                            step_id: self.step_id,
                            line,
                        }))
                        .await;

                    if let Err(e) = result {
                        tracing::error!("Failed to send stdout line: {}", e);
                        anyhow::bail!("Failed to send stdout line: {}", e);
                    }
                }
                Output::Stderr(line) => {
                    tracing::info!("Sending Worker stderr line. [{}] characters", line.len());
                    let result = self
                        .channel_tx
                        .send(Message::Worker(WorkerMessage::StderrLine {
                            runner_id: self.runner_state.runner_id,
                            task_id: self.task_id,
                            step_id: self.step_id,
                            line,
                        }))
                        .await;

                    if let Err(e) = result {
                        tracing::error!("Failed to send stderr line: {}", e);
                        anyhow::bail!("Failed to send stderr line: {}", e);
                    }
                }
            }
        }
        Ok(())
    }
}
