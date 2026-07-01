use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use muzanci_interpreter::{EvalContext, EvalResult, Interpreter, Step};
use muzanci_transport::channel::{
    ChannelReceiver, ChannelSender, ChannelType, EvaluatorMessage, Message, RepoUrl, TriggerId,
};
use tokio::sync::mpsc;

use crate::{
    RunnerState,
    sandbox::{Output, Sandbox},
};

pub struct EvaluatorHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for EvaluatorHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct Evaluator {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
    trigger_id: TriggerId,
}

impl Evaluator {
    pub fn spawn(runner_state: Arc<RunnerState>, trigger_id: TriggerId) -> EvaluatorHandle {
        let runner_state = runner_state.clone();
        let handle = tokio::spawn(async move {
            let (channel_tx, channel_rx) = runner_state
                .mux_handle
                .open_channel(ChannelType::Evaluator)
                .await
                .unwrap();
            Evaluator {
                runner_state,
                channel_tx,
                channel_rx,
                trigger_id,
            }
            .run()
            .await
            .unwrap();
        });
        EvaluatorHandle { handle }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let cancellation_token = self.runner_state.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                eprintln!("Evaluator received cancellation signal.");
                Ok(())
            }

            result = self.main() => {
                result
            }
        }
    }

    async fn main(&mut self) -> anyhow::Result<()> {
        let repo_url = self.start().await?;
        let sandbox = self.runner_state.sandboxer.create()?;
        match self.evaluate(sandbox, repo_url).await {
            Ok(eval_result) => self.complete(eval_result).await,
            Err(e) => self.fail(e.to_string()).await,
        }
    }

    async fn start(&mut self) -> anyhow::Result<RepoUrl> {
        self.channel_tx
            .send(Message::Evaluator(EvaluatorMessage::StartRequest {
                runner_id: self.runner_state.runner_id,
                trigger_id: self.trigger_id,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Evaluator(EvaluatorMessage::StartResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn evaluate(
        &mut self,
        sandbox: Arc<dyn Sandbox>,
        repo_url: RepoUrl,
    ) -> anyhow::Result<EvalResult> {
        let evaluator_path = PathBuf::from("evaluator");
        let contents = &[0x0];
        let eval_result_path = PathBuf::from("pipeline.json");
        sandbox
            .create_executable_file(&evaluator_path, contents)
            .await?;

        let exit_status = {
            let (output_tx, output_rx) = mpsc::channel(1);
            let output_handle = EvaluatorStepOutput::spawn(
                self.runner_state.clone(),
                self.channel_tx.clone(),
                self.trigger_id,
                output_rx,
            );
            let command = format!(
                "./{} -o {} {}",
                evaluator_path.display(),
                eval_result_path.display(),
                repo_url
            );
            let secrets = vec![]; // TODO: Optionally add secrets for evaluator.
            let process_handle = sandbox.run(&command, secrets, output_tx);
            let (process_result, _output_result) = tokio::join!(process_handle, output_handle);
            process_result?
        };

        match exit_status.code() {
            Some(0) => {
                self.channel_tx
                    .send(Message::Evaluator(EvaluatorMessage::ExitCode {
                        runner_id: self.runner_state.runner_id,
                        trigger_id: self.trigger_id,
                        exit_code: 0,
                    }))
                    .await?
            }
            Some(exit_code) => {
                self.channel_tx
                    .send(Message::Evaluator(EvaluatorMessage::ExitCode {
                        runner_id: self.runner_state.runner_id,
                        trigger_id: self.trigger_id,
                        exit_code,
                    }))
                    .await?;
                anyhow::bail!("Evaluator exited with non-zero exit code: {}", exit_code)
            }
            None => {
                anyhow::bail!("Evaluator process terminated by signal")
            }
        };

        let eval_result_json = sandbox.read_file(&eval_result_path).await?;
        let eval_result = serde_json::from_str(&eval_result_json)?;
        Ok(eval_result)
    }

    async fn complete(&mut self, eval_result: EvalResult) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Evaluator(EvaluatorMessage::CompleteRequest {
                runner_id: self.runner_state.runner_id,
                trigger_id: self.trigger_id,
                pipelines: eval_result.pipelines,
                jobs: eval_result.jobs,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Evaluator(EvaluatorMessage::CompleteResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }

    async fn fail(&mut self, reason: String) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Evaluator(EvaluatorMessage::FailRequest {
                runner_id: self.runner_state.runner_id,
                trigger_id: self.trigger_id,
                reason,
            }))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::Evaluator(EvaluatorMessage::FailResponse { result }) => {
                    result.map_err(|e| anyhow::anyhow!(e))
                }
                _ => Err(anyhow::anyhow!("Unexpected message type")),
            })
    }
}

pub struct EvaluatorStepOutputHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for EvaluatorStepOutputHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct EvaluatorStepOutput {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    trigger_id: TriggerId,
    output_rx: mpsc::Receiver<Output>,
}

impl EvaluatorStepOutput {
    pub fn spawn(
        runner_state: Arc<RunnerState>,
        channel_tx: ChannelSender,
        trigger_id: TriggerId,
        output_rx: mpsc::Receiver<Output>,
    ) -> EvaluatorStepOutputHandle {
        let runner_state = runner_state.clone();
        let handle = tokio::spawn(async move {
            EvaluatorStepOutput {
                runner_state,
                channel_tx,
                trigger_id,
                output_rx,
            }
            .run()
            .await
            .unwrap();
        });
        EvaluatorStepOutputHandle { handle }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let cancellation_token = self.runner_state.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                eprintln!("EvaluatorStepOutput received cancellation signal.");
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
                    tracing::info!("Sending Evaluator stdout line. [{}] characters", line.len());
                    let result = self
                        .channel_tx
                        .send(Message::Evaluator(EvaluatorMessage::StdoutLine {
                            runner_id: self.runner_state.runner_id,
                            trigger_id: self.trigger_id,
                            line,
                        }))
                        .await;

                    if let Err(e) = result {
                        tracing::error!("Failed to send stdout line: {}", e);
                        anyhow::bail!("Failed to send stdout line: {}", e);
                    }
                }
                Output::Stderr(line) => {
                    tracing::info!("Sending Evaluator stderr line. [{}] characters", line.len());
                    let result = self
                        .channel_tx
                        .send(Message::Evaluator(EvaluatorMessage::StderrLine {
                            runner_id: self.runner_state.runner_id,
                            trigger_id: self.trigger_id,
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
