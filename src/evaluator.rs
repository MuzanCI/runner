use std::path::PathBuf;
use std::sync::Arc;

use muzanci_interpreter::Config;
use muzanci_interpreter::GitCloneShowArgs;
use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::channel::EvaluatorMessage;
use muzanci_transport::channel::ExitStatus;
use muzanci_transport::channel::Message;
use muzanci_transport::channel::ProcessOutput;
use muzanci_transport::channel::TriggerId;
use tokio::sync::mpsc;

use crate::RunnerState;
use crate::sandbox::Sandbox;

const INTERPRETER_BIN_BYTES: &[u8] = include_bytes!("../embed/interpreter");

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
                tracing::info!("Evaluator received cancellation signal.");
                Ok(())
            }

            result = self.main() => {
                result
            }
        }
    }

    async fn main(&mut self) -> anyhow::Result<()> {
        let args = self.start().await?;
        let sandbox = self.runner_state.sandboxer.create()?;
        match self.evaluate(sandbox, args.clone()).await {
            Ok(eval_result) => self.complete(eval_result).await,
            Err(e) => self.fail(e.to_string()).await,
        }
    }

    async fn start(&mut self) -> anyhow::Result<GitCloneShowArgs> {
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
        args: GitCloneShowArgs,
    ) -> anyhow::Result<Config> {
        let exec_path = PathBuf::from("./interpreter");
        sandbox
            .create_executable_file(&exec_path, INTERPRETER_BIN_BYTES)
            .await?;

        let eval_result_path = PathBuf::from("./muzanci.eval_result.json");
        let process_result = {
            let (output_tx, output_rx) = mpsc::channel(1);
            let output_handle = EvaluatorProcessOutput::spawn(
                self.runner_state.clone(),
                self.channel_tx.clone(),
                self.trigger_id,
                output_rx,
            );
            let args: String = args.into();
            let command = format!("./{} {}", exec_path.display(), args);
            let secrets = vec![]; // TODO: Optionally add secrets for evaluator.
            let process_handle = sandbox.run(&command, secrets, output_tx);
            let (process_result, _output_result) = tokio::join!(process_handle, output_handle);
            process_result
        };

        let exit_status = match process_result?.code() {
            Some(code) => ExitStatus::Code(code),
            None => ExitStatus::Signal,
        };

        self.channel_tx
            .send(Message::Evaluator(EvaluatorMessage::ProcessExitStatus {
                runner_id: self.runner_state.runner_id,
                trigger_id: self.trigger_id,
                exit_status,
            }))
            .await?;

        match exit_status {
            ExitStatus::Code(code) if code == 0 => {
                // Evaluator process completed successfully.
            }
            ExitStatus::Code(code) => {
                anyhow::bail!("Evaluator exited with non-zero status code: {}", code);
            }
            ExitStatus::Signal => {
                anyhow::bail!("Evaluator terminated by signal.");
            }
        }

        let eval_result_json = sandbox.read_file(&eval_result_path).await?;
        let eval_result = serde_json::from_str(&eval_result_json)?;
        Ok(eval_result)
    }

    async fn complete(&mut self, config: Config) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Evaluator(EvaluatorMessage::CompleteRequest {
                runner_id: self.runner_state.runner_id,
                trigger_id: self.trigger_id,
                config,
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

pub struct EvaluatorProcessOutputHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for EvaluatorProcessOutputHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct EvaluatorProcessOutput {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    trigger_id: TriggerId,
    output_rx: mpsc::Receiver<ProcessOutput>,
}

impl EvaluatorProcessOutput {
    pub fn spawn(
        runner_state: Arc<RunnerState>,
        channel_tx: ChannelSender,
        trigger_id: TriggerId,
        output_rx: mpsc::Receiver<ProcessOutput>,
    ) -> EvaluatorProcessOutputHandle {
        let runner_state = runner_state.clone();
        let handle = tokio::spawn(async move {
            EvaluatorProcessOutput {
                runner_state,
                channel_tx,
                trigger_id,
                output_rx,
            }
            .run()
            .await
            .unwrap();
        });
        EvaluatorProcessOutputHandle { handle }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        let cancellation_token = self.runner_state.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                eprintln!("EvaluatorProcessOutput received cancellation signal.");
                Ok(())
            }

            result = self.main() => {
                result
            }
        }
    }

    async fn main(&mut self) -> anyhow::Result<()> {
        while let Some(output) = self.output_rx.recv().await {
            let result = self
                .channel_tx
                .send(Message::Evaluator(EvaluatorMessage::ProcessOutput {
                    runner_id: self.runner_state.runner_id,
                    trigger_id: self.trigger_id,
                    output,
                }))
                .await;

            if let Err(e) = result {
                tracing::error!("Failed to send process output: {}", e);
                anyhow::bail!("Failed to send process output: {}", e);
            }
        }
        Ok(())
    }
}
