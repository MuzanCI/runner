use std::{path::Path, sync::Arc};

use muzanci_interpreter::{EvalContext, EvalResult, Interpreter};
use muzanci_transport::channel::{
    ChannelReceiver, ChannelSender, ChannelType, EvaluatorId, EvaluatorMessage, Message, RepoUrl,
};

use crate::RunnerState;

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
    evaluator_id: EvaluatorId,
}

impl Evaluator {
    pub fn spawn(runner_state: Arc<RunnerState>, evaluator_id: EvaluatorId) -> EvaluatorHandle {
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
                evaluator_id,
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
        let repo_url = self.start_evaluation().await?;

        match self.run_evaluation(repo_url).await {
            Ok(eval_result) => self.complete_evaluation(eval_result).await,
            Err(e) => self.fail_evaluation(e.to_string()).await,
        }
    }

    async fn start_evaluation(&mut self) -> anyhow::Result<RepoUrl> {
        self.channel_tx
            .send(Message::Evaluator(EvaluatorMessage::StartRequest {
                evaluator_id: self.evaluator_id,
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

    async fn run_evaluation(&mut self, repo_url: RepoUrl) -> anyhow::Result<EvalResult> {
        // Create Sandbox.
        let sandbox = self.runner_state.sandboxer.create()?;
        // git clone repo_url
        sandbox
            .spawn(&format!("git clone {}", repo_url.to_string()))?
            .wait()
            .await?;
        // Parse muzan.py from root.
        let interpreter = Interpreter::new(EvalContext {
            git_repo: repo_url.to_string(),
            git_branch: "main".to_string(),
            git_commit: "HEAD".to_string(),
        });
        interpreter.evaluate(&Path::new("muzan.py"))
    }

    async fn complete_evaluation(&mut self, eval_result: EvalResult) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Evaluator(EvaluatorMessage::CompleteRequest {
                evaluator_id: self.evaluator_id,
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

    async fn fail_evaluation(&mut self, reason: String) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::Evaluator(EvaluatorMessage::FailRequest {
                evaluator_id: self.evaluator_id,
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
