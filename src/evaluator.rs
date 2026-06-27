use std::sync::Arc;

use muzanci_transport::channel::{
    ChannelReceiver, ChannelSender, ChannelType, EvaluationId, EvaluatorMessage, Message,
    TriggerId, WaitingTrigger,
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
        // Since JoinHandle is Unpin, we can pin a mutable reference to it directly
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct Evaluator {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
    evaluation_id: EvaluationId,
}

impl Evaluator {
    pub fn spawn(runner_state: Arc<RunnerState>, evaluation_id: EvaluationId) -> EvaluatorHandle {
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
                evaluation_id,
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

            result = self.evaluate() => {
                result
            }
        }
    }

    async fn evaluate(&mut self) -> anyhow::Result<()> {
        // 1. Send StartEvaluationRequest to the server
        // 2.
        unimplemented!();
    }
}
