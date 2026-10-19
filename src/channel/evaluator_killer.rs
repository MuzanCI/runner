use muzanci_observability::TError;
use muzanci_transport::message::EvaluatorKillerMessage;
use muzanci_transport::message::TriggerId;
use muzanci_transport::message::TriggerStatus;
use std::sync::Arc;
use tokio::task::AbortHandle;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::Message;

use crate::RunnerState;

#[derive(Debug, thiserror::Error)]
#[error("EvaluatorKillerError: {0}")]
struct EvaluatorKillerError(String);

pub struct EvaluatorKiller {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
    trigger_id: TriggerId,
}

impl EvaluatorKiller {
    pub fn spawn(
        join_set: &mut JoinSet<Result<(), TError>>,
        runner_state: Arc<RunnerState>,
        killed: CancellationToken,
        trigger_id: TriggerId,
    ) -> AbortHandle {
        join_set.spawn(async move {
            let (channel_tx, channel_rx, channel_closed) = runner_state
                .mux_handle
                .open_channel(ChannelType::EvaluatorKiller)
                .await
                .unwrap();

            EvaluatorKiller {
                runner_state,
                channel_tx,
                channel_rx,
                trigger_id,
            }
            .main(channel_closed, killed)
            .await
        })
    }

    async fn main(
        mut self,
        channel_closed: CancellationToken,
        killed: CancellationToken,
    ) -> Result<(), TError> {
        tokio::select! {
            _ = channel_closed.cancelled() => {
                killed.cancel();
                Ok(())
            }
            result = self.long_poll() => {
                match result {
                    Ok(status) => {
                        if matches!(status, TriggerStatus::Interrupted | TriggerStatus::Disconnected | TriggerStatus::TimedOut | TriggerStatus::Canceled) {
                            killed.cancel();
                        }

                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    async fn long_poll(&mut self) -> Result<TriggerStatus, TError> {
        self.channel_tx
            .send(Message::EvaluatorKiller(
                EvaluatorKillerMessage::PollRequest {
                    runner_id: self.runner_state.runner_id,
                    trigger_id: self.trigger_id,
                },
            ))
            .await?;

        let message = self
            .channel_rx
            .recv()
            .await
            .ok_or_else(|| EvaluatorKillerError("Channel closed".to_string()))?;

        match message {
            Message::EvaluatorKiller(EvaluatorKillerMessage::PollResponse {
                result: Ok(status),
            }) => Ok(status),
            Message::EvaluatorKiller(EvaluatorKillerMessage::PollResponse { result: Err(e) }) => {
                Err(EvaluatorKillerError(e).into())
            }
            _ => Err(EvaluatorKillerError(format!("Unexpected response: {message:?}")).into()),
        }
    }
}
