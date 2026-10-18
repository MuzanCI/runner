use muzanci_observability::TError;
use muzanci_transport::message::TaskId;
use muzanci_transport::message::WorkerKillerMessage;
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
#[error("WorkerKillerError: {0}")]
struct WorkerKillerError(String);

pub struct WorkerKiller {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    task_id: TaskId,
}

impl WorkerKiller {
    pub fn spawn(
        join_set: &mut JoinSet<Result<(), TError>>,
        runner_state: Arc<RunnerState>,
        killed: CancellationToken,
        task_id: TaskId,
    ) -> AbortHandle {
        join_set.spawn(async move {
            let (channel_tx, channel_rx, channel_closed) = runner_state
                .mux_handle
                .open_channel(ChannelType::WorkerKiller)
                .await
                .unwrap();

            WorkerKiller {
                runner_state,
                channel_tx,
                task_id,
            }
            .run(killed, channel_closed, channel_rx)
            .await
        })
    }

    async fn run(
        self,
        killed: CancellationToken,
        channel_closed: CancellationToken,
        mut channel_rx: ChannelReceiver,
    ) -> Result<(), TError> {
        let runner_shutdown = self.runner_state.runner_shutdown();

        tokio::select! {
            _ = channel_closed.cancelled() => {
                Ok(())
            }
            _ = runner_shutdown.cancelled() => {
                Ok(())
            }
            result = self.long_poll(&mut channel_rx) => {
                if result.is_ok() {
                    killed.cancel();
                }

                result
            }
        }
    }

    async fn long_poll(&self, channel_rx: &mut ChannelReceiver) -> Result<(), TError> {
        self.channel_tx
            .send(Message::WorkerKiller(WorkerKillerMessage::PollRequest {
                runner_id: self.runner_state.runner_id,
                task_id: self.task_id,
            }))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::WorkerKiller(WorkerKillerMessage::PollResponse { .. }) => Ok(()),
                _ => Err(TError::new(&format!("Unexpected response: {response:?}"))),
            })
    }
}
