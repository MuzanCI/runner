use muzanci_observability::TError;
use muzanci_transport::message::TaskId;
use muzanci_transport::message::TaskStatus;
use muzanci_transport::message::WorkerKillerMessage;
use std::sync::Arc;
use tokio::task::AbortHandle;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::Message;

use crate::app::runner_state::RunnerState;

#[derive(Debug, thiserror::Error)]
#[error("WorkerKillerError: {0}")]
struct WorkerKillerError(String);

pub struct WorkerKiller {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
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
                .runner_mux_handle()
                .open_channel(ChannelType::DebugClient)
                .await?;

            WorkerKiller {
                runner_state,
                channel_tx,
                channel_rx,
                task_id,
            }
            .main(channel_closed, killed)
            .await
        })
    }

    #[tracing::instrument(
        skip_all,
        name = "worker_killer",
        fields(
            id = Uuid::now_v7().to_string(),
            server_id = self.runner_state.runner_mux_handle().runner_id().to_string(),
        )
    )]
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
                        if matches!(status, TaskStatus::Interrupted | TaskStatus::Disconnected | TaskStatus::TimedOut | TaskStatus::Canceled) {
                            killed.cancel();
                        }

                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    async fn long_poll(&mut self) -> Result<TaskStatus, TError> {
        self.channel_tx
            .send(Message::WorkerKiller(WorkerKillerMessage::PollRequest {
                runner_id: self.runner_state.runner_mux_handle().runner_id(),
                task_id: self.task_id,
            }))
            .await?;

        let message = self
            .channel_rx
            .recv()
            .await
            .ok_or_else(|| WorkerKillerError("Channel closed".to_string()))?;

        match message {
            Message::WorkerKiller(WorkerKillerMessage::PollResponse { result: Ok(status) }) => {
                Ok(status)
            }
            Message::WorkerKiller(WorkerKillerMessage::PollResponse { result: Err(e) }) => {
                Err(WorkerKillerError(e).into())
            }
            _ => Err(WorkerKillerError(format!("Unexpected response: {message:?}")).into()),
        }
    }
}
