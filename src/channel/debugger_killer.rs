use muzanci_config::config::DebugSessionId;
use muzanci_observability::TError;
use muzanci_transport::message::DebugSessionStatus;
use muzanci_transport::message::DebuggerKillerMessage;
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
#[error("DebuggerKillerError: {0}")]
struct DebuggerKillerError(String);

pub struct DebuggerKiller {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
    debug_session_id: DebugSessionId,
}

impl DebuggerKiller {
    pub fn spawn(
        join_set: &mut JoinSet<Result<(), TError>>,
        runner_state: Arc<RunnerState>,
        killed: CancellationToken,
        debug_session_id: DebugSessionId,
    ) -> AbortHandle {
        join_set.spawn(async move {
            let (channel_tx, channel_rx, channel_closed) = runner_state
                .runner_mux_handle()
                .open_channel(ChannelType::DebuggerKiller)
                .await
                .unwrap();

            DebuggerKiller {
                runner_state,
                channel_tx,
                channel_rx,
                debug_session_id,
            }
            .main(channel_closed, killed)
            .await
        })
    }

    #[tracing::instrument(
        skip_all,
        name = "debugger_killer",
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
                        if matches!(status, DebugSessionStatus::Interrupted | DebugSessionStatus::Disconnected | DebugSessionStatus::DebugClientConnectionTimedOut) {
                            killed.cancel();
                        }

                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn long_poll(&mut self) -> Result<DebugSessionStatus, TError> {
        self.channel_tx
            .send(Message::DebuggerKiller(
                DebuggerKillerMessage::PollRequest {
                    runner_id: self.runner_state.runner_mux_handle().runner_id(),
                    debug_session_id: self.debug_session_id,
                },
            ))
            .await?;

        let message = self
            .channel_rx
            .recv()
            .await
            .ok_or_else(|| DebuggerKillerError("Channel closed".to_string()))?;

        match message {
            Message::DebuggerKiller(DebuggerKillerMessage::PollResponse { result: Ok(status) }) => {
                Ok(status)
            }
            Message::DebuggerKiller(DebuggerKillerMessage::PollResponse { result: Err(e) }) => {
                Err(DebuggerKillerError(e).into())
            }
            _ => Err(DebuggerKillerError(format!("Unexpected response: {message:?}")).into()),
        }
    }
}
