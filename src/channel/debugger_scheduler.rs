use std::sync::Arc;

use muzanci_config::config::DebugSessionId;
use muzanci_observability::TError;
use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::DebuggerSchedulerMessage;
use muzanci_transport::message::Message;
use muzanci_transport::message::WaitingDebugSession;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::RunnerState;
use crate::channel::debugger::Debugger;

pub struct DebuggerSchedulerHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for DebuggerSchedulerHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct DebuggerScheduler {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
    join_set: JoinSet<Result<(), TError>>,
}

impl DebuggerScheduler {
    pub fn spawn(runner_state: Arc<RunnerState>) -> DebuggerSchedulerHandle {
        let handle = tokio::spawn(async move {
            let (channel_tx, channel_rx, channel_closed) = runner_state
                .mux_handle
                .open_channel(ChannelType::DebuggerScheduler)
                .await
                .unwrap();

            let join_set = JoinSet::new();

            DebuggerScheduler {
                runner_state,
                channel_tx,
                channel_rx,
                join_set,
            }
            .main(channel_closed)
            .await
            .unwrap();
        });

        DebuggerSchedulerHandle { handle }
    }

    async fn main(mut self, channel_closed: CancellationToken) -> Result<(), TError> {
        tracing::info!("DebuggerScheduler started running.");

        let cancelled_fut = {
            let runner_shutdown = self.runner_state.runner_shutdown();

            async move {
                tokio::select! {
                    _ = runner_shutdown.cancelled() => {
                        tracing::info!("DebuggerScheduler received runner shutdown signal.");
                    }
                    _ = channel_closed.cancelled() => {
                        tracing::info!("Channel closed. Stopping DebuggerScheduler.");
                    }
                }
            }
        };

        tokio::select! {
            _ = cancelled_fut => {
                tracing::info!("Waiting for subtasks to finish...");

                while let Some(result) = self.join_set.join_next().await {
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            tracing::error!("DebuggerScheduler subtask error: {e:?}");
                        }
                        Err(e) => {
                            tracing::error!("DebuggerScheduler join error: {e:?}");
                        }
                    }
                }

                tracing::info!("All subtasks finished. Stopping DebuggerScheduler.");
            }

            result = self.run() => {
                result?;
            }
        }

        Ok(())
    }

    async fn run(&mut self) -> Result<(), TError> {
        loop {
            let debugs = self.fetch_waiting_debugs().await?;

            // Iterate over debugs and attempt to reserve until capacity is reached or no more debugs are available.
            for waiting_debug in debugs {
                let capacity = 1;
                let permit = match self
                    .runner_state
                    .shared_assignment_capacity_handle
                    .reserve_high(capacity)
                    .await
                {
                    Ok(permit) => permit,
                    Err(e) => {
                        tracing::error!("Failed to reserve capacity {:?}: {:?}", capacity, e);
                        continue;
                    }
                };
                match self.reserve_debug(waiting_debug.debug_session_id).await {
                    Ok(()) => {
                        tracing::info!("Successfully reserved debug {:?}", waiting_debug);

                        Debugger::spawn(
                            &mut self.join_set,
                            self.runner_state.clone(),
                            waiting_debug.debug_session_id,
                            permit,
                        );
                    }
                    Err(e) => {
                        tracing::error!("Failed to reserve debug {:?}: {:?}", waiting_debug, e);
                        drop(permit);
                    }
                }
            }

            // Wait for notification of available capacity before checking for waiting debugs again.
            tracing::info!(
                "Waiting for available capacity before checking for waiting debugs again."
            );
            // TODO: Fix bug where scheduler does not check server again, even if capacity is available.
            self.runner_state
                .shared_assignment_capacity_handle
                .notified()
                .await;
        }
    }

    // TODO: Add filters for waiting debugs.
    async fn fetch_waiting_debugs(&mut self) -> Result<Vec<WaitingDebugSession>, TError> {
        tracing::info!("Fetching waiting debugs from the server.");
        self.channel_tx
            .send(Message::DebuggerScheduler(
                DebuggerSchedulerMessage::FetchWaitingDebugSessionsRequest {
                    runner_id: self.runner_state.runner_id,
                },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::DebuggerScheduler(
                    DebuggerSchedulerMessage::FetchWaitingDebugSessionsResponse { result },
                ) => result.map_err(|e| TError::new(&e.to_string())),
                _ => {
                    tracing::error!("Unexpected response: {:?}", response);
                    Err(TError::new("Unexpected response"))
                }
            })
    }

    // Uses the reserve and commit pattern for cancellation safety.
    async fn reserve_debug(&mut self, debug_session_id: DebugSessionId) -> Result<(), TError> {
        self.channel_tx
            .send(Message::DebuggerScheduler(
                DebuggerSchedulerMessage::ReserveDebugSessionRequest {
                    runner_id: self.runner_state.runner_id,
                    debug_session_id,
                },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::DebuggerScheduler(
                    DebuggerSchedulerMessage::ReserveDebugSessionResponse { result },
                ) => result.map_err(|e| TError::new(&e.to_string())),
                _ => {
                    eprintln!("Unexpected response: {:?}", response);
                    Err(TError::new("Unexpected response"))
                }
            })
    }
}
