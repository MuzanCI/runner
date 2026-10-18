use muzanci_observability::TError;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::EvaluatorSchedulerMessage;
use muzanci_transport::message::Message;
use muzanci_transport::message::TriggerId;
use muzanci_transport::message::WaitingTrigger;

use crate::RunnerState;
use crate::channel::evaluator::Evaluator;

pub struct EvaluatorSchedulerHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for EvaluatorSchedulerHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct EvaluatorScheduler {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
    join_set: JoinSet<Result<(), TError>>,
}

impl EvaluatorScheduler {
    pub fn spawn(runner_state: Arc<RunnerState>) -> EvaluatorSchedulerHandle {
        let handle = tokio::spawn(async move {
            let (channel_tx, channel_rx, channel_closed) = runner_state
                .mux_handle
                .open_channel(ChannelType::EvaluatorScheduler)
                .await
                .unwrap();

            let join_set = JoinSet::new();

            EvaluatorScheduler {
                runner_state,
                channel_tx,
                channel_rx,
                join_set,
            }
            .main(channel_closed)
            .await
            .unwrap();
        });

        EvaluatorSchedulerHandle { handle }
    }

    async fn main(mut self, channel_closed: CancellationToken) -> Result<(), TError> {
        tracing::info!("EvaluatorScheduler started running.");

        let cancelled_fut = {
            let runner_shutdown = self.runner_state.runner_shutdown();

            async move {
                tokio::select! {
                    _ = runner_shutdown.cancelled() => {
                        tracing::info!("EvaluatorScheduler received runner shutdown signal.");
                    }
                    _ = channel_closed.cancelled() => {
                        tracing::info!("Channel closed. Stopping EvaluatorScheduler.");
                    }
                }
            }
        };

        tokio::select! {
            _ = cancelled_fut => {
                while let Some(result) = self.join_set.join_next().await {
                    tracing::info!("Waiting for EvaluatorScheduler subtasks to finish...");

                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            tracing::error!("EvaluatorScheduler subtask error: {e:?}");
                        }
                        Err(e) => {
                            tracing::error!("EvaluatorScheduler join error: {e:?}");
                        }
                    }

                    tracing::info!("All subtasks finished. Stopping EvaluatorScheduler.");
                }
            }
            result = self.run() => {
                result?;
            }
        }

        Ok(())
    }

    async fn run(&mut self) -> Result<(), TError> {
        loop {
            let triggers = self.fetch_waiting_triggers().await?;

            // Iterate over triggers and attempt to reserve until capacity is reached or no more triggers are available.
            for trigger in triggers {
                let permit = match self
                    .runner_state
                    .shared_evaluation_capacity
                    .reserve(trigger.capacity)
                    .await
                {
                    Ok(permit) => permit,
                    Err(e) => {
                        tracing::error!("Failed to reserve capacity {:?}: {:?}", trigger, e);
                        continue;
                    }
                };

                match self.reserve_trigger(trigger.trigger_id).await {
                    Ok(()) => {
                        tracing::info!("Successfully reserved trigger {:?}", trigger);

                        Evaluator::spawn(
                            &mut self.join_set,
                            self.runner_state.clone(),
                            trigger.trigger_id,
                            trigger.capacity,
                            permit,
                        );
                    }
                    Err(e) => {
                        tracing::error!("Failed to reserve trigger {:?}: {:?}", trigger, e);
                        drop(permit);
                    }
                }
            }

            // TODO: Fix bug where scheduler does not check server again, even if capacity is available.
            // Wait for notification of available capacity before checking for triggers again.
            self.runner_state
                .shared_evaluation_capacity
                .notified()
                .await;
        }
    }

    // TODO: Add filters for waiting triggers.
    async fn fetch_waiting_triggers(&mut self) -> Result<Vec<WaitingTrigger>, TError> {
        tracing::info!("Fetching waiting triggers from the server.");
        self.channel_tx
            .send(Message::EvaluatorScheduler(
                EvaluatorSchedulerMessage::FetchWaitingTriggersRequest {
                    runner_id: self.runner_state.runner_id,
                },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::EvaluatorScheduler(
                    EvaluatorSchedulerMessage::FetchWaitingTriggersResponse { result },
                ) => result.map_err(|e| TError::new(&e.to_string())),
                _ => {
                    tracing::error!("Unexpected response: {:?}", response);
                    Err(TError::new("Unexpected response"))
                }
            })
    }

    // Uses the reserve and commit pattern for cancellation safety.
    async fn reserve_trigger(&mut self, trigger_id: TriggerId) -> Result<(), TError> {
        self.channel_tx
            .send(Message::EvaluatorScheduler(
                EvaluatorSchedulerMessage::ReserveTriggerRequest {
                    runner_id: self.runner_state.runner_id,
                    trigger_id,
                },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::EvaluatorScheduler(
                    EvaluatorSchedulerMessage::ReserveTriggerResponse { result },
                ) => result.map_err(|e| TError::new(&e.to_string())),
                _ => {
                    tracing::error!("Unexpected response: {:?}", response);
                    Err(TError::new("Unexpected response"))
                }
            })
    }
}
