use muzanci_observability::TError;
use std::sync::Arc;
use tokio::task::AbortHandle;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::EvaluatorSchedulerMessage;
use muzanci_transport::message::Message;
use muzanci_transport::message::TriggerId;
use muzanci_transport::message::WaitingTrigger;

use crate::app::runner_state::RunnerState;
use crate::channel::evaluator::Evaluator;
use crate::channel::evaluator_killer::EvaluatorKiller;

pub struct EvaluatorScheduler {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
    join_set: JoinSet<Result<(), TError>>,
}

impl EvaluatorScheduler {
    pub fn spawn(
        join_set: &mut JoinSet<Result<(), TError>>,
        runner_state: Arc<RunnerState>,
    ) -> AbortHandle {
        join_set.spawn(async move {
            let (channel_tx, channel_rx, channel_closed) = runner_state
                .runner_mux_handle()
                .open_channel(ChannelType::DebugClient)
                .await?;

            EvaluatorScheduler {
                runner_state,
                channel_tx,
                channel_rx,
                join_set: JoinSet::new(),
            }
            .main(channel_closed)
            .await
        })
    }

    #[tracing::instrument(
        skip_all,
        name = "evaluator_scheduler",
        fields(
            id = Uuid::now_v7().to_string(),
            runner_id = self.runner_state.runner_mux_handle().runner_id().to_string(),
        )
    )]
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
            biased;
            _ = cancelled_fut => {
                self.runner_state.runner_shutdown().cancel();

                tracing::info!("Waiting for subtasks to finish...");

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

    #[tracing::instrument(skip_all)]
    async fn run(&mut self) -> Result<(), TError> {
        loop {
            let waiting_triggers = self.fetch_waiting_triggers().await?;

            // Iterate over triggers and attempt to reserve until capacity is reached or no more triggers are available.
            for waiting_trigger in waiting_triggers {
                let permit = match self
                    .runner_state
                    .evaluation_capacity()
                    .reserve(waiting_trigger.capacity)
                    .await
                {
                    Ok(permit) => permit,
                    Err(e) => {
                        tracing::error!(
                            "Failed to reserve capacity {:?}: {:?}",
                            waiting_trigger,
                            e
                        );
                        continue;
                    }
                };

                match self.reserve_trigger(waiting_trigger.trigger_id).await {
                    Ok(()) => {
                        tracing::info!("Successfully reserved trigger {:?}", waiting_trigger);

                        let killed = CancellationToken::new();

                        EvaluatorKiller::spawn(
                            &mut self.join_set,
                            self.runner_state.clone(),
                            killed.clone(),
                            waiting_trigger.trigger_id,
                        );

                        Evaluator::spawn(
                            &mut self.join_set,
                            self.runner_state.clone(),
                            killed,
                            waiting_trigger.trigger_id,
                            waiting_trigger.capacity,
                            permit,
                        );
                    }
                    Err(e) => {
                        tracing::error!("Failed to reserve trigger {:?}: {:?}", waiting_trigger, e);
                        drop(permit);
                    }
                }
            }

            // TODO: Fix bug where scheduler does not check server again, even if capacity is available.
            // Wait for notification of available capacity before checking for triggers again.
            self.runner_state.evaluation_capacity().notified().await;
        }
    }

    // TODO: Add filters for waiting triggers.
    #[tracing::instrument(skip_all)]
    async fn fetch_waiting_triggers(&mut self) -> Result<Vec<WaitingTrigger>, TError> {
        tracing::info!("Fetching waiting triggers from the server.");
        self.channel_tx
            .send(Message::EvaluatorScheduler(
                EvaluatorSchedulerMessage::FetchWaitingTriggersRequest {
                    runner_id: self.runner_state.runner_mux_handle().runner_id(),
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
    #[tracing::instrument(skip_all)]
    async fn reserve_trigger(&mut self, trigger_id: TriggerId) -> Result<(), TError> {
        self.channel_tx
            .send(Message::EvaluatorScheduler(
                EvaluatorSchedulerMessage::ReserveTriggerRequest {
                    runner_id: self.runner_state.runner_mux_handle().runner_id(),
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
