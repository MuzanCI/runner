use muzanci_observability::TError;
use std::sync::Arc;
use tokio::task::AbortHandle;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::Message;
use muzanci_transport::message::TaskId;
use muzanci_transport::message::WaitingTask;
use muzanci_transport::message::WorkerSchedulerMessage;

use crate::app::runner_state::RunnerState;
use crate::channel::worker::Worker;
use crate::channel::worker_killer::WorkerKiller;

pub struct WorkerScheduler {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
    join_set: JoinSet<Result<(), TError>>,
}

impl WorkerScheduler {
    pub fn spawn(
        join_set: &mut JoinSet<Result<(), TError>>,
        runner_state: Arc<RunnerState>,
    ) -> AbortHandle {
        join_set.spawn(async move {
            let (channel_tx, channel_rx, channel_closed) = runner_state
                .runner_mux_handle()
                .open_channel(ChannelType::WorkerScheduler)
                .await?;

            WorkerScheduler {
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
        name = "worker_scheduler",
        fields(
            id = Uuid::now_v7().to_string(),
            runner_id = self.runner_state.runner_mux_handle().runner_id().to_string(),
        )
    )]
    async fn main(mut self, channel_closed: CancellationToken) -> Result<(), TError> {
        tracing::info!("WorkerScheduler started running.");

        let cancelled_fut = {
            let runner_shutdown = self.runner_state.runner_shutdown();

            async move {
                tokio::select! {
                    _ = runner_shutdown.cancelled() => {
                        tracing::info!("WorkerScheduler received runner shutdown signal.");
                    }
                    _ = channel_closed.cancelled() => {
                        tracing::info!("Channel closed. Stopping WorkerScheduler.");
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
                    match result {
                        Ok(Ok(())) => {}
                        Ok(Err(e)) => {
                            tracing::error!("WorkerScheduler subtask error: {e:?}");
                        }
                        Err(e) => {
                            tracing::error!("WorkerScheduler join error: {e:?}");
                        }
                    }
                }

                tracing::info!("All subtasks finished. Stopping WorkerScheduler.");
            }

            result = self.run() => {
                result?;
            }
        }

        Ok(())
    }

    async fn run(&mut self) -> Result<(), TError> {
        loop {
            let waiting_tasks = self.fetch_waiting_tasks().await?;

            // Iterate over tasks and attempt to reserve until capacity is reached or no more tasks are available.
            for waiting_task in waiting_tasks {
                let permit = match self
                    .runner_state
                    .assignment_capacity()
                    .reserve_low(waiting_task.capacity)
                    .await
                {
                    Ok(permit) => permit,
                    Err(e) => {
                        tracing::error!(
                            "Failed to reserve capacity {:?}: {:?}",
                            waiting_task.capacity,
                            e
                        );
                        continue;
                    }
                };

                match self.reserve_task(waiting_task.task_id).await {
                    Ok(()) => {
                        tracing::info!("Successfully reserved waiting_task {:?}", waiting_task);

                        let killed = CancellationToken::new();

                        WorkerKiller::spawn(
                            &mut self.join_set,
                            self.runner_state.clone(),
                            killed.clone(),
                            waiting_task.task_id,
                        );

                        Worker::spawn(
                            &mut self.join_set,
                            self.runner_state.clone(),
                            killed,
                            waiting_task.task_id,
                            permit,
                        );
                    }
                    Err(e) => {
                        tracing::error!("Failed to reserve task {:?}: {:?}", waiting_task, e);
                        drop(permit);
                    }
                }
            }

            // TODO: Fix bug where scheduler does not check server again, even if capacity is available.

            // Wait for notification of available capacity before checking for tasks again.
            self.runner_state.assignment_capacity().notified().await;
        }
    }

    // TODO: Add filters for waiting tasks.
    async fn fetch_waiting_tasks(&mut self) -> Result<Vec<WaitingTask>, TError> {
        tracing::info!("Fetching waiting tasks from the server.");
        self.channel_tx
            .send(Message::WorkerScheduler(
                WorkerSchedulerMessage::FetchWaitingTasksRequest {
                    runner_id: self.runner_state.runner_mux_handle().runner_id(),
                },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::WorkerScheduler(WorkerSchedulerMessage::FetchWaitingTasksResponse {
                    result,
                }) => result.map_err(|e| TError::new(&e.to_string())),
                _ => {
                    tracing::error!("Unexpected response: {:?}", response);
                    Err(TError::new("Unexpected response"))
                }
            })
    }

    // Uses the reserve and commit pattern for cancellation safety.
    async fn reserve_task(&mut self, task_id: TaskId) -> Result<(), TError> {
        self.channel_tx
            .send(Message::WorkerScheduler(
                WorkerSchedulerMessage::ReserveTaskRequest {
                    runner_id: self.runner_state.runner_mux_handle().runner_id(),
                    task_id,
                },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::WorkerScheduler(WorkerSchedulerMessage::ReserveTaskResponse {
                    result,
                }) => result.map_err(|e| TError::new(&e.to_string())),
                _ => {
                    eprintln!("Unexpected response: {:?}", response);
                    Err(TError::new("Unexpected response"))
                }
            })
    }
}
