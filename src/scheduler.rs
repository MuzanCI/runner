use std::sync::Arc;

use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::channel::EvaluatorSchedulerMessage;
use muzanci_transport::channel::Message;
use muzanci_transport::channel::TaskId;
use muzanci_transport::channel::TriggerId;
use muzanci_transport::channel::WaitingTask;
use muzanci_transport::channel::WaitingTrigger;
use muzanci_transport::channel::WorkerSchedulerMessage;

use crate::RunnerState;
use crate::evaluator::Evaluator;
use crate::worker::Worker;

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
}

impl EvaluatorScheduler {
    pub fn spawn(runner_state: Arc<RunnerState>) -> EvaluatorSchedulerHandle {
        let runner_state = runner_state.clone();
        let handle = tokio::spawn(async move {
            let (channel_tx, channel_rx) = runner_state
                .mux_handle
                .open_channel(ChannelType::EvaluatorScheduler)
                .await
                .unwrap();
            EvaluatorScheduler {
                runner_state,
                channel_tx,
                channel_rx,
            }
            .run()
            .await
            .unwrap();
        });
        EvaluatorSchedulerHandle { handle }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        tracing::info!("EvaluatorScheduler started running.");
        let cancellation_token = self.runner_state.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("EvaluatorScheduler received cancellation signal.");
                Ok(())
            }

            result = self.main() => {
                match result {
                    Ok(_) => {
                        tracing::info!("EvaluatorScheduler finished running.");
                    }
                    Err(e) => {
                        tracing::error!("EvaluatorScheduler encountered an error: {:?}", e);
                    }
                }
                Ok(())
            }
        }
    }

    async fn main(&mut self) -> anyhow::Result<()> {
        loop {
            let triggers = self.fetch_waiting_triggers().await?;

            // Iterate over triggers and attempt to reserve until capacity is reached or no more triggers are available.
            for trigger in triggers {
                let permit = self
                    .runner_state
                    .evaluation_capacity
                    .reserve(trigger.capacity)
                    .await?;
                match self.reserve_trigger(trigger.trigger_id).await {
                    Ok(_) => {
                        tracing::info!("Successfully reserved trigger {:?}", trigger);
                        Evaluator::spawn(
                            self.runner_state.clone(),
                            trigger.trigger_id,
                            trigger.capacity,
                        );
                        permit.commit();
                    }
                    Err(e) => {
                        tracing::error!("Failed to reserve trigger {:?}: {:?}", trigger, e);
                        drop(permit);
                    }
                }
            }

            // Wait for notification of available capacity before checking for triggers again.
            self.runner_state.evaluation_capacity.notified().await;
        }
    }

    // TODO: Add filters for waiting triggers.
    async fn fetch_waiting_triggers(&mut self) -> anyhow::Result<Vec<WaitingTrigger>> {
        tracing::info!("Fetching waiting triggers from the server.");
        self.channel_tx
            .send(Message::EvaluatorScheduler(
                EvaluatorSchedulerMessage::FetchWaitingTriggersRequest,
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::EvaluatorScheduler(
                    EvaluatorSchedulerMessage::FetchWaitingTriggersResponse { result },
                ) => result.map_err(|e| anyhow::anyhow!(e)),
                _ => {
                    tracing::error!("Unexpected response: {:?}", response);
                    Err(anyhow::anyhow!("Unexpected response"))
                }
            })
    }

    // Uses the reserve and commit pattern for cancellation safety.
    async fn reserve_trigger(&mut self, trigger_id: TriggerId) -> anyhow::Result<()> {
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
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::EvaluatorScheduler(
                    EvaluatorSchedulerMessage::ReserveTriggerResponse { result },
                ) => result.map_err(|e| anyhow::anyhow!(e)),
                _ => {
                    tracing::error!("Unexpected response: {:?}", response);
                    Err(anyhow::anyhow!("Unexpected response"))
                }
            })
    }
}

pub struct WorkerSchedulerHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for WorkerSchedulerHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct WorkerScheduler {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    channel_rx: ChannelReceiver,
}

impl WorkerScheduler {
    pub fn spawn(runner_state: Arc<RunnerState>) -> WorkerSchedulerHandle {
        let runner_state = runner_state.clone();
        let handle = tokio::spawn(async move {
            let (channel_tx, channel_rx) = runner_state
                .mux_handle
                .open_channel(ChannelType::WorkerScheduler)
                .await
                .unwrap();
            WorkerScheduler {
                runner_state,
                channel_tx,
                channel_rx,
            }
            .run()
            .await
            .unwrap();
        });
        WorkerSchedulerHandle { handle }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        tracing::info!("WorkerScheduler started running.");
        let cancellation_token = self.runner_state.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("WorkerScheduler received cancellation signal.");
                Ok(())
            }

            result = self.main() => {
                match result {
                    Ok(_) => {
                        tracing::info!("WorkerScheduler finished running.");
                    }
                    Err(e) => {
                        tracing::error!("WorkerScheduler encountered an error: {:?}", e);
                    }
                }
                Ok(())
            }
        }
    }

    async fn main(&mut self) -> anyhow::Result<()> {
        loop {
            let tasks = self.fetch_waiting_tasks().await?;

            // Iterate over tasks and attempt to reserve until capacity is reached or no more tasks are available.
            for task in tasks {
                let permit = self
                    .runner_state
                    .assignment_capacity
                    .reserve(task.capacity)
                    .await?;
                match self.reserve_task(task.task_id).await {
                    Ok(_) => {
                        tracing::info!("Successfully reserved task {:?}", task);
                        Worker::spawn(self.runner_state.clone(), task.task_id, task.capacity);
                        permit.commit();
                    }
                    Err(e) => {
                        tracing::error!("Failed to reserve task {:?}: {:?}", task, e);
                        drop(permit);
                    }
                }
            }

            // Wait for notification of available capacity before checking for tasks again.
            self.runner_state.assignment_capacity.notified().await;
        }
    }

    // TODO: Add filters for waiting tasks.
    async fn fetch_waiting_tasks(&mut self) -> anyhow::Result<Vec<WaitingTask>> {
        tracing::info!("Fetching waiting tasks from the server.");
        self.channel_tx
            .send(Message::WorkerScheduler(
                WorkerSchedulerMessage::FetchWaitingTasksRequest,
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::WorkerScheduler(WorkerSchedulerMessage::FetchWaitingTasksResponse {
                    result,
                }) => result.map_err(|e| anyhow::anyhow!(e)),
                _ => {
                    tracing::error!("Unexpected response: {:?}", response);
                    Err(anyhow::anyhow!("Unexpected response"))
                }
            })
    }

    // Uses the reserve and commit pattern for cancellation safety.
    async fn reserve_task(&mut self, task_id: TaskId) -> anyhow::Result<()> {
        self.channel_tx
            .send(Message::WorkerScheduler(
                WorkerSchedulerMessage::ReserveTaskRequest {
                    runner_id: self.runner_state.runner_id,
                    task_id,
                },
            ))
            .await?;

        self.channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))
            .and_then(|response| match response {
                Message::WorkerScheduler(WorkerSchedulerMessage::ReserveTaskResponse {
                    result,
                }) => result.map_err(|e| anyhow::anyhow!(e)),
                _ => {
                    eprintln!("Unexpected response: {:?}", response);
                    Err(anyhow::anyhow!("Unexpected response"))
                }
            })
    }
}
