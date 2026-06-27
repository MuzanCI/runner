use std::sync::Arc;

use muzanci_transport::channel::{
    ChannelReceiver, ChannelSender, ChannelType, EvaluatorSchedulerMessage, Message, TriggerId,
    WaitingTrigger,
};

use crate::{RunnerState, evaluator::Evaluator};

pub struct EvaluatorSchedulerHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for EvaluatorSchedulerHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // Since JoinHandle is Unpin, we can pin a mutable reference to it directly
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
        let cancellation_token = self.runner_state.cancellation_token.clone();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                eprintln!("EvaluatorScheduler received cancellation signal.");
                Ok(())
            }

            result = self.tick() => {
                result
            }
        }
    }

    async fn tick(&mut self) -> anyhow::Result<()> {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            if !self.runner_state.has_evaluation_capacity() {
                continue;
            }

            let triggers = self.fetch_waiting_triggers().await?;

            // Iterate over triggers and attempt to reserve until capacity is reached or no more triggers are available.
            for trigger in triggers {
                if !self.runner_state.has_evaluation_capacity() {
                    break;
                }

                match self.reserve_trigger(trigger).await {
                    Ok(_) => {
                        // Commit the permit since the trigger was successfully reserved.
                        // Spawn a task to handle evaluation of the trigger.
                        eprintln!("Reserved trigger: {:?}", trigger);
                    }
                    Err(e) => {
                        eprintln!("Failed to reserve trigger {:?}: {:?}", trigger, e);
                    }
                }
            }
        }
    }

    async fn fetch_waiting_triggers(&mut self) -> anyhow::Result<Vec<WaitingTrigger>> {
        self.channel_tx
            .send(Message::EvaluatorScheduler(
                EvaluatorSchedulerMessage::FetchWaitingTriggersRequest,
            ))
            .await?;

        let response = self
            .channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))?;
        match response {
            Message::EvaluatorScheduler(
                EvaluatorSchedulerMessage::FetchWaitingTriggersResponse { triggers },
            ) => Ok(triggers),
            _ => {
                eprintln!("Unexpected response: {:?}", response);
                Err(anyhow::anyhow!("Unexpected response"))
            }
        }
    }

    // Uses the reserve and commit pattern for cancellation safety.
    async fn reserve_trigger(&mut self, trigger: WaitingTrigger) -> anyhow::Result<()> {
        let permit = self
            .runner_state
            .evaluation_capacity
            .reserve(trigger.capacity)
            .await?;

        self.channel_tx
            .send(Message::EvaluatorScheduler(
                EvaluatorSchedulerMessage::ReserveTriggerRequest {
                    trigger_id: trigger.trigger_id,
                },
            ))
            .await?;

        let response = self
            .channel_rx
            .recv()
            .await
            .ok_or(anyhow::anyhow!("Channel closed"))?;

        match response {
            Message::EvaluatorScheduler(EvaluatorSchedulerMessage::ReserveTriggerResponse {
                evaluation_id,
            }) => {
                permit.commit();
                Evaluator::spawn(self.runner_state.clone(), evaluation_id).await;
                Ok(())
            }
            _ => {
                eprintln!("Unexpected response: {:?}", response);
                Err(anyhow::anyhow!("Unexpected response"))
            }
        }
    }
}
