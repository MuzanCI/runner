use muzanci_observability::TError;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::task::AbortHandle;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use muzanci_config::config::Config;
use muzanci_config::config::TriggerConfig;
use muzanci_git::GitClient;
use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::message::EvaluatorMessage;
use muzanci_transport::message::Message;
use muzanci_transport::message::TriggerId;

use crate::app::runner_state::RunnerState;
use crate::evaluation_capacity::EvaluationCapacity;
use crate::evaluation_capacity::EvaluationCapacityPermit;

pub struct Evaluator {
    runner_state: Arc<RunnerState>,
    channel_tx: ChannelSender,
    trigger_id: TriggerId,
    _capacity: EvaluationCapacity,
    _permit: EvaluationCapacityPermit,
}

impl Evaluator {
    pub fn spawn(
        join_set: &mut JoinSet<Result<(), TError>>,
        runner_state: Arc<RunnerState>,
        killed: CancellationToken,
        trigger_id: TriggerId,
        capacity: EvaluationCapacity,
        permit: EvaluationCapacityPermit,
    ) -> AbortHandle {
        join_set.spawn(async move {
            let (channel_tx, channel_rx, channel_closed) = runner_state
                .runner_mux_handle()
                .open_channel(ChannelType::Evaluator)
                .await?;

            Evaluator {
                runner_state,
                channel_tx,
                trigger_id,
                _capacity: capacity,
                _permit: permit,
            }
            .main(killed, channel_closed, channel_rx)
            .await
        })
    }

    #[tracing::instrument(
        skip_all,
        name = "evaluator",
        fields(
            id = Uuid::now_v7().to_string(),
            server_id = self.runner_state.runner_mux_handle().runner_id().to_string(),
        )
    )]
    async fn main(
        self,
        killed: CancellationToken,
        channel_closed: CancellationToken,
        channel_rx: ChannelReceiver,
    ) -> Result<(), TError> {
        tokio::select! {
            _ = killed.cancelled() => {
                tracing::info!("Killed, stopping Evaluator.");
                Ok(())
            }

            _ = channel_closed.cancelled() => {
                tracing::info!("Channel closed. Stopping Evaluator.");
                Ok(())
            }

            result = self.run(channel_rx) => {
                result
            }
        }
    }

    async fn run(self, mut channel_rx: ChannelReceiver) -> Result<(), TError> {
        let config = self.start(&mut channel_rx).await?;
        match self.evaluate(&config).await {
            Ok(config) => self.complete(&mut channel_rx, config).await,
            Err(e) => self.fail(&mut channel_rx, e.to_string()).await,
        }
    }

    async fn start(&self, channel_rx: &mut ChannelReceiver) -> Result<TriggerConfig, TError> {
        self.channel_tx
            .send(Message::Evaluator(EvaluatorMessage::StartRequest {
                runner_id: self.runner_state.runner_mux_handle().runner_id(),
                trigger_id: self.trigger_id,
            }))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::Evaluator(EvaluatorMessage::StartResponse { result }) => {
                    result.map_err(|e| TError::new(&e.to_string()))
                }
                _ => Err(TError::new("Unexpected message type")),
            })
    }

    async fn evaluate(&self, config: &TriggerConfig) -> Result<Config, TError> {
        let evaluator_dir = tempfile::tempdir_in(&self.runner_state.evaluator_dir_root())?;

        {
            let git_client = GitClient::try_default()?;
            git_client.checkout_commit(
                &config.remote.url,
                &config.remote.branch,
                evaluator_dir.path(),
                &config.commit_sha,
            )?;
            // git_client must be dropped here because it is not Send.
            // TODO: Consider offloading to a tokio::task::spawn_blocking.
        }

        tracing::info!(
            "checked out {}/{}@{:?}",
            config.remote.url,
            config.remote.branch,
            config.commit_sha
        );

        let input = evaluator_dir.path().join(&config.input);
        let mut env = HashMap::new();
        env.insert("GIT_BRANCH".to_string(), config.remote.branch.clone());
        env.insert("GIT_COMMIT".to_string(), config.commit_sha.clone());
        Config::from_file(&input, &env)
    }

    async fn complete(
        &self,
        channel_rx: &mut ChannelReceiver,
        config: Config,
    ) -> Result<(), TError> {
        self.channel_tx
            .send(Message::Evaluator(EvaluatorMessage::CompleteRequest {
                runner_id: self.runner_state.runner_mux_handle().runner_id(),
                trigger_id: self.trigger_id,
                config,
            }))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::Evaluator(EvaluatorMessage::CompleteResponse { result }) => {
                    result.map_err(|e| TError::new(&e.to_string()))
                }
                _ => Err(TError::new("Unexpected message type")),
            })
    }

    async fn fail(&self, channel_rx: &mut ChannelReceiver, reason: String) -> Result<(), TError> {
        self.channel_tx
            .send(Message::Evaluator(EvaluatorMessage::FailRequest {
                runner_id: self.runner_state.runner_mux_handle().runner_id(),
                trigger_id: self.trigger_id,
                reason,
            }))
            .await?;

        channel_rx
            .recv()
            .await
            .ok_or_else(|| TError::new("Channel closed"))
            .and_then(|response| match response {
                Message::Evaluator(EvaluatorMessage::FailResponse { result }) => {
                    result.map_err(|e| TError::new(&e.to_string()))
                }
                _ => Err(TError::new("Unexpected message type")),
            })
    }
}
