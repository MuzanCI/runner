use std::sync::Arc;
use tokio::signal::unix::SignalKind;
use tokio::signal::unix::signal;
use tokio::task::AbortHandle;

use muzanci_observability::TError;
use tokio::task::JoinSet;
use uuid::Uuid;

use crate::app::runner_state::RunnerState;

#[derive(thiserror::Error, Debug)]
pub enum SignalReceiverError {
    #[error("failed to install SIGINT handler: {0}")]
    SigintInstallFailed(#[source] std::io::Error),

    #[error("failed to install SIGTERM handler: {0}")]
    SigtermInstallFailed(#[source] std::io::Error),
}

pub struct SignalReceiver {
    runner_state: Arc<RunnerState>,
}

impl SignalReceiver {
    pub fn spawn(
        join_set: &mut JoinSet<Result<(), TError>>,
        runner_state: Arc<RunnerState>,
    ) -> AbortHandle {
        join_set.spawn(async move { SignalReceiver { runner_state }.main().await })
    }

    #[tracing::instrument(
        skip_all,
        name = "signal_receiver"
        fields(
            id = Uuid::now_v7().to_string(),
            runner_id = self.runner_state.runner_mux_handle().runner_id().to_string(),
        )
    )]
    async fn main(mut self) -> Result<(), TError> {
        tracing::info!("signal_receiver.started");

        let runner_shutdown = self.runner_state.runner_shutdown();

        tokio::select! {
            _ = runner_shutdown.cancelled() => {
                tracing::info!("SignalReceiver received runner shutdown signal.");
            }
            result = self.run() => {
                result?
            }
        }

        tracing::info!("signal_receiver.finished");

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    async fn run(&mut self) -> Result<(), TError> {
        let runner_shutdown = self.runner_state.runner_shutdown();

        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                runner_shutdown.cancel();
                let e = SignalReceiverError::SigintInstallFailed(e);
                return Err(e.into());
            }
        };

        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                runner_shutdown.cancel();
                let e = SignalReceiverError::SigtermInstallFailed(e);
                return Err(e.into());
            }
        };

        tokio::select! {
            _ = sigint.recv() => {
                tracing::warn!("SIGINT received, shutting down runner...");
                runner_shutdown.cancel();
            }
            _ = sigterm.recv() => {
                tracing::warn!("SIGTERM received, shutting down runner...");
                runner_shutdown.cancel();
            }
        }

        Ok(())
    }
}
