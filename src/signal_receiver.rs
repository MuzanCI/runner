use std::future::IntoFuture;
use std::sync::Arc;
use tokio::task::JoinHandle;

use tokio::signal::unix::SignalKind;
use tokio::signal::unix::signal;

use crate::RunnerState;

impl IntoFuture for SignalReceiverHandle {
    type Output = <tokio::task::JoinHandle<()> as IntoFuture>::Output;
    type IntoFuture = tokio::task::JoinHandle<()>;

    fn into_future(self) -> Self::IntoFuture {
        self.handle
    }
}

pub struct SignalReceiverHandle {
    handle: JoinHandle<()>,
}

pub struct SignalReceiver {
    runner_state: Arc<RunnerState>,
}

impl SignalReceiver {
    pub fn spawn(runner_state: Arc<RunnerState>) -> SignalReceiverHandle {
        let handle = tokio::spawn(async move {
            SignalReceiver { runner_state }.run().await.unwrap();
        });
        SignalReceiverHandle { handle }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        tracing::info!("[SignalReceiver::run] starting main...");
        let cancellation_token = self.runner_state.cancellation_token();
        tokio::select! {
            _ = cancellation_token.cancelled() => {
                tracing::info!("[SignalReceiver::run] received cancellation signal.");
                Ok(())
            }
            result = self.main() => {
                match result {
                    Ok(_) => {
                        tracing::info!("[SignalReceiver::run] finished main");
                        Ok(())
                    }
                    Err(e) => {
                        tracing::error!("[SignalReceiver::run] main encountered an error: {:?}", e);
                        Err(e)
                    }
                }
            }
        }
    }

    async fn main(&mut self) -> anyhow::Result<()> {
        let cancellation_token = self.runner_state.cancellation_token();

        let mut sigint = match signal(SignalKind::interrupt()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("failed to install SIGINT handler: {e}");
                cancellation_token.cancel();
                anyhow::bail!("failed to install SIGINT handler: {e}");
            }
        };

        let mut sigterm = match signal(SignalKind::terminate()) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("failed to install SIGTERM handler: {e}");
                cancellation_token.cancel();
                anyhow::bail!("failed to install SIGTERM handler: {e}");
            }
        };

        tokio::select! {
            _ = sigint.recv() => {
                tracing::warn!("SIGINT received, shutting down...");
                cancellation_token.cancel();
            }
            _ = sigterm.recv() => {
                tracing::warn!("SIGTERM received, shutting down...");
                cancellation_token.cancel();
            }
        }

        Ok(())
    }
}
