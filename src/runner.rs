use std::sync::{Arc, atomic::AtomicU64};

use muzanci_transport::channel::{ChannelHandle, Message, RunnerEvent};

pub struct RunnerHandle {}

pub struct Runner {
    /// A channel for communicating with the server.
    channel_handle: ChannelHandle,

    /// Capacity units of the worker.
    worker_capacity: Arc<AtomicU64>,
}

impl Runner {
    /// Spawns a new [`Runner`] task and returns a [`RunnerHandle`].
    pub fn spawn(
        worker_id: u64,
        channel_handle: ChannelHandle,
        worker_capacity: Arc<AtomicU64>,
    ) -> RunnerHandle {
        let runner = Runner {
            channel_handle,
            worker_capacity,
        };
        tokio::spawn(runner.run(worker_id));
        RunnerHandle {}
    }

    async fn run(mut self, worker_id: u64) {
        self.channel_handle
            .send(Message::InitializeRunnerRequest {
                worker_id: worker_id,
            })
            .await
            .unwrap();

        let runner_config = match self.channel_handle.recv().await.unwrap() {
            Message::InitializeRunnerResponse(Ok(runner_config)) => {
                println!("Received runner config: {:?}", runner_config);
                runner_config
            }
            Message::InitializeRunnerResponse(Err(err)) => {
                panic!("Failed to initialize runner: {}", err);
            }
            msg => {
                panic!("Expected InitializeRunnerResponse. Got {:?}", msg);
            }
        };

        self.worker_capacity.fetch_sub(
            runner_config.worker_capacity(),
            std::sync::atomic::Ordering::SeqCst,
        );

        // TODO: Use trait object for async process execution.
        tokio::process::Command::new("echo")
            .arg("Creating ZFS dataset...")
            .spawn()
            .unwrap();

        tokio::process::Command::new("echo")
            .arg("Creating jail...")
            .spawn()
            .unwrap();

        tokio::process::Command::new("echo")
            .arg(format!(
                "Cloning repo {}/{} at commit {}...",
                runner_config.repo_owner(),
                runner_config.repo_name(),
                runner_config.commit_sha()
            ))
            .spawn()
            .unwrap();

        self.channel_handle
            .send(Message::RunnerEvent(RunnerEvent::Started {
                runner_id: runner_config.runner_id(),
            }))
            .await
            .unwrap();

        self.channel_handle
            .send(Message::RunnerEvent(RunnerEvent::Exited {
                runner_id: runner_config.runner_id(),
                exit_code: 0,
            }))
            .await
            .unwrap();

        tokio::process::Command::new("echo")
            .arg("Destroying jail...")
            .spawn()
            .unwrap();
    }
}
