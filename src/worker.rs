use std::sync::{Arc, atomic::AtomicU64};

use muzanci_transport::{
    channel::{ChannelHandle, Message},
    job::JobId,
    worker::{WorkerEvent, WorkerId},
};

pub struct WorkerHandle {}

pub struct Worker {
    /// A channel for communicating with the server.
    channel_handle: ChannelHandle,

    /// Capacity units of the worker.
    worker_capacity: Arc<AtomicU64>,
}

impl Worker {
    /// Spawns a new [`Worker`] task and returns a [`WorkerHandle`].
    pub fn spawn(
        worker_id: WorkerId,
        channel_handle: ChannelHandle,
        worker_capacity: Arc<AtomicU64>,
    ) -> WorkerHandle {
        let worker = Worker {
            channel_handle,
            worker_capacity,
        };
        tokio::spawn(worker.run(worker_id));
        WorkerHandle {}
    }

    async fn run(mut self, worker_id: WorkerId) {
        self.channel_handle
            .send(Message::WorkerConfigRequest { worker_id })
            .await
            .unwrap();

        let worker_config = match self.channel_handle.recv().await.unwrap() {
            Message::WorkerConfigResponse(Ok(worker_config)) => {
                println!("Received worker config: {:?}", worker_config);
                worker_config
            }
            Message::WorkerConfigResponse(Err(err)) => {
                panic!("Failed to initialize worker: {}", err);
            }
            msg => {
                panic!("Expected WorkerConfigResponse. Got {:?}", msg);
            }
        };

        self.worker_capacity.fetch_sub(
            worker_config.worker_capacity(),
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
                worker_config.repo_owner(),
                worker_config.repo_name(),
                worker_config.commit_sha()
            ))
            .spawn()
            .unwrap();

        self.channel_handle
            .send(Message::WorkerEvent(WorkerEvent::Started))
            .await
            .unwrap();

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        self.channel_handle
            .send(Message::WorkerEvent(WorkerEvent::Completed))
            .await
            .unwrap();

        tokio::process::Command::new("echo")
            .arg("Destroying jail...")
            .spawn()
            .unwrap();
    }
}
