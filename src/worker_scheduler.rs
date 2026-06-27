use std::sync::Arc;

use muzanci_transport::mux::MuxHandle;

pub struct WorkerSchedulerHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for WorkerSchedulerHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // Since JoinHandle is Unpin, we can pin a mutable reference to it directly
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct WorkerScheduler;

impl WorkerScheduler {
    pub fn spawn(mux_handle: Arc<MuxHandle>) -> WorkerSchedulerHandle {
        let handle = tokio::spawn(WorkerScheduler.run());
        WorkerSchedulerHandle { handle }
    }

    pub async fn run(self) {
        unimplemented!("Implement the worker scheduler logic here.");
    }
}
