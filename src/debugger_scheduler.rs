use std::sync::Arc;

use muzanci_transport::mux::MuxHandle;

pub struct DebuggerSchedulerHandle {
    handle: tokio::task::JoinHandle<()>,
}

impl Future for DebuggerSchedulerHandle {
    type Output = Result<(), tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        // Since JoinHandle is Unpin, we can pin a mutable reference to it directly
        std::pin::Pin::new(&mut self.handle).poll(cx)
    }
}

pub struct DebuggerScheduler;

impl DebuggerScheduler {
    pub fn spawn(mux_handle: Arc<MuxHandle>) -> DebuggerSchedulerHandle {
        let handle = tokio::spawn(DebuggerScheduler.run());
        DebuggerSchedulerHandle { handle }
    }

    pub async fn run(self) {
        unimplemented!("Implement the debugger scheduler logic here.");
    }
}
