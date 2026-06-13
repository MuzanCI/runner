use std::sync::{Arc, atomic::AtomicU64};

use muzanci_transport::channel::ChannelType;

#[tokio::main]
async fn main() {
    let hostname = "localhost:8000";
    let (runner_id, mux_handle) = muzanci_runner::connect(hostname).await.unwrap();
    eprintln!("Runner ID: {}", runner_id);

    let runner_capacity = Arc::new(AtomicU64::new(16));

    let scheduler_channel_handle = mux_handle
        .open_channel(ChannelType::Scheduler, 64)
        .await
        .unwrap();

    let scheduler_task = muzanci_runner::scheduler::Scheduler::spawn(
        scheduler_channel_handle,
        runner_id,
        runner_capacity.clone(),
        mux_handle.clone(),
    );

    scheduler_task.join().await.unwrap();
}
