use std::sync::{Arc, atomic::AtomicU64};

#[tokio::main]
async fn main() {
    let hostname = "localhost:8000";
    let (worker_id, mux_handle) = muzan_worker::connect(hostname).await.unwrap();

    let worker_capacity = Arc::new(AtomicU64::new(16));

    let scheduler_channel_handle = mux_handle.initialize_scheduler_channel(1).await.unwrap();

    let scheduler_task = muzan_worker::scheduler::Scheduler::spawn(
        scheduler_channel_handle,
        worker_capacity.clone(),
    );

    eprintln!("success");

    loop {}
}
