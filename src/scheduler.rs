use std::sync::{Arc, atomic::AtomicU64};

use muzanci_transport::{
    channel::{ChannelHandle, ChannelType, Message},
    mux,
    runner::RunnerId,
};
use tokio::task::JoinError;

use crate::worker::Worker;

// Scheduler Task
// Loop
//     Wait until capacityFreed signal.
//     Runner long-poll queries available jobs from Server.
//     Server returns filtered available jobs.
//     For each available job:
//         If Runner has local capacity:
//             Runner attempts to acquire Job.
//             If Job is acquired:
//                 Update capacity.
//                 Spawn a Runner for the Job.
pub struct SchedulerHandle {
    task_handle: tokio::task::JoinHandle<()>,
}

impl SchedulerHandle {
    pub async fn join(self) -> Result<(), JoinError> {
        self.task_handle.await
    }
}

pub struct Scheduler {
    channel_handle: ChannelHandle,
    runner_id: RunnerId,
    runner_capacity: Arc<AtomicU64>,
    mux_handle: mux::MuxHandle,
}

impl Scheduler {
    pub fn spawn(
        channel_handle: ChannelHandle,
        runner_id: RunnerId,
        runner_capacity: Arc<AtomicU64>,
        mux_handle: mux::MuxHandle,
    ) -> SchedulerHandle {
        let scheduler = Scheduler {
            channel_handle,
            runner_id,
            runner_capacity,
            mux_handle,
        };
        let task_handle = tokio::spawn(scheduler.run());
        SchedulerHandle { task_handle }
    }

    async fn run(mut self) {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            // TODO: Replace with capacity_freed signal.

            let runner_capacity = self
                .runner_capacity
                .load(std::sync::atomic::Ordering::SeqCst);
            if runner_capacity == 0 {
                eprintln!("Runner has no capacity. Skipping job query.");
                continue;
            }

            self.channel_handle
                .send(Message::QueryAvailableJobsRequest)
                .await
                .unwrap();
            let available_jobs = match self.channel_handle.recv().await {
                Some(Message::QueryAvailableJobsResponse { available_jobs }) => available_jobs,
                Some(message) => {
                    eprintln!(
                        "Expected QueryAvailableJobsResponse message. Got: {:?}",
                        message
                    );
                    break;
                }
                None => {
                    eprintln!("Scheduler channel closed");
                    break;
                }
            };

            for available_job in available_jobs {
                eprintln!("Processing available job: {:?}", available_job);
                let runner_capacity = self
                    .runner_capacity
                    .load(std::sync::atomic::Ordering::SeqCst);
                if runner_capacity < available_job.runner_capacity_required() {
                    eprintln!("Not enough capacity for job: {:?}", available_job);
                    continue;
                }

                self.channel_handle
                    .send(Message::AcquireJobRequest {
                        job_id: available_job.job_id(),
                    })
                    .await
                    .unwrap();

                let (job_id, result) = match self.channel_handle.recv().await {
                    Some(Message::AcquireJobResponse { job_id, result }) => {
                        if job_id != available_job.job_id() {
                            eprintln!(
                                "Received AcquireJobResponse for unexpected job_id: {:?}",
                                job_id
                            );
                            continue;
                        }
                        (job_id, result)
                    }
                    Some(message) => {
                        eprintln!("Expected AcquireJobResponse message. Got: {:?}", message);
                        continue;
                    }
                    None => {
                        eprintln!("Scheduler channel closed");
                        break;
                    }
                };

                match result {
                    Ok(worker_id) => {
                        eprintln!("Successfully acquired job: {:?}", job_id);
                        self.runner_capacity.fetch_sub(
                            available_job.runner_capacity_required(),
                            std::sync::atomic::Ordering::SeqCst,
                        );
                        // Spawn worker
                        let worker_channel_handle = {
                            self.mux_handle
                                .open_channel(ChannelType::Worker, 64)
                                .await
                                .unwrap()
                        };

                        Worker::spawn(
                            worker_id,
                            worker_channel_handle,
                            self.runner_capacity.clone(),
                        );
                    }
                    Err(err) => {
                        eprintln!("Failed to acquire job {:?}: {}", job_id, err);
                    }
                }
            }
        }
    }
}
