use std::sync::{Arc, atomic::AtomicU64};

use muzanci_transport::channel::ChannelHandle;

// Scheduler Task
// Loop
//     Wait until capacityFreed signal.
//     Worker long-poll queries available jobs from Server.
//     Server returns filtered available jobs.
//     For each available job:
//         If Worker has local capacity:
//             Worker attempts to acquire Job.
//             If Job is acquired:
//                 Update capacity.
//                 Spawn a Runner for the Job.
pub struct SchedulerHandle {}

pub struct Scheduler {
    channel_handle: ChannelHandle,
    worker_capacity: Arc<AtomicU64>,
}
impl Scheduler {
    pub fn spawn(
        channel_handle: ChannelHandle,
        worker_capacity: Arc<AtomicU64>,
    ) -> SchedulerHandle {
        let scheduler = Scheduler {
            channel_handle,
            worker_capacity,
        };
        tokio::spawn(scheduler.run());
        SchedulerHandle {}
    }

    async fn run(mut self) {
        loop {
            unimplemented!();
        }
    }
}
