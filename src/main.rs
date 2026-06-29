use std::sync::Arc;

use muzanci_runner::{
    RunnerState,
    capacity::{SharedAssignmentCapacity, SharedEvaluationCapacity},
    jail::FakeJailer,
    scheduler::{EvaluatorScheduler, WorkerScheduler},
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let _guard = muzanci_runner::logging::init().unwrap();

    let cancellation_token = CancellationToken::new();
    let hostname = "localhost:8002";
    let (runner_id, mux_handle) = muzanci_runner::connect(hostname).await.unwrap();
    tracing::info!("Assigned runner ID [{}]", runner_id);

    let evaluation_capacity = SharedEvaluationCapacity::new(10);
    let assignment_capacity = SharedAssignmentCapacity::new(10);

    let jailer = Arc::new(FakeJailer);

    let runner_state = Arc::new(RunnerState::new(
        cancellation_token,
        runner_id,
        mux_handle,
        evaluation_capacity,
        assignment_capacity,
        jailer,
    ));

    let evaluator_scheduler_handle = EvaluatorScheduler::spawn(runner_state.clone());
    let worker_scheduler_handle = WorkerScheduler::spawn(runner_state.clone());
    // let debugger_scheduler_handle = DebuggerScheduler::spawn(runner_state.clone());

    // TODO: Add cancellation token for graceful shutdown.
    let _ = tokio::join!(
        evaluator_scheduler_handle,
        worker_scheduler_handle,
        // debugger_scheduler_handle
    );
}
