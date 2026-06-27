use std::sync::Arc;

use muzanci_runner::{
    RunnerState, debugger_scheduler::DebuggerScheduler, evaluator_scheduler::EvaluatorScheduler,
    worker_scheduler::WorkerScheduler,
};
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let cancellation_token = CancellationToken::new();
    let hostname = "localhost:8000";
    let (runner_id, mux_handle) = muzanci_runner::connect(hostname).await.unwrap();
    eprintln!("Runner ID: {}", runner_id);

    let runner_state = Arc::new(RunnerState::new(
        cancellation_token,
        runner_id,
        mux_handle,
        10,
    ));

    let evaluator_scheduler_handle = EvaluatorScheduler::spawn(runner_state.clone());
    let worker_scheduler_handle = WorkerScheduler::spawn(runner_state.clone());
    let debugger_scheduler_handle = DebuggerScheduler::spawn(runner_state.clone());

    // TODO: Add cancellation token for graceful shutdown.
    let _ = tokio::join!(
        evaluator_scheduler_handle,
        worker_scheduler_handle,
        debugger_scheduler_handle
    );
}
