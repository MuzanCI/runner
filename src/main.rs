use std::collections::HashMap;
use std::sync::Arc;

use muzanci_runner::RunnerState;
use muzanci_runner::capacity::SharedAssignmentCapacity;
use muzanci_runner::capacity::SharedEvaluationCapacity;
use muzanci_runner::sandbox::FakeSandboxer;
use muzanci_runner::scheduler::EvaluatorScheduler;
use muzanci_runner::scheduler::WorkerScheduler;
use muzanci_runner::secret::SecretService;
use muzanci_runner::signal_receiver::SignalReceiver;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    let _guard = muzanci_runner::logging::init().unwrap();

    let cancellation_token = CancellationToken::new();
    let hostname = "localhost:8002";
    let (runner_id, mux_handle) = muzanci_runner::connect(hostname, cancellation_token.clone())
        .await
        .unwrap();
    tracing::info!("Assigned runner ID [{}]", runner_id);

    let evaluation_capacity = SharedEvaluationCapacity::new(10);
    let assignment_capacity = SharedAssignmentCapacity::new(10);

    let secrets_service = Arc::new(SecretService::new(HashMap::new()));
    let sandboxer = Arc::new(FakeSandboxer::new(secrets_service));

    let runner_state = Arc::new(RunnerState::new(
        cancellation_token,
        runner_id,
        mux_handle,
        evaluation_capacity,
        assignment_capacity,
        sandboxer,
    ));

    let evaluator_scheduler_handle = EvaluatorScheduler::spawn(runner_state.clone());
    let worker_scheduler_handle = WorkerScheduler::spawn(runner_state.clone());
    let signal_receiver_handle = SignalReceiver::spawn(runner_state.clone());
    // let debugger_scheduler_handle = DebuggerScheduler::spawn(runner_state.clone());

    // TODO: Add cancellation token for graceful shutdown.
    let _ = tokio::join!(
        evaluator_scheduler_handle,
        worker_scheduler_handle,
        signal_receiver_handle,
        // debugger_scheduler_handle
    );
}
