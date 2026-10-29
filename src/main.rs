use muzanci_observability::TracingConfig;
use muzanci_runner::runner_mux::RunnerMuxHandle;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use muzanci_image::reqwest_registry_client::ReqwestRegistryClient;
use muzanci_runner::app::runner_state::RunnerState;
use muzanci_runner::assignment_capacity::SharedAssignmentCapacity;
use muzanci_runner::channel::debugger_scheduler::DebuggerScheduler;
use muzanci_runner::channel::evaluator_scheduler::EvaluatorScheduler;
use muzanci_runner::channel::worker_scheduler::WorkerScheduler;
use muzanci_runner::evaluation_capacity::SharedEvaluationCapacity;
use muzanci_runner::sandbox::fake_sandboxer::FakeSandboxer;
use muzanci_runner::sandbox::jail_sandboxer::JailSandboxer;
use muzanci_runner::sandbox::zfs_image_store::ZfsImageStore;
use muzanci_runner::sandbox::zfs_image_store::ZfsPool;
use muzanci_runner::signal_receiver::SignalReceiver;

#[tokio::main]
async fn main() {
    muzanci_observability::init_tracing(TracingConfig::default()).unwrap();

    // Configuration
    let server_hostname = std::env::var("MUZANCI_SERVER_HOSTNAME_AND_PORT")
        .expect("MUZANCI_SERVER_HOSTNAME_AND_PORT is not set");
    let secret = std::env::var("MUZANCI_PERSONAL_ACCESS_TOKEN_SECRET")
        .expect("MUZANCI_PERSONAL_ACCESS_TOKEN_SECRET is not set");
    let account_id = {
        let account_id_str =
            std::env::var("MUZANCI_ACCOUNT_ID").expect("MUZANCI_ACCOUNT_ID is not set");
        Uuid::try_parse(&account_id_str).unwrap().into()
    };

    // Runner registration
    let runner_shutdown = CancellationToken::new();
    let runner_mux_handle = RunnerMuxHandle::new(
        runner_shutdown.clone(),
        &server_hostname,
        &secret,
        account_id,
    )
    .await
    .unwrap();

    tracing::info!(
        runner_id = runner_mux_handle.runner_id().to_string(),
        "runner.registered"
    );

    let evaluation_capacity = SharedEvaluationCapacity::new(10);
    let assignment_capacity = SharedAssignmentCapacity::spawn(10);

    // let sandboxer = {
    //     let zfs_pool = ZfsPool::new("zroot");
    //     let registry_client = Arc::new(ReqwestRegistryClient::new());
    //     let root_dir = PathBuf::from("/tmp/runner");
    //     let image_store =
    //         Arc::new(ZfsImageStore::try_new(&root_dir, zfs_pool, registry_client).unwrap());
    //     let bridge_if = "bridge0".to_string();
    //     let num_slots = 10;
    //     Arc::new(JailSandboxer::try_new(&root_dir, bridge_if, image_store, num_slots).unwrap())
    // };
    let sandboxer = Arc::new(FakeSandboxer::new());

    let evaluator_dir_root = PathBuf::from("/tmp/evaluators");

    let runner_state = Arc::new(RunnerState::new(
        runner_shutdown,
        runner_mux_handle,
        evaluation_capacity,
        assignment_capacity,
        sandboxer,
        evaluator_dir_root,
    ));

    let mut join_set = JoinSet::new();

    SignalReceiver::spawn(&mut join_set, runner_state.clone());
    EvaluatorScheduler::spawn(&mut join_set, runner_state.clone());
    WorkerScheduler::spawn(&mut join_set, runner_state.clone());
    DebuggerScheduler::spawn(&mut join_set, runner_state.clone());

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                tracing::error!("{e}");
            }
            Err(e) => {
                tracing::error!("Join error: {e:?}");
            }
        }
    }
}
