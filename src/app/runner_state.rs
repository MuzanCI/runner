use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use crate::assignment_capacity::SharedAssignmentCapacityHandle;
use crate::evaluation_capacity::SharedEvaluationCapacity;
use crate::runner_mux::RunnerMuxHandle;
use crate::sandbox::Sandboxer;

#[derive(Clone)]
pub struct RunnerState {
    runner_shutdown: CancellationToken,
    runner_mux_handle: RunnerMuxHandle,
    shared_evaluation_capacity: SharedEvaluationCapacity,
    shared_assignment_capacity_handle: SharedAssignmentCapacityHandle,
    sandboxer: Arc<dyn Sandboxer>,
    evaluator_dir_root: PathBuf,
}

impl RunnerState {
    pub fn new(
        runner_shutdown: CancellationToken,
        runner_mux_handle: RunnerMuxHandle,
        shared_evaluation_capacity: SharedEvaluationCapacity,
        shared_assignment_capacity_handle: SharedAssignmentCapacityHandle,
        sandboxer: Arc<dyn Sandboxer>,
        evaluator_dir_root: PathBuf,
    ) -> Self {
        Self {
            runner_shutdown,
            runner_mux_handle,
            shared_evaluation_capacity,
            shared_assignment_capacity_handle,
            sandboxer,
            evaluator_dir_root,
        }
    }

    pub fn runner_shutdown(&self) -> CancellationToken {
        self.runner_shutdown.clone()
    }

    pub fn runner_mux_handle(&self) -> RunnerMuxHandle {
        self.runner_mux_handle.clone()
    }

    pub fn sandboxer(&self) -> Arc<dyn Sandboxer> {
        self.sandboxer.clone()
    }

    pub fn assignment_capacity(&self) -> SharedAssignmentCapacityHandle {
        self.shared_assignment_capacity_handle.clone()
    }

    pub fn evaluation_capacity(&self) -> SharedEvaluationCapacity {
        self.shared_evaluation_capacity.clone()
    }

    pub fn evaluator_dir_root(&self) -> &Path {
        &self.evaluator_dir_root
    }
}
