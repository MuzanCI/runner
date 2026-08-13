use std::sync::Arc;

use crate::sandbox::Sandbox;
use crate::sandbox::SandboxConfig;
use crate::sandbox::Sandboxer;
use crate::sandbox::SandboxerError;
use crate::sandbox::fake_sandbox::FakeSandbox;

pub struct FakeSandboxer;

impl FakeSandboxer {
    pub fn new() -> Self {
        Self
    }
}

#[async_trait::async_trait]
impl Sandboxer for FakeSandboxer {
    async fn create(&self, config: SandboxConfig) -> Result<Arc<dyn Sandbox>, SandboxerError> {
        let sandbox = FakeSandbox::new(config);
        Ok(Arc::new(sandbox))
    }

    fn destroy(&self, sandbox: Arc<dyn Sandbox>) -> Result<(), SandboxerError> {
        drop(sandbox);
        Ok(())
    }
}
