use std::{collections::HashMap, path::Path, sync::Arc};

use tokio::process::{Child, Command};

#[async_trait::async_trait]
pub trait Sandboxer
where
    Self: Send + Sync,
{
    fn create(&self) -> anyhow::Result<Arc<dyn Sandbox>>;
}

#[async_trait::async_trait]
pub trait Sandbox
where
    Self: Send + Sync,
{
    fn spawn(&self, cmd_str: &str) -> anyhow::Result<Child>;
    fn read_file(&self, path: &Path) -> anyhow::Result<String>;
    fn add_secret(&self, name: &str, value: &str) -> anyhow::Result<()>;
    fn clear_secrets(&self) -> anyhow::Result<()>;
}

pub struct FakeSandboxer;

impl Sandboxer for FakeSandboxer {
    fn create(&self) -> anyhow::Result<Arc<dyn Sandbox>> {
        // create temporary filesystem.
        Ok(Arc::new(FakeSandbox))
    }
}

pub struct FakeSandbox;

pub struct SandboxCommand {
    command: String,
    args: Vec<String>,
    env: HashMap<String, String>,
}

impl Sandbox for FakeSandbox {
    fn spawn(&self, cmd_str: &str) -> anyhow::Result<Child> {
        // Create tmp directory.
        Ok(Command::new("sh").arg("-c").arg(cmd_str).spawn()?)
    }

    fn read_file(&self, path: &Path) -> anyhow::Result<String> {
        Ok(std::fs::read_to_string(path)?)
    }

    fn add_secret(&self, name: &str, value: &str) -> anyhow::Result<()> {
        anyhow::bail!("FakeSandbox::add_secret is not implemented");
    }

    fn clear_secrets(&self) -> anyhow::Result<()> {
        anyhow::bail!("FakeSandbox::clear_secrets is not implemented");
    }
}
