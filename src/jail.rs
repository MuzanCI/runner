use std::{collections::HashMap, path::Path};

use tokio::process::{Child, Command};

#[async_trait::async_trait]
pub trait Jailer
where
    Self: Send + Sync,
{
    fn create(&self) -> anyhow::Result<Box<dyn Jail>>;
}

#[async_trait::async_trait]
pub trait Jail
where
    Self: Send + Sync,
{
    fn spawn(&self, cmd_str: &str) -> anyhow::Result<Child>;
    fn read_file(&self, path: &Path) -> anyhow::Result<String>;
}

pub struct FakeJailer;

impl Jailer for FakeJailer {
    fn create(&self) -> anyhow::Result<Box<dyn Jail>> {
        Ok(Box::new(FakeJail))
    }
}

pub struct FakeJail;

pub struct JailCommand {
    command: String,
    args: Vec<String>,
    env: HashMap<String, String>,
}

impl Jail for FakeJail {
    fn spawn(&self, cmd_str: &str) -> anyhow::Result<Child> {
        Ok(Command::new("sh").arg("-c").arg(cmd_str).spawn()?)
    }

    fn read_file(&self, path: &Path) -> anyhow::Result<String> {
        Ok(std::fs::read_to_string(path)?)
    }
}
