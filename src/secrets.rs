use std::{collections::HashMap, sync::Arc};

use muzanci_interpreter::Secret;

pub struct SecretsService {
    secrets: Arc<HashMap<String, String>>,
}

impl SecretsService {
    pub fn new(secrets: HashMap<String, String>) -> Self {
        Self {
            secrets: Arc::new(secrets),
        }
    }

    pub async fn resolve(&self, secret: &Secret) -> anyhow::Result<String> {
        self.secrets
            .get(&secret.key)
            .cloned()
            .ok_or(anyhow::anyhow!("Secret not found"))
    }
}
