use std::{collections::HashMap, sync::Arc};

use muzanci_interpreter::Secret;

pub struct SecretService {
    secrets: Arc<HashMap<String, String>>,
}

impl SecretService {
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
