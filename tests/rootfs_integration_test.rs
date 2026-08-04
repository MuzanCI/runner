use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

use muzanci_runner::image::manifest_ref::ManifestRef;
use muzanci_runner::image::reqwest_registry_client::ReqwestRegistryClient;
use muzanci_runner::image::zfs_image_store::ZfsImageStore;
use muzanci_runner::image::zfs_image_store::ZfsPool;

#[test]
fn integration_test() -> anyhow::Result<()> {
    tokio::runtime::Runtime::new()?.block_on(async {
        // Setup test environment.
        let root_dir = PathBuf::from(format!(
            "/tmp/muzanci_image/integration_test/{}",
            Uuid::now_v7()
        ));
        std::fs::create_dir_all(&root_dir)?;

        // Construct ZFS image store.
        let image_store = {
            let registry_client = Arc::new(ReqwestRegistryClient::new());
            let pool = ZfsPool::new("zroot");
            ZfsImageStore::try_new(&root_dir, pool, registry_client)?
        };
        // Build snapshot from manifest ref.
        let manifest_ref = ManifestRef::try_from("alpine:latest")?;
        let _snapshot = image_store.snapshot(&manifest_ref).await?;
        Ok(())
    })
}
