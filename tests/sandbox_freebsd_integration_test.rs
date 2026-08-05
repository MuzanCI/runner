use std::path::PathBuf;
use std::sync::Arc;

use muzanci_runner::image::image::ImagePlatform;
use uuid::Uuid;

use muzanci_runner::image::manifest_ref::ManifestRef;
use muzanci_runner::image::reqwest_registry_client::ReqwestRegistryClient;
use muzanci_runner::image::zfs_image_store::ZfsImageStore;
use muzanci_runner::image::zfs_image_store::ZfsPool;
use muzanci_runner::sandbox::SandboxConfig;
use muzanci_runner::sandbox::Sandboxer;
use muzanci_runner::sandbox::jail_sandboxer::JailSandboxer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let root_dir = PathBuf::from(format!(
        "/tmp/muzanci_runner/sandbox_freebsd_integration_test/{}",
        Uuid::now_v7(),
    ));
    std::fs::create_dir_all(&root_dir)?;

    let bridge_if = "bridge0".to_string();

    let image_store = {
        let zfs_pool = ZfsPool::new("zroot");
        let registry_client = Arc::new(ReqwestRegistryClient::new());
        let image_store = ZfsImageStore::try_new(&root_dir, zfs_pool, registry_client)?;
        Arc::new(image_store)
    };

    let num_slots = 10;

    let sandboxer = JailSandboxer::try_new(&root_dir, bridge_if, image_store, num_slots)?;

    eprintln!("Created jail sandboxer");

    let manifest_ref = ManifestRef::try_from("freebsd/freebsd-toolchain:15.0")?;
    let platform = ImagePlatform {
        os: "freebsd".to_string(),
        architecture: "arm64".to_string(),
    };
    let sandbox_config = SandboxConfig {
        sandbox_id: Uuid::now_v7(),
        manifest_ref,
        platform,
    };

    let sandbox = sandboxer.create(sandbox_config).await?;

    eprintln!("created sandbox");

    sandboxer.destroy(sandbox)?;

    eprintln!("destroyed sandbox");

    Ok(())
}
