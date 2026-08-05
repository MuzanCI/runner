use muzanci_runner::image::image::ImagePlatform;
use std::path::PathBuf;
use std::sync::Arc;
use uuid::Uuid;

use muzanci_runner::image::manifest_ref::ManifestRef;
use muzanci_runner::image::reqwest_registry_client::ReqwestRegistryClient;
use muzanci_runner::image::zfs_image_store::ZfsImageStore;
use muzanci_runner::image::zfs_image_store::ZfsPool;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Setup test environment.
    let root_dir = PathBuf::from(format!(
        "/tmp/muzanci_runner/rootfs_freebsd_integration_test/{}",
        Uuid::now_v7()
    ));
    std::fs::create_dir_all(&root_dir)?;

    eprintln!("Created test dir [{}]", root_dir.display());

    // Construct ZFS image store.
    let image_store = {
        let registry_client = Arc::new(ReqwestRegistryClient::new());
        let pool = ZfsPool::new("zroot");
        ZfsImageStore::try_new(&root_dir, pool, registry_client)?
    };

    eprintln!("Constructed ZFS image store");

    // Build snapshot from manifest ref.
    let manifest_ref = ManifestRef::try_from("freebsd/freebsd-toolchain:15.0")?;
    let platform = ImagePlatform {
        os: "freebsd".to_string(),
        architecture: "arm64".to_string(),
    };
    let snapshot = image_store.snapshot(&manifest_ref, &platform).await?;

    eprintln!("Built snapshot [{}]", snapshot);

    Ok(())
}
