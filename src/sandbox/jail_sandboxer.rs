use std::fs::File;
use std::io::Write;
use std::net::Ipv4Addr;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use crate::image::zfs_image_store::ZfsImageStore;
use crate::image::zfs_image_store::ZfsSnapshot;

use crate::sandbox::NetworkInterface;
use crate::sandbox::Sandbox;
use crate::sandbox::SandboxConfig;
use crate::sandbox::SandboxId;
use crate::sandbox::Sandboxer;
use crate::sandbox::SandboxerError;
use crate::sandbox::jail_config::JailConfig;
use crate::sandbox::jail_sandbox::JailSandbox;
use crate::sandbox::jail_slot::FreeJailSlots;
use crate::sandbox::jail_slot::JailSlotId;

pub type ZfsDatasetQuotaGigabyte = usize;

#[derive(Clone)]
pub struct JailSandboxer {
    sandbox_dir: PathBuf,
    bridge_if: NetworkInterface,
    free_slots: FreeJailSlots,
    image_store: Arc<ZfsImageStore>,
}

impl JailSandboxer {
    pub fn try_new(
        root_dir: &Path,
        bridge_if: NetworkInterface,
        image_store: Arc<ZfsImageStore>,
        num_slots: usize,
    ) -> Result<Self, SandboxerError> {
        if !root_dir.is_dir() {
            let e = format!("root dir [{}] is not a directory", root_dir.display());
            return Err(SandboxerError(e));
        }

        let sandbox_dir = root_dir.join("sandbox.d");
        std::fs::create_dir_all(&root_dir).map_err(|e| SandboxerError(e.to_string()))?;

        // Assert bridge interface is ready to use.
        {
            let output = Command::new("sh")
                .arg("-c")
                .arg(format!("ifconfig {bridge_if}"))
                .output()
                .map_err(|e| SandboxerError(e.to_string()))?;
            if !output.status.success() {
                let e = String::from_utf8_lossy(&output.stderr).to_string();
                return Err(SandboxerError(e));
            }
        }

        // pf rules are assumed to exist for NAT table
        // dummy net pipes are assumed to exist.
        // sysrctl properties are assumed to be set.
        // /boot/loader.conf is assumed to be set.
        // devfs service is assumed to be started.

        let free_slots =
            FreeJailSlots::try_new(num_slots).map_err(|e| SandboxerError(e.to_string()))?;

        let jail_sandboxer = JailSandboxer {
            sandbox_dir,
            image_store,
            bridge_if,
            free_slots,
        };

        Ok(jail_sandboxer)
    }

    fn create_jail(
        &self,
        sandbox_id: SandboxId,
        slot_id: JailSlotId,
        zfs_snapshot: ZfsSnapshot,
        sandbox_dir: PathBuf,
        zfs_quota: ZfsDatasetQuotaGigabyte,
    ) -> Result<JailConfig, SandboxerError> {
        let zfs_dataset = format!(
            "{}/sandbox_rootfs-{sandbox_id}",
            self.image_store.zfs_pool(),
        );
        let jail_conf = self.jail_config(
            sandbox_id,
            &sandbox_dir,
            slot_id,
            zfs_dataset,
            zfs_snapshot,
            zfs_quota,
        );
        let jail_conf_path = sandbox_dir.join("jail.conf");

        // Create jail configuration file.
        {
            let mut file =
                File::create(&jail_conf_path).map_err(|e| SandboxerError(e.to_string()))?;

            file.write_all(jail_conf.to_string().as_bytes())
                .map_err(|e| SandboxerError(e.to_string()))?;

            file.sync_all().map_err(|e| SandboxerError(e.to_string()))?;
        }

        // Create jail.
        {
            let output = Command::new("sh")
                .arg("-c")
                .arg(format!(
                    "jail -c -f {} {}",
                    jail_conf_path.display(),
                    jail_conf.name(),
                ))
                .output()
                .map_err(|e| SandboxerError(e.to_string()))?;
            if !output.status.success() {
                let e = String::from_utf8_lossy(&output.stderr).into_owned();
                Err(SandboxerError(e))?;
            }
        }

        Ok(jail_conf)
    }

    fn jail_config(
        &self,
        sandbox_id: SandboxId,
        sandbox_dir: &Path,
        slot_id: JailSlotId,
        zfs_dataset: String,
        zfs_snapshot: ZfsSnapshot,
        zfs_quota: ZfsDatasetQuotaGigabyte,
    ) -> JailConfig {
        let name = format!("sandbox-{}", sandbox_id.to_string());
        let path = sandbox_dir.join("root");
        let hostname = format!("sandbox-{sandbox_id}.local");
        let epair_interface = format!("epair{slot_id}");
        let epair_host_interface = format!("epair{slot_id}a");
        let epair_jail_interface = format!("epair{slot_id}b");

        // Jail IP=11.0.1.$SLOT_ID
        // Netmask=255.255.0.0
        // Broadcast=11.0.255.255
        // Bridge IP=11.0.0.1
        let ip_addr = Ipv4Addr::new(11, 0, 1, slot_id as u8);

        let exec_console_log = sandbox_dir.join("exec_console_log.txt");

        let exec_prepare = vec![
            format!(
                "zfs clone -o mountpoint={} {zfs_snapshot} {zfs_dataset}",
                path.display()
            ),
            format!("zfs set quota={zfs_quota}G {zfs_dataset}"),
        ];

        let exec_prestart = vec![
            // Create vmnet interface and peer
            format!("ifconfig {epair_interface} create up"),
            // Add vmnet interface peer to bridge
            format!(
                "ifconfig {} addm {} private {} up",
                self.bridge_if, epair_host_interface, epair_host_interface,
            ),
        ];

        let exec_created = vec![];

        let exec_start = vec![
            // Init loopback interface
            format!("/sbin/ifconfig lo0 127.0.0.1 up"),
            // Acquire IP address for vmnet interface
            format!(
                "/sbin/ifconfig {epair_jail_interface} inet {ip_addr} netmask 255.255.0.0 broadcast 11.0.255.255 up"
            ),
            // Start base services
            format!("/bin/sh /etc/rc"),
        ];

        let exec_stop = vec![
            // Stop base services
            format!("/bin/sh /etc/rc.shutdown"),
        ];

        let exec_poststop = vec![
            // Remove vmnet interface peer from bridge
            format!(
                "ifconfig {} deletem {}",
                self.bridge_if, epair_host_interface,
            ),
        ];

        let exec_release = vec![
            // Unmount devfs
            format!("umount {}/dev", path.display()),
            // Unmount and destroy ZFS dataset
            format!("zfs destroy -r {}", zfs_dataset),
        ];

        JailConfig::new(
            name,
            slot_id,
            path,
            epair_jail_interface,
            hostname,
            exec_console_log,
            exec_prepare,
            exec_prestart,
            exec_created,
            exec_start,
            exec_stop,
            exec_poststop,
            exec_release,
        )
    }
}

#[async_trait::async_trait]
impl Sandboxer for JailSandboxer {
    async fn create(&self, config: SandboxConfig) -> Result<Arc<dyn Sandbox>, SandboxerError> {
        let slot = self
            .free_slots
            .reserve()
            .map_err(|e| SandboxerError(e.to_string()))?;

        eprintln!("reserved slot");
        let snapshot = self
            .image_store
            .snapshot(&config.manifest_ref)
            .await
            .map_err(|e| SandboxerError(e.to_string()))?;

        eprintln!("snapshot created");

        let sandbox_dir = self.sandbox_dir.join(&config.sandbox_id.to_string());
        std::fs::create_dir_all(&sandbox_dir).map_err(|e| SandboxerError(e.to_string()))?;

        let zfs_quota = 10;

        let jail_conf = self.create_jail(
            config.sandbox_id,
            slot.slot_id(),
            snapshot,
            sandbox_dir,
            zfs_quota,
        )?;

        eprintln!("jail created");

        let sandbox = JailSandbox::new(jail_conf, slot);

        eprintln!("sandbox created");

        Ok(Arc::new(sandbox))
    }
}
