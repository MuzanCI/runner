use std::path::Path;
use std::path::PathBuf;

use crate::sandbox::NetworkInterface;
use muzanci_image::image::ImagePlatformOs;

pub type Execs = Vec<String>;

pub struct JailConfig {
    platform_os: ImagePlatformOs,
    name: String,
    jid: usize,
    path: PathBuf,
    vnet_interface: NetworkInterface,
    hostname: String,
    exec_console_log: PathBuf,
    exec_prepare: Execs,
    exec_prestart: Execs,
    exec_created: Execs,
    exec_start: Execs,
    exec_stop: Execs,
    exec_poststop: Execs,
    exec_release: Execs,
}

impl JailConfig {
    pub fn new(
        platform_os: ImagePlatformOs,
        name: String,
        jid: usize,
        path: PathBuf,
        vnet_interface: NetworkInterface,
        hostname: String,
        exec_console_log: PathBuf,
        exec_prepare: Execs,
        exec_prestart: Execs,
        exec_created: Execs,
        exec_start: Execs,
        exec_stop: Execs,
        exec_poststop: Execs,
        exec_release: Execs,
    ) -> Self {
        Self {
            platform_os,
            name,
            jid,
            path,
            vnet_interface,
            hostname,
            exec_console_log,
            exec_prepare,
            exec_prestart,
            exec_created,
            exec_start,
            exec_stop,
            exec_poststop,
            exec_release,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl std::fmt::Display for JailConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "{} {{", self.name)?;

        // Jail ID
        writeln!(f, "jid = {};", self.jid)?;

        // Persist jail for multiple sequential `jexec`
        f.write_str("persist;\n")?;

        // Network stack
        f.write_str("vnet;\n")?;
        writeln!(f, "vnet.interface = \"{}\";", self.vnet_interface)?;
        writeln!(f, "host.hostname = \"{}\";", self.hostname)?;

        // Root filesystem path
        writeln!(f, "path = \"{}\";", self.path.display())?;
        f.write_str("enforce_statfs = 2;\n")?;

        // DevFS
        f.write_str("mount.devfs;\n")?;
        f.write_str("mount.fdescfs;\n")?;
        f.write_str("devfs_ruleset = 100;\n")?;

        // Permissions
        f.write_str("securelevel = 1;\n")?;
        f.write_str("children.max = 10;\n")?;
        f.write_str("allow.suser = 1;\n")?;
        f.write_str("allow.reserved_ports = 1;\n")?;
        f.write_str("allow.quotas = 1;\n")?;
        f.write_str("allow.set_hostname = 0;\n")?;
        f.write_str("allow.sysvipc = 0;\n")?;
        f.write_str("allow.raw_sockets = 0;\n")?;
        f.write_str("allow.chflags = 0;\n")?;
        f.write_str("allow.mount = 0;\n")?;
        f.write_str("allow.mount.devfs = 0;\n")?;
        f.write_str("allow.read_msgbuf = 0;\n")?;
        f.write_str("allow.socket_af = 0;\n")?;
        f.write_str("allow.mlock = 0;\n")?;
        f.write_str("allow.nfsd = 0;\n")?;
        f.write_str("allow.unprivileged_parent_tampering = 0;\n")?;
        f.write_str("allow.unprivileged_proc_debug = 0;\n")?;
        f.write_str("allow.extattr = 0;\n")?;
        f.write_str("allow.adjtime = 0;\n")?;
        f.write_str("allow.settime = 0;\n")?;
        f.write_str("allow.routing = 0;\n")?;
        f.write_str("allow.setaudit = 0;\n")?;

        match self.platform_os {
            ImagePlatformOs::LINUX => {
                f.write_str("linux = new;\n")?;
                f.write_str("mount += \"devfs ${path}/compat/linux/dev devfs rw 0 0\";\n")?;
                f.write_str(
                    "mount += \"tmpfs ${path}/compat/linux/dev/shm tmpfs rw,size=1g,mode=1777 0 0\";\n",
                )?;
                f.write_str(
                    "mount += \"fdescfs ${path}/compat/linux/dev/fd fdescfs rw,linrdlnk 0 0\";\n",
                )?;
                f.write_str(
                    "mount += \"linprocfs ${path}/compat/linux/proc linprocfs rw 0 0\";\n",
                )?;
                f.write_str("mount += \"linsysfs ${path}/compat/linux/sys linsysfs rw 0 0\";\n")?;
                f.write_str("mount += \"/tmp ${path}/compat/linux/tmp nullfs rw 0 0\";\n")?;
                f.write_str("mount += \"/home ${path}/compat/linux/home nullfs rw 0 0\";\n")?;
            }
            ImagePlatformOs::FREEBSD => {}
            ImagePlatformOs::OTHER(_) => {}
        }

        // Exec psudo-parameters
        f.write_str("exec.clean;\n")?;

        writeln!(
            f,
            "exec.consolelog = \"{}\";",
            self.exec_console_log.display(),
        )?;

        f.write_str("exec.prepare = \"echo exec.prepare\";\n")?;
        for exec in &self.exec_prepare {
            writeln!(f, "exec.prepare += \"{}\";", exec)?;
        }

        f.write_str("exec.prestart = \"echo exec.prestart\";\n")?;
        for exec in &self.exec_prestart {
            writeln!(f, "exec.prestart += \"{}\";", exec)?;
        }

        f.write_str("exec.created = \"echo exec.created\";\n")?;
        for exec in &self.exec_created {
            writeln!(f, "exec.created += \"{}\";", exec)?;
        }

        f.write_str("exec.start = \"/bin/echo exec.start\";\n")?;
        for exec in &self.exec_start {
            writeln!(f, "exec.start += \"{}\";", exec)?;
        }

        f.write_str("exec.stop = \"/bin/echo exec.stop\";\n")?;
        for exec in &self.exec_stop {
            writeln!(f, "exec.stop += \"{}\";", exec)?;
        }

        f.write_str("exec.poststop = \"echo exec.poststop\";\n")?;
        for exec in &self.exec_poststop {
            writeln!(f, "exec.poststop += \"{}\";", exec)?;
        }

        f.write_str("exec.release = \"echo exec.release\";\n")?;
        for exec in &self.exec_release {
            writeln!(f, "exec.release += \"{}\";", exec)?;
        }

        f.write_str("}\n")?;

        Ok(())
    }
}
