use std::path::Path;
use std::path::PathBuf;

use crate::sandbox::NetworkInterface;

pub type Execs = Vec<String>;

pub struct JailConfig {
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

impl ToString for JailConfig {
    fn to_string(&self) -> String {
        let mut s = String::new();

        s.push_str(&format!("{} {{\n", self.name));

        // Jail ID
        s.push_str(&format!("jid = {};\n", self.jid));

        // Persist jail for multiple sequential `jexec`
        s.push_str("persist;\n");

        // Network stack
        s.push_str("vnet;\n");
        s.push_str(&format!("vnet.interface = \"{}\";\n", self.vnet_interface));
        s.push_str(&format!("host.hostname = \"{}\";\n", self.hostname));

        // Root filesystem path
        s.push_str(&format!("path = \"{}\";\n", self.path.display()));
        s.push_str("enforce_statfs = 2;\n");

        // DevFS
        s.push_str("mount.devfs;\n");
        s.push_str("devfs_ruleset = 100;\n");

        // Permissions
        s.push_str("securelevel = 1;\n");
        s.push_str("children.max = 10;\n");
        s.push_str("allow.set_hostname = 0;\n");
        s.push_str("allow.sysvipc = 0;\n");
        s.push_str("allow.raw_sockets = 0;\n");
        s.push_str("allow.chflags = 0;\n");
        s.push_str("allow.mount = 0;\n");
        s.push_str("allow.mount.devfs = 0;\n");
        s.push_str("allow.quotas = 1;\n");
        s.push_str("allow.read_msgbuf = 0;\n");
        s.push_str("allow.socket_af = 0;\n");
        s.push_str("allow.mlock = 0;\n");
        s.push_str("allow.nfsd = 0;\n");
        s.push_str("allow.reserved_ports = 1;\n");
        s.push_str("allow.unprivileged_parent_tampering = 0;\n");
        s.push_str("allow.unprivileged_proc_debug = 0;\n");
        s.push_str("allow.suser = 0;\n");
        s.push_str("allow.extattr = 0;\n");
        s.push_str("allow.adjtime = 0;\n");
        s.push_str("allow.settime = 0;\n");
        s.push_str("allow.routing = 0;\n");
        s.push_str("allow.setaudit = 0;\n");

        // Exec psudo-parameters
        s.push_str(&format!(
            "exec.consolelog = \"{}\";\n",
            self.exec_console_log.display()
        ));
        s.push_str("exec.prepare = \"echo exec.prepare\";\n");
        for exec in &self.exec_prepare {
            s.push_str(&format!("exec.prepare += \"{}\";\n", exec))
        }

        s.push_str("exec.prestart = \"echo exec.prestart\";\n");
        for exec in &self.exec_prestart {
            s.push_str(&format!("exec.prestart += \"{}\";\n", exec))
        }

        s.push_str("exec.created = \"echo exec.created\";\n");
        for exec in &self.exec_created {
            s.push_str(&format!("exec.created += \"{}\";\n", exec))
        }

        s.push_str("exec.start = \"echo exec.start\";\n");
        for exec in &self.exec_start {
            s.push_str(&format!("exec.start += \"{}\";\n", exec))
        }

        s.push_str("exec.stop = \"echo exec.stop\";\n");
        for exec in &self.exec_stop {
            s.push_str(&format!("exec.stop += \"{}\";\n", exec))
        }

        s.push_str("exec.poststop = \"echo exec.poststop\";\n");
        for exec in &self.exec_poststop {
            s.push_str(&format!("exec.poststop += \"{}\";\n", exec))
        }

        s.push_str("exec.release = \"echo exec.release\";\n");
        for exec in &self.exec_release {
            s.push_str(&format!("exec.release += \"{}\";\n", exec))
        }

        s.push_str("}\n");

        s
    }
}
