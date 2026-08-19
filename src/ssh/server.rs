use bytes::Bytes;
use russh::CryptoVec;
use std::collections::HashMap;
use std::io;
use std::os::fd::AsFd;
use std::os::fd::AsRawFd;
use std::os::fd::OwnedFd;
use std::os::unix::process::CommandExt;
use std::process::Command;
use std::sync::Mutex;
use tokio::io::Interest;
use tokio::io::unix::AsyncFd;

use nix::fcntl::FcntlArg;
use nix::fcntl::OFlag;
use nix::fcntl::fcntl;
use nix::pty::Winsize;
use nix::pty::openpty;
use russh::ChannelId;
use russh::server::Handler;
use russh::server::Session;
use std::sync::Arc;

pub struct ServerHandler {
    jid: String,
    cols: u16,
    rows: u16,
    pty: Option<Arc<Pty>>,
    processes: Arc<Mutex<HashMap<russh::ChannelId, u32>>>,
}

impl ServerHandler {
    pub fn new(jid: String) -> Self {
        Self {
            jid,
            cols: 80,
            rows: 24,
            pty: None,
            processes: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Handler for ServerHandler {
    type Error = russh::Error;

    async fn auth_none(&mut self, _username: &str) -> Result<russh::server::Auth, Self::Error> {
        Ok(russh::server::Auth::Accept)
    }

    // async fn auth_publickey(
    //     &mut self,
    //     _username: &str,
    //     _public_key: &russh::keys::PublicKey,
    // ) -> Result<russh::server::Auth, Self::Error> {
    //     Ok(russh::server::Auth::Accept)
    // }
    //
    async fn channel_open_session(
        &mut self,
        channel: russh::Channel<russh::server::Msg>,
        reply: russh::server::ChannelOpenHandle,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        reply.accept().await;
        Ok(())
    }

    async fn pty_request(
        &mut self,
        _channel: ChannelId,
        _term: &str,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _modes: &[(russh::Pty, u32)],
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.cols = col_width as u16;
        self.rows = row_height as u16;
        Ok(())
    }

    async fn window_change_request(
        &mut self,
        _channel: ChannelId,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        self.cols = col_width as u16;
        self.rows = row_height as u16;

        if let Some(ref pty) = self.pty {
            let _ = pty.resize(self.cols, self.rows);
        }
        Ok(())
    }

    async fn signal(
        &mut self,
        channel: ChannelId,
        signal_name: russh::Sig,
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        let processes = self
            .processes
            .lock()
            .expect("SSH server handler processes lock is poisoned");
        if let Some(&pid) = processes.get(&channel) {
            let sig_num = match signal_name {
                russh::Sig::INT => libc::SIGINT,
                russh::Sig::TERM => libc::SIGTERM,
                russh::Sig::KILL => libc::SIGKILL,
                russh::Sig::QUIT => libc::SIGQUIT,
                _ => libc::SIGINT,
            };
            unsafe {
                libc::kill(pid as i32, sig_num);
            }
        }
        Ok(())
    }

    async fn exec_request(
        &mut self,
        channel: ChannelId,
        data: &[u8],
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        let command_str = match std::str::from_utf8(data) {
            Ok(s) => s,
            Err(_) => {
                session.request_failure();
                return Ok(());
            }
        };

        // 1. Confirm to client that the exec request was accepted
        session.request_success();

        // 2. Spawn the child shell process
        let mut child = match Command::new("sh")
            .arg("-c")
            .arg(command_str)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
        {
            Ok(c) => c,
            Err(_) => {
                let _ = session.exit_status_request(channel, 127);
                let _ = session.close(channel);
                return Ok(());
            }
        };

        // 3. Track PID for signal forwarding
        {
            let mut processes = self
                .processes
                .lock()
                .expect("SSH server handler processes lock is poisoned");
            processes.insert(channel, child.id());
        }

        let mut stdout = child.stdout.take().unwrap();
        let mut stderr = child.stderr.take().unwrap();

        let handle = session.handle();
        let processes_mutex = self.processes.clone();

        // 4. Stream I/O in a background Tokio task
        tokio::spawn(async move {
            use std::io::Read;
            let handle_out = handle.clone();
            let stdout_task = tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                while let Ok(n) = stdout.read(&mut buf) {
                    if n == 0 {
                        break;
                    }
                    let _ = handle_out.data(channel, Vec::from(&buf[..n])).await;
                }
            });

            let handle_err = handle.clone();
            let stderr_task = tokio::spawn(async move {
                let mut buf = [0u8; 4096];
                while let Ok(n) = stderr.read(&mut buf) {
                    if n == 0 {
                        break;
                    }
                    let _ = handle_err
                        .extended_data(channel, 1, Vec::from(&buf[..n]))
                        .await;
                }
            });

            // Wait for stdout/stderr readers and process completion
            let _ = tokio::join!(stdout_task, stderr_task);

            let exit_code = match child.wait() {
                Ok(status) => status.code().unwrap_or(128) as u32,
                Err(_) => 1,
            };

            // 5. Send exit code and close channel
            let _ = handle.exit_status_request(channel, exit_code).await;
            let _ = handle.close(channel).await;

            // Remove process entry
            //             let mut processes = self
            let mut processes = processes_mutex
                .lock()
                .expect("SSH server handler processes lock is poisoned");
            processes.remove(&channel);
        });

        Ok(())
    }

    async fn shell_request(
        &mut self,
        channel: ChannelId,
        session: &mut Session,
    ) -> Result<(), Self::Error> {
        // TODO: Construct jexec command for FreeBSD Jail execution
        let cmd = std::process::Command::new("sh");

        // Spawn child process with PTY
        let (pty, mut child) = match Pty::spawn(cmd, self.cols, self.rows) {
            Ok(res) => res,
            Err(_) => {
                return Err(russh::Error::IO(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "failed to spawn PTY",
                )));
            }
        };

        let pty_arc = Arc::new(pty);
        self.pty = Some(Arc::clone(&pty_arc));
        let handle = session.handle();

        // Spawn I/O forwarding task on Tokio runtime
        tokio::spawn(async move {
            let _ = read_pty_to_channel(&pty_arc, channel, handle.clone()).await;

            // Retrieve child process exit code
            let status = child.wait().map(|s| s.code().unwrap_or(1)).unwrap_or(1);
            let _ = handle.exit_status_request(channel, status as u32).await;
            let _ = handle.close(channel).await;
        });

        Ok(())
    }

    async fn data(
        &mut self,
        _channel: ChannelId,
        data: &[u8],
        _session: &mut Session,
    ) -> Result<(), Self::Error> {
        if let Some(ref pty) = self.pty {
            write_channel_to_pty(pty, data)
                .await
                .map_err(|e| russh::Error::from(e))?;
        }
        Ok(())
    }
}

struct Pty {
    master: AsyncFd<OwnedFd>,
}

impl Pty {
    fn spawn(mut cmd: Command, cols: u16, rows: u16) -> io::Result<(Self, std::process::Child)> {
        let winsize = Winsize {
            ws_row: rows,
            ws_col: cols,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };

        // 1. Safe PTY Allocation via nix
        let pty =
            openpty(Some(&winsize), None).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // 2. Safe Non-Blocking Configuration via nix
        let current_flags = fcntl(pty.master.as_fd(), FcntlArg::F_GETFL)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        let new_flags = OFlag::from_bits_truncate(current_flags) | OFlag::O_NONBLOCK;
        fcntl(pty.master.as_fd(), FcntlArg::F_SETFL(new_flags))
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;

        // 3. Command Pre-Exec Hook
        // UNSAFE EXPLANATION: `pre_exec` is inherently unsafe in Rust because running non-async-signal-safe
        // functions post-fork in a multi-threaded Tokio runtime can cause deadlocks.
        let raw_slave = pty.slave.as_raw_fd();
        unsafe {
            cmd.pre_exec(move || {
                // libc::login_tty handles setsid(), setting TIOCSCTTY, and dup2 for stdio
                if libc::login_tty(raw_slave) != 0 {
                    return Err(std::io::Error::last_os_error());
                }
                Ok(())
            });
        }

        // Spawn child process (`slave_fd` is closed automatically when `pty.slave` drops)
        let child = cmd.spawn()?;
        let async_master = AsyncFd::new(pty.master)?;

        Ok((
            Self {
                master: async_master,
            },
            child,
        ))
    }

    /// Resize the window dimensions using system ioctl
    fn resize(&self, cols: u16, rows: u16) -> io::Result<()> {
        let winsize = Winsize {
            ws_row: rows,
            ws_col: cols,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };

        // UNSAFE EXPLANATION: `ioctl` is an untyped C vararg syscall.
        // `libc::TIOCSWINSZ` is used here because nix does not expose a standard safe function for winsize ioctls.
        let res = unsafe {
            libc::ioctl(
                self.master.as_raw_fd(),
                libc::TIOCSWINSZ,
                &winsize as *const _ as *const libc::c_void,
            )
        };

        if res == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

/// Reads PTY master output asynchronously without blocking the Tokio reactor thread
async fn read_pty_to_channel(
    pty: &Pty,
    channel: russh::ChannelId,
    handle: russh::server::Handle,
) -> io::Result<()> {
    let mut buf = [0u8; 2048];

    loop {
        let n = pty
            .master
            .async_io(Interest::READABLE, |inner| {
                nix::unistd::read(inner, &mut buf)
                    .map_err(|e| io::Error::from_raw_os_error(e as i32))
            })
            .await?;

        if n == 0 {
            break;
        }

        let data_to_send = Bytes::copy_from_slice(&buf[..n]);
        if handle.data(channel, data_to_send).await.is_err() {
            break;
        }
    }
    Ok(())
}

/// Writes incoming SSH channel client data directly into master PTY
async fn write_channel_to_pty(pty: &Pty, data: &[u8]) -> io::Result<()> {
    let mut written = 0;
    while written < data.len() {
        // Standard Write trait on OwnedFd
        let n = pty
            .master
            .async_io(Interest::WRITABLE, |inner| {
                nix::unistd::write(inner, &data[written..])
                    .map_err(|e| io::Error::from_raw_os_error(e as i32))
            })
            .await?;

        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::WriteZero,
                "failed to write any data",
            ));
        }

        written += n;
    }
    Ok(())
}
