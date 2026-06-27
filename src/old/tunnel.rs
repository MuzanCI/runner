use std::{
    collections::HashMap,
    os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd},
    sync::Arc,
};

use muzanci_transport::channel::ChannelHandle;

pub struct TunnelServer {
    channel_handle: muzanci_transport::channel::ChannelHandle,
}

impl TunnelServer {
    pub fn new(channel_handle: muzanci_transport::channel::ChannelHandle) -> Self {
        Self { channel_handle }
    }

    pub async fn run(self) {
        let config = SshConfig {
            host_key: russh::keys::PrivateKey::random(
                &mut rand::rng(),
                russh::keys::Algorithm::Ed25519,
            )
            .unwrap(),
            authorized_keys: vec![],
        };
        if let Err(e) = run_ssh_server(self.channel_handle, config).await {
            eprintln!("Error running SSH server: {:?}", e);
        }
    }
}

#[derive(Clone)]
pub struct SshConfig {
    pub host_key: russh::keys::PrivateKey,
    pub authorized_keys: Vec<russh::keys::PublicKey>,
}

pub async fn run_ssh_server(
    channel_handle: ChannelHandle,
    config: SshConfig,
) -> anyhow::Result<()> {
    let ssh_server_config = Arc::new(russh::server::Config {
        keys: vec![config.host_key.clone()],
        auth_rejection_time: std::time::Duration::from_secs(1),
        auth_rejection_time_initial: Some(std::time::Duration::from_secs(0)),
        ..Default::default()
    });

    let stream = channel_handle.into_stream();

    let handler = SshHandler {
        // config: config,
        sessions: HashMap::new(),
    };

    if let Err(e) = russh::server::run_stream(ssh_server_config, stream, handler).await {
        eprintln!("SSH server error: {:?}", e);
        return Err(anyhow::anyhow!("SSH server error: {:?}", e));
    }
    Ok(())
}

type PtyMaster = OwnedFd;
type PtySlave = OwnedFd;

/// An SSH session channel. An SSH [`SessionChannel`] is one-to-one with a
/// [`muzanci_transport::channel::ChannelStream`].
///
/// [`russh::server::run_stream`] takes a [`muzanci_transport::channel::ChannelStream`], performs authentication, and
/// then calls the [`russh::server::Handler::channel_open_session`] callback with a newly constructed [`russh::server::Session`].
///
/// SSH specifies multiples channel types, but the [`SshHandler`] only supports session
/// channels for executing shell commands or creating interactive terminals.
pub struct SessionChannel {
    /// A handle to the SSH session channel.
    session_handle: russh::server::Handle,

    /// The child process spawned to execute the client's command.
    child: Option<tokio::process::Child>,

    /// The stdin sender for the child process, if no PTY was requested by the client.
    stdin_tx: Option<tokio::sync::mpsc::Sender<Vec<u8>>>,

    /// The PTY window size, if PTY was requested by the client.
    pty_winsize: Option<nix::pty::Winsize>,

    /// The PTY master file descriptor, if PTY was requested by the client.
    pty_master: Option<PtyMaster>,

    /// The PTY slave file descriptor, if PTY was requested by the client.
    pty_slave: Option<PtySlave>,
}

/// An SSH server handler that authenticates clients, maintains a mapping of
/// SSH channels to [`SessionChannel`]s, and handles SSH requests.
pub struct SshHandler {
    // config: SshConfig,
    sessions: HashMap<russh::ChannelId, SessionChannel>,
}

impl russh::server::Handler for SshHandler {
    type Error = anyhow::Error;

    /// SSH clients are tunneled through the MuzanCI server, which already performs authentication.
    /// Therefore, the worker SSH server accepts all authentication attempts without checking credentials.
    async fn auth_none(&mut self, _user: &str) -> Result<russh::server::Auth, Self::Error> {
        Ok(russh::server::Auth::Accept)
    }

    async fn channel_open_session(
        &mut self,
        channel: russh::Channel<russh::server::Msg>,
        session: &mut russh::server::Session,
    ) -> Result<bool, Self::Error> {
        println!("SSH session channel opened: {:?}", channel);
        self.sessions.insert(
            channel.id(),
            SessionChannel {
                session_handle: session.handle(),
                child: None,
                stdin_tx: None,
                pty_winsize: None,
                pty_master: None,
                pty_slave: None,
            },
        );
        Ok(true)
    }

    async fn channel_close(
        &mut self,
        channel: russh::ChannelId,
        _session: &mut russh::server::Session,
    ) -> Result<(), Self::Error> {
        println!("SSH session channel closed: {:?}", channel);
        if let Some(mut session_channel) = self.sessions.remove(&channel) {
            if let Some(ref mut child) = session_channel.child {
                if let Err(e) = child.kill().await {
                    eprintln!(
                        "Error killing child process for channel {:?}: {:?}",
                        channel, e
                    );
                }
            }
        }
        Ok(())
    }

    async fn pty_request(
        &mut self,
        channel: russh::ChannelId,
        term: &str,
        col: u32,
        row: u32,
        _pix_width: u32,
        _pix_height: u32,
        _modes: &[(russh::Pty, u32)],
        session: &mut russh::server::Session,
    ) -> Result<(), Self::Error> {
        println!(
            "PTY request on channel {:?}: term={}, cols={}, rows={}",
            channel, term, col, row
        );

        let session_channel = match self.sessions.get_mut(&channel) {
            Some(s) => s,
            None => {
                eprintln!(
                    "No session found for channel {:?} during PTY request",
                    channel
                );
                session.channel_failure(channel)?;
                return Err(anyhow::anyhow!(
                    "No session found for channel {:?} during PTY request",
                    channel
                ));
            }
        };

        let winsize = nix::pty::Winsize {
            ws_col: col as u16,
            ws_row: row as u16,
            ws_xpixel: 0,
            ws_ypixel: 0,
        };
        let nix::pty::OpenptyResult { master, slave } = nix::pty::openpty(Some(&winsize), None)?;

        session_channel.pty_master = Some(master);
        session_channel.pty_slave = Some(slave);
        session_channel.pty_winsize = Some(winsize);

        session.channel_success(channel)?;
        Ok(())
    }

    async fn window_change_request(
        &mut self,
        channel: russh::ChannelId,
        col_width: u32,
        row_height: u32,
        _pix_width: u32,
        _pix_height: u32,
        session: &mut russh::server::Session,
    ) -> Result<(), Self::Error> {
        println!(
            "Window change request on channel {:?}: cols={}, rows={}",
            channel, col_width, row_height
        );
        let session_channel = match self.sessions.get_mut(&channel) {
            Some(s) => s,
            None => {
                eprintln!(
                    "No session found for channel {:?} during window change request",
                    channel
                );
                session.channel_failure(channel)?;
                return Err(anyhow::anyhow!(
                    "No session found for channel {:?} during window change request",
                    channel
                ));
            }
        };

        match session_channel.pty_winsize {
            Some(ref mut winsize) => {
                winsize.ws_col = col_width as u16;
                winsize.ws_row = row_height as u16;
            }
            None => {
                eprintln!(
                    "No PTY allocated for channel {:?} during window change request",
                    channel
                );
                session.channel_failure(channel)?;
                return Err(anyhow::anyhow!(
                    "No PTY allocated for channel {:?} during window change request",
                    channel
                ));
            }
        }

        session.channel_success(channel)?;

        Ok(())
    }

    async fn exec_request(
        &mut self,
        channel: russh::ChannelId,
        data: &[u8],
        session: &mut russh::server::Session,
    ) -> Result<(), Self::Error> {
        let cmd_str = std::str::from_utf8(data)?.to_string();
        println!("Exec request on channel {:?}: cmd_str={}", channel, cmd_str);

        let session_channel = match self.sessions.get_mut(&channel) {
            Some(s) => s,
            None => {
                eprintln!("No session found for channel {:?}", channel);
                session.channel_failure(channel)?;
                return Ok(());
            }
        };

        match (
            session_channel.pty_master.as_ref(),
            session_channel.pty_slave.as_ref(),
        ) {
            // SSH client did not request PTY
            (None, None) => spawn_process(&cmd_str, channel, session_channel).await?,

            // SSH client request PTY
            (Some(_), Some(_)) => {
                spawn_process_with_pty(&cmd_str, channel, session_channel).await?
            }

            // Invalid PTY state - one of master/slave is Some but the other is None
            _ => {
                eprintln!(
                    "Invalid PTY state for channel {:?} during exec request",
                    channel
                );
                session.channel_failure(channel)?;
                return Ok(());
            }
        };

        session.channel_success(channel)?;
        Ok(())
    }

    async fn shell_request(
        &mut self,
        channel: russh::ChannelId,
        session: &mut russh::server::Session,
    ) -> Result<(), Self::Error> {
        println!("Shell request on channel {:?}", channel);

        let session_channel = match self.sessions.get_mut(&channel) {
            Some(s) => s,
            None => {
                eprintln!("No session found for channel {:?}", channel);
                session.channel_failure(channel)?;
                return Ok(());
            }
        };

        let cmd_str = std::env::var("SHELL").unwrap_or("/bin/sh".to_string());

        match (
            session_channel.pty_master.as_ref(),
            session_channel.pty_slave.as_ref(),
        ) {
            // SSH client did not request PTY
            (None, None) => spawn_process(&cmd_str, channel, session_channel).await?,

            // SSH client request PTY
            (Some(_), Some(_)) => {
                spawn_process_with_pty(&cmd_str, channel, session_channel).await?
            }

            // Invalid PTY state - one of master/slave is Some but the other is None
            _ => {
                eprintln!(
                    "Invalid PTY state for channel {:?} during shell request",
                    channel
                );
                session.channel_failure(channel)?;
                return Ok(());
            }
        };

        session.channel_success(channel)?;
        Ok(())
    }

    async fn data(
        &mut self,
        channel: russh::ChannelId,
        data: &[u8],
        session: &mut russh::server::Session,
    ) -> Result<(), Self::Error> {
        println!(
            "Data received on channel {:?}: {} bytes",
            channel,
            data.len()
        );

        let session_channel = match self.sessions.get_mut(&channel) {
            Some(s) => s,
            None => {
                eprintln!(
                    "No session found for channel {:?} during data reception",
                    channel
                );
                session.channel_failure(channel)?;
                return Ok(());
            }
        };

        if let Some(stdin_tx) = &session_channel.stdin_tx {
            stdin_tx.send(data.to_vec()).await?;
        } else {
            eprintln!(
                "No stdin channel available for channel {:?} during data reception",
                channel
            );
            session.channel_failure(channel)?;
        }

        session.channel_success(channel)?;
        Ok(())
    }
}

use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
async fn spawn_process(
    cmd_str: &str,
    channel_id: russh::ChannelId,
    session_channel: &mut SessionChannel,
) -> anyhow::Result<()> {
    let mut child = tokio::process::Command::new("sh")
        .arg("-c")
        .arg(cmd_str)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()?;

    // Spawn subtask to receive stdin.
    let mut child_stdin = child.stdin.take().unwrap();
    let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::channel(100);
    session_channel.stdin_tx = Some(stdin_tx);

    tokio::spawn(async move {
        while let Some(bytes) = stdin_rx.recv().await {
            if let Err(e) = child_stdin.write_all(&bytes).await {
                eprintln!("Error writing to child stdin: {:?}", e);
                break;
            }
        }
        child_stdin.shutdown().await.unwrap();
    });

    // Spawn subtask to send stdout.
    let child_stdout = child.stdout.take().unwrap();
    let stdout_session_handle = session_channel.session_handle.clone();

    tokio::spawn(async move {
        let mut reader = BufReader::new(child_stdout);
        let mut buffer = vec![0u8; 4096];
        loop {
            match reader.read(&mut buffer).await {
                Ok(0) => break, // EOF
                Ok(n) => {
                    let data = russh::CryptoVec::from_slice(&buffer[..n]);
                    if let Err(e) = stdout_session_handle.data(channel_id, data.to_vec()).await {
                        eprintln!("Error sending stdout data to SSH client: {:?}", e);
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Error reading from child stdout: {:?}", e);
                    break;
                }
            }
        }
    });

    // Spawn subtask to send stderr.
    let child_stderr = child.stderr.take().unwrap();
    let stderr_session_handle = session_channel.session_handle.clone();

    tokio::spawn(async move {
        let mut reader = BufReader::new(child_stderr);
        let mut buffer = vec![0u8; 4096];
        loop {
            match reader.read(&mut buffer).await {
                Ok(0) => break, // EOF
                Ok(n) => {
                    let data = russh::CryptoVec::from_slice(&buffer[..n]);
                    if let Err(e) = stderr_session_handle.data(channel_id, data.to_vec()).await {
                        eprintln!("Error sending stderr data to SSH client: {:?}", e);
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Error reading from child stderr: {:?}", e);
                    break;
                }
            }
        }
    });

    // Spawn subtask to send exit code.
    let exit_code_session_handle = session_channel.session_handle.clone();

    tokio::spawn(async move {
        if let Ok(status) = child.wait().await {
            let exit_code = status.code().unwrap();
            exit_code_session_handle
                .exit_status_request(channel_id, exit_code as u32)
                .await
                .unwrap();
            exit_code_session_handle.close(channel_id).await.unwrap();
        }
    });

    Ok(())
}

async fn spawn_process_with_pty(
    cmd_str: &str,
    channel_id: russh::ChannelId,
    session_channel: &mut SessionChannel,
) -> anyhow::Result<()> {
    let pty_master_fd = session_channel.pty_master.take().unwrap();
    let pty_slave_fd = session_channel.pty_slave.take().unwrap();
    let pty_slave_file = std::fs::File::from(pty_slave_fd);
    let mut child = tokio::process::Command::new("sh")
        .arg("-c")
        .arg(cmd_str)
        .stdin(pty_slave_file.try_clone()?)
        .stdout(pty_slave_file.try_clone()?)
        .stderr(pty_slave_file)
        .spawn()?;

    // Spawn subtask to receive PTY input.
    let input_pty_master_fd = pty_master_fd.try_clone()?;
    let mut input_pty_master_file =
        tokio::fs::File::from_std(std::fs::File::from(input_pty_master_fd));
    let (stdin_tx, mut stdin_rx) = tokio::sync::mpsc::channel(100);
    session_channel.stdin_tx = Some(stdin_tx);

    tokio::spawn(async move {
        while let Some(bytes) = stdin_rx.recv().await {
            if let Err(e) = input_pty_master_file.write_all(&bytes).await {
                eprintln!("Error writing to child stdin: {:?}", e);
                break;
            }
        }
    });

    // Spawn subtask to send PTY output.
    let output_pty_master_fd = pty_master_fd.try_clone()?;
    let output_pty_master_file =
        tokio::fs::File::from_std(std::fs::File::from(output_pty_master_fd));
    let session_handle = session_channel.session_handle.clone();

    tokio::spawn(async move {
        let mut reader = BufReader::new(output_pty_master_file);
        let mut buffer = vec![0u8; 4096];
        loop {
            match reader.read(&mut buffer).await {
                Ok(0) => break, // EOF
                Ok(n) => {
                    let data = russh::CryptoVec::from_slice(&buffer[..n]);
                    if let Err(e) = session_handle.data(channel_id, data.to_vec()).await {
                        eprintln!("Error sending stdout data to SSH client: {:?}", e);
                        break;
                    }
                }
                Err(e) => {
                    eprintln!("Error reading from child stdout: {:?}", e);
                    break;
                }
            }
        }
    });

    // Spawn subtask to send exit code.
    let exit_code_session_handle = session_channel.session_handle.clone();
    tokio::spawn(async move {
        if let Ok(status) = child.wait().await {
            let exit_code = status.code().unwrap();
            exit_code_session_handle
                .exit_status_request(channel_id, exit_code as u32)
                .await
                .unwrap();
            exit_code_session_handle.close(channel_id).await.unwrap();
        }
    });

    Ok(())
}
