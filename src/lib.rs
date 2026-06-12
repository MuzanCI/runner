use http::Request;
use muzanci_transport::MUZANCI_RUNNER_ID_HEADER;
use muzanci_transport::MUZANCI_TRANSPORT_V1;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::channel::FnChannelAcceptor;
use muzanci_transport::channel::accept;
use muzanci_transport::mux::Mux;
use muzanci_transport::mux::MuxHandle;
use muzanci_transport::runner::RunnerId;

pub mod scheduler;
pub mod worker;

pub async fn connect(hostname: &str) -> anyhow::Result<(RunnerId, MuxHandle)> {
    let server_stream = {
        let stream = tokio::net::TcpStream::connect(hostname).await?;
        stream.set_nodelay(true)?;
        hyper_util::rt::TokioIo::new(stream)
    };

    let (mut send_request, connection) =
        hyper::client::conn::http1::handshake(server_stream).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.with_upgrades().await {
            eprintln!("Connection error: {:?}", e);
        }
    });

    let request = Request::builder()
        .method("POST")
        .uri("/runner/register")
        .header(http::header::HOST, hostname)
        .header(http::header::CONNECTION, "Upgrade")
        .header(http::header::UPGRADE, MUZANCI_TRANSPORT_V1)
        .body(http_body_util::Empty::<bytes::Bytes>::new())
        .unwrap();

    let response = send_request.send_request(request).await?;

    if response.status() != http::StatusCode::SWITCHING_PROTOCOLS {
        return Err(anyhow::anyhow!(
            "Failed to upgrade connection. Server responded with status: {}",
            response.status()
        ));
    }

    let runner_id = response
        .headers()
        .get(MUZANCI_RUNNER_ID_HEADER)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<RunnerId>().ok())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Missing or invalid {} header in response",
                MUZANCI_RUNNER_ID_HEADER
            )
        })?;

    let server_stream = hyper::upgrade::on(response).await?;
    let server_stream = hyper_util::rt::TokioIo::new(server_stream);

    let channel_acceptor = FnChannelAcceptor::new(move |channel_id, channel_type| {
        eprintln!(
            "Received request to open channel [{}] of type {:?}",
            channel_id, channel_type
        );

        // Runner only accepts tunnel channels for now.
        match channel_type {
            ChannelType::Tunnel => {
                eprintln!("Accepting tunnel channel [{}]", channel_id);
            }
            _ => {
                return Err(format!("Channel type {:?} not supported", channel_type));
            }
        };

        Ok(accept(move |channel_handle| async move {
            eprintln!("Accepted channel [{}]", channel_id);
            // TODO: The task that will be spawned to handle the channel.
            let mut tunnel_server = TunnelServer::new(channel_handle);
            tunnel_server.run().await;
        }))
    });
    let mux_handle = Mux::spawn(server_stream, channel_acceptor);

    Ok((runner_id, mux_handle))
}

struct TunnelServer {
    channel_handle: muzanci_transport::channel::ChannelHandle,
}

impl TunnelServer {
    pub fn new(channel_handle: muzanci_transport::channel::ChannelHandle) -> Self {
        Self { channel_handle }
    }

    pub async fn run(&mut self) {
        loop {
            match self.channel_handle.recv().await {
                Some(message) => {
                    println!("Tunnel server received message: {:?}", message);
                    self.handle_message(message).await;
                }
                None => {
                    eprintln!("Tunnel server channel closed");
                    break;
                }
            }
        }
    }

    pub async fn handle_message(&mut self, message: muzanci_transport::channel::Message) {
        println!("Tunnel server handling message: {:?}", message);
        unimplemented!("Tunnel server message handling not implemented yet");
    }
}
