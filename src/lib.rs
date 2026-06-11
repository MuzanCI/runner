use futures::future::BoxFuture;
use http::Request;
use muzanci_transport::MUZANCI_TRANSPORT_V1;
use muzanci_transport::MUZANCI_WORKER_ID_HEADER;
use muzanci_transport::channel::FnOpenChannelRequestHandler;
use muzanci_transport::channel::accept;
use muzanci_transport::mux::Mux;
use muzanci_transport::mux::MuxHandle;

pub mod runner;
pub mod scheduler;

type WorkerId = u64;

pub async fn connect(hostname: &str) -> anyhow::Result<(WorkerId, MuxHandle)> {
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
        .uri("/worker/register")
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

    let worker_id = response
        .headers()
        .get(MUZANCI_WORKER_ID_HEADER)
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<WorkerId>().ok())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "Missing or invalid {} header in response",
                MUZANCI_WORKER_ID_HEADER
            )
        })?;

    let server_stream = hyper::upgrade::on(response).await?;
    let server_stream = hyper_util::rt::TokioIo::new(server_stream);

    let open_handler = FnOpenChannelRequestHandler::new(move |channel_id| {
        eprintln!("Received request to open channel [{}]", channel_id);
        // TODO: Decide whether the channel can be opened.
        Ok(accept(move |channel_handle| async move {
            eprintln!("Accepted channel [{}]", channel_id);
            // TODO: Spawn a task to handle the channel.
        }))
    });
    let mux_handle = Mux::spawn(server_stream, open_handler);

    Ok((worker_id, mux_handle))
}
