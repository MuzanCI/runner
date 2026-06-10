use std::sync::Arc;

use http::Request;
use muzanci_transport::MUZANCI_RUNNER_ID_HEADER;
use muzanci_transport::MUZANCI_TRANSPORT_V1;
use muzanci_transport::mux::Mux;
use muzanci_transport::mux::MuxHandle;

type RunnerId = u64;

pub struct ClientHandle {
    runner_id: RunnerId,
    /// A stateful connection to the server that supports multiplexed communication.
    mux_handle: MuxHandle,
}

pub async fn connect(
    hostname: &str,
    tls_config: Arc<rustls::ClientConfig>,
) -> anyhow::Result<ClientHandle> {
    let raw_stream = tokio::net::TcpStream::connect(hostname).await?;
    raw_stream.set_nodelay(true)?;
    let tls_connector = tokio_rustls::TlsConnector::from(tls_config);
    let server_name = rustls::pki_types::ServerName::try_from(hostname.to_owned())
        .map_err(|_| anyhow::anyhow!("Invalid hostname for TLS: {}", &hostname))?;
    let tls_stream = tls_connector.connect(server_name, raw_stream).await?;
    let tls_stream = hyper_util::rt::TokioIo::new(tls_stream);

    let (mut send_request, connection) = hyper::client::conn::http1::handshake(tls_stream).await?;

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

    Ok(ClientHandle {
        runner_id,
        mux_handle: Mux::spawn(server_stream),
    })
}
