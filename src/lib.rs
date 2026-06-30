use std::sync::Arc;

use http::Request;

use muzanci_transport::MUZANCI_RUNNER_ID_HEADER;
use muzanci_transport::MUZANCI_TRANSPORT_V1;
use muzanci_transport::RunnerId;
use muzanci_transport::channel::FnChannelAcceptor;
use muzanci_transport::mux::Mux;
use muzanci_transport::mux::MuxHandle;
use tokio_util::sync::CancellationToken;

use crate::capacity::SharedAssignmentCapacity;
use crate::capacity::SharedEvaluationCapacity;
use crate::jail::Jailer;
use crate::secrets::SecretsService;

pub mod capacity;
pub mod evaluator;
pub mod jail;
pub mod logging;
pub mod scheduler;
pub mod secrets;
pub mod worker;

#[derive(Clone)]
pub struct RunnerState {
    cancellation_token: CancellationToken,
    runner_id: RunnerId,
    mux_handle: MuxHandle,
    evaluation_capacity: SharedEvaluationCapacity,
    assignment_capacity: SharedAssignmentCapacity,
    jailer: Arc<dyn Jailer>,
    secrets_service: Arc<SecretsService>,
}

impl RunnerState {
    pub fn new(
        cancellation_token: CancellationToken,
        runner_id: RunnerId,
        mux_handle: MuxHandle,
        evaluation_capacity: SharedEvaluationCapacity,
        assignment_capacity: SharedAssignmentCapacity,
        jailer: Arc<dyn Jailer>,
        secrets_service: Arc<SecretsService>,
    ) -> Self {
        Self {
            cancellation_token,
            runner_id,
            mux_handle,
            evaluation_capacity,
            assignment_capacity,
            jailer,
            secrets_service,
        }
    }
}

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
        panic!(
            "Runner received request to open channel [{}] of type {:?}",
            channel_id, channel_type
        );
    });

    let mux_handle = Mux::spawn(server_stream, channel_acceptor);

    Ok((runner_id, mux_handle))
}
