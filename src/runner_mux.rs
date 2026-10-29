use http::HeaderMap;
use http::HeaderValue;
use http::Request;
use http::StatusCode;
use http_body_util::BodyExt;
use muzanci_transport::channel::ChannelReceiver;
use muzanci_transport::channel::ChannelSender;
use muzanci_transport::channel::ChannelType;
use muzanci_transport::channel::FnChannelAcceptor;
use muzanci_transport::http::MUZANCI_RUNNER_ID_HEADER;
use muzanci_transport::http::MUZANCI_TRANSPORT_V1;
use muzanci_transport::mux::Mux;
use tokio_util::sync::CancellationToken;

use muzanci_observability::TError;
use muzanci_transport::http::RegisterRunnerResponse;
use muzanci_transport::message::AccountId;
use muzanci_transport::message::RunnerId;
use muzanci_transport::mux::MuxHandle;
use uuid::Uuid;

#[derive(Debug, thiserror::Error)]
#[error("RunnerMuxError: {0}")]
pub struct RunnerMuxError(String);

#[derive(Clone)]
pub struct RunnerMuxHandle {
    runner_id: RunnerId,
    mux_handle: MuxHandle,
}

impl RunnerMuxHandle {
    pub async fn new(
        runner_shutdown: CancellationToken,
        hostname: &str,
        secret: &str,
        account_id: AccountId,
    ) -> Result<Self, TError> {
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
            .uri("/register_runner")
            .header(http::header::HOST, hostname)
            .header(http::header::CONNECTION, "Upgrade")
            .header(http::header::UPGRADE, MUZANCI_TRANSPORT_V1)
            .header("x-muzanci-personal-access-token", secret.to_string())
            .header("x-muzanci-account-id", account_id.to_string())
            .body(http_body_util::Empty::<bytes::Bytes>::new())
            .unwrap();

        let response = send_request.send_request(request).await?;

        match response.status() {
            StatusCode::SWITCHING_PROTOCOLS => {}
            StatusCode::BAD_REQUEST => {
                let body_bytes = response.collect().await?.to_bytes();
                let e = serde_json::from_slice::<RegisterRunnerResponse>(&body_bytes)?.error;
                let e = RunnerMuxError(format!("Client error: {e}"));
                return Err(e.into());
            }
            StatusCode::INTERNAL_SERVER_ERROR => {
                let body_bytes = response.collect().await?.to_bytes();
                let e = serde_json::from_slice::<RegisterRunnerResponse>(&body_bytes)?.error;
                let e = RunnerMuxError(format!("Server error: {e}"));
                return Err(e.into());
            }
            code => {
                let e = RunnerMuxError(format!("Unexpected status code: {code}"));
                return Err(e.into());
            }
        }

        let runner_id = extract_runner_id(response.headers())?;

        let hyper_upgraded_stream = hyper::upgrade::on(response).await?;
        let tokio_stream = hyper_util::rt::TokioIo::new(hyper_upgraded_stream);
        let (read_half, write_half) = tokio::io::split(tokio_stream);

        let channel_acceptor = FnChannelAcceptor::new(move |channel_id, channel_type| {
            panic!(
                "Runner received request to open channel [{}] of type {:?}",
                channel_id, channel_type
            );
        });

        let (_join_handle, mux_handle) =
            Mux::spawn(read_half, write_half, channel_acceptor, runner_shutdown);

        Ok(Self {
            runner_id,
            mux_handle,
        })
    }

    pub fn runner_id(&self) -> RunnerId {
        self.runner_id
    }

    pub async fn open_channel(
        &self,
        channel_type: ChannelType,
    ) -> Result<(ChannelSender, ChannelReceiver, CancellationToken), TError> {
        self.mux_handle
            .open_channel(channel_type)
            .await
            .map_err(|e| e.into())
    }
}

fn extract_runner_id(headers: &HeaderMap<HeaderValue>) -> Result<RunnerId, TError> {
    let val = headers
        .get(MUZANCI_RUNNER_ID_HEADER)
        .ok_or_else(|| TError::new(&format!("missing header [{MUZANCI_RUNNER_ID_HEADER}]")))?;

    let s = val.to_str().map_err(|e| {
        RunnerMuxError(format!(
            "Header [{MUZANCI_RUNNER_ID_HEADER}] is not a string: {e}"
        ))
    })?;

    let uuid = Uuid::try_parse(s).map_err(|e| {
        RunnerMuxError(format!(
            "Header [{MUZANCI_RUNNER_ID_HEADER}] is not a valid UUID: {e}"
        ))
    })?;

    Ok(uuid.into())
}
