use std::sync::Arc;
use std::sync::Mutex;

use http::Request;

use muzanci_transport::MUZANCI_RUNNER_ID_HEADER;
use muzanci_transport::MUZANCI_TRANSPORT_V1;
use muzanci_transport::RunnerId;
use muzanci_transport::channel::FnChannelAcceptor;
use muzanci_transport::mux::Mux;
use muzanci_transport::mux::MuxHandle;
use tokio_util::sync::CancellationToken;

pub mod debugger_scheduler;
pub mod evaluator;
pub mod evaluator_scheduler;
pub mod worker_scheduler;

#[derive(Clone)]
pub struct RunnerState {
    cancellation_token: CancellationToken,
    runner_id: RunnerId,
    mux_handle: MuxHandle,
    evaluation_capacity: SharedEvaluationCapacity,
}

impl RunnerState {
    pub fn new(
        cancellation_token: CancellationToken,
        runner_id: RunnerId,
        mux_handle: MuxHandle,
        evaluation_capacity: SharedEvaluationCapacity,
    ) -> Self {
        Self {
            cancellation_token,
            runner_id,
            mux_handle,
            evaluation_capacity,
        }
    }

    pub fn has_evaluation_capacity(&self) -> bool {
        let capacity = self.evaluation_capacity.capacity.lock().unwrap();
        *capacity > 0
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

pub type EvaluationCapacity = u64;

#[derive(Clone)]
pub struct SharedEvaluationCapacity {
    capacity: Arc<Mutex<EvaluationCapacity>>,
}

pub struct EvaluationCapacityPermit {
    shared: SharedEvaluationCapacity,
    amount: EvaluationCapacity,
    committed: bool,
}

impl SharedEvaluationCapacity {
    pub fn new(initial_capacity: EvaluationCapacity) -> Self {
        Self {
            capacity: Arc::new(Mutex::new(initial_capacity)),
        }
    }

    /// Reserves evaluation capacity. To commit the capacity reservation, call [`EvaluationCapacityPermit::commit`].
    pub async fn reserve(
        &self,
        amount: EvaluationCapacity,
    ) -> anyhow::Result<EvaluationCapacityPermit> {
        let mut capacity = self.capacity.lock().unwrap();
        if *capacity < amount {
            return Err(anyhow::anyhow!("Not enough evaluation capacity available"));
        }
        *capacity -= amount;
        Ok(EvaluationCapacityPermit {
            shared: self.clone(),
            amount,
            committed: false,
        })
    }

    /// Restores evaluation capacity.
    pub fn restore(&self, amount: EvaluationCapacity) {
        let mut capacity = self.capacity.lock().unwrap();
        *capacity += amount;
    }
}

impl EvaluationCapacityPermit {
    /// Consumes the permit and commits the capacity reduction.
    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for EvaluationCapacityPermit {
    /// If permit is not committed when dropped, then restore the reserved capacity.
    fn drop(&mut self) {
        if !self.committed {
            let mut capacity = self.shared.capacity.lock().unwrap();
            *capacity += self.amount;
        }
    }
}
