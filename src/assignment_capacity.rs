use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Notify;
use tokio::sync::futures::Notified;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

pub type AssignmentCapacity = u64;

/// A request from another task to reserve capacity.
struct AssignmentCapacityRequest {
    /// The amount of capacity to reserve.
    amount: AssignmentCapacity,

    /// The channel to notify the sender that the request has been fulfilled.
    reply_tx: oneshot::Sender<anyhow::Result<AssignmentCapacityPermit>>,
}

/// A handle to the actor that manages assignment capacity.
#[derive(Clone)]
pub struct SharedAssignmentCapacityHandle {
    high_prio_request_tx: mpsc::Sender<AssignmentCapacityRequest>,
    low_prio_request_tx: mpsc::Sender<AssignmentCapacityRequest>,
    notify: Arc<Notify>,
}

impl SharedAssignmentCapacityHandle {
    /// Reserve capacity with high priority.
    pub async fn reserve_high(
        &self,
        amount: AssignmentCapacity,
    ) -> anyhow::Result<AssignmentCapacityPermit> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.high_prio_request_tx
            .send(AssignmentCapacityRequest { amount, reply_tx })
            .await?;

        reply_rx.await?
    }

    /// Reserve capacity with low priority.
    pub async fn reserve_low(
        &self,
        amount: AssignmentCapacity,
    ) -> anyhow::Result<AssignmentCapacityPermit> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.low_prio_request_tx
            .send(AssignmentCapacityRequest { amount, reply_tx })
            .await?;

        reply_rx.await?
    }

    /// Return a [`Notified`] future that is notified when capacity is restored.
    pub fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }
}

/// The actor task that manages assignment capacity.
pub struct SharedAssignmentCapacity {
    capacity: AssignmentCapacity,
    high_prio_request_rx: mpsc::Receiver<AssignmentCapacityRequest>,
    low_prio_request_rx: mpsc::Receiver<AssignmentCapacityRequest>,
    restore_tx: mpsc::Sender<AssignmentCapacity>,
    restore_rx: mpsc::Receiver<AssignmentCapacity>,
    high_prio_request_queue: VecDeque<AssignmentCapacityRequest>,
    low_prio_request_queue: VecDeque<AssignmentCapacityRequest>,
    notify: Arc<Notify>,
}

impl SharedAssignmentCapacity {
    pub fn spawn(capacity: AssignmentCapacity) -> SharedAssignmentCapacityHandle {
        let (high_prio_request_tx, high_prio_request_rx) = mpsc::channel(8);
        let (low_prio_request_tx, low_prio_request_rx) = mpsc::channel(8);
        let notify = Arc::new(Notify::new());

        let notify_clone = notify.clone();
        tokio::spawn(async move {
            let (restore_tx, restore_rx) = mpsc::channel(8);
            SharedAssignmentCapacity {
                capacity,
                high_prio_request_rx,
                low_prio_request_rx,
                restore_tx,
                restore_rx,
                high_prio_request_queue: VecDeque::new(),
                low_prio_request_queue: VecDeque::new(),
                notify: notify_clone,
            }
            .run()
            .await
            .unwrap();
        });

        SharedAssignmentCapacityHandle {
            high_prio_request_tx,
            low_prio_request_tx,
            notify,
        }
    }

    async fn run(mut self) -> anyhow::Result<()> {
        loop {
            // Process restorations first to free up capacity
            while let Ok(amount) = self.restore_rx.try_recv() {
                self.capacity += amount;
                self.notify.notify_waiters();
            }

            self.process_queues()?;

            tokio::select! {
                biased; // Guarantees evaluation in strictly declared top-to-bottom order

                Some(amount) = self.restore_rx.recv() => {
                    self.capacity += amount;
                    self.notify.notify_waiters();
                }
                Some(req) = self.high_prio_request_rx.recv() => {
                    self.high_prio_request_queue.push_back(req);
                }
                Some(req) = self.low_prio_request_rx.recv() => {
                    self.low_prio_request_queue.push_back(req);
                }
                else => break,
            }
        }

        Ok(())
    }

    fn process_queues(&mut self) -> anyhow::Result<()> {
        Self::fulfill_queue(
            &mut self.capacity,
            &mut self.high_prio_request_queue,
            &self.restore_tx,
        )?;

        if !self.high_prio_request_queue.is_empty() {
            return Ok(());
        }

        Self::fulfill_queue(
            &mut self.capacity,
            &mut self.low_prio_request_queue,
            &self.restore_tx,
        )
    }

    fn fulfill_queue(
        capacity: &mut AssignmentCapacity,
        queue: &mut VecDeque<AssignmentCapacityRequest>,
        restore_tx: &mpsc::Sender<AssignmentCapacity>,
    ) -> anyhow::Result<()> {
        while let Some(request) = queue.pop_front() {
            let reply = if *capacity < request.amount {
                Err(anyhow::anyhow!("Insufficient capacity"))
            } else {
                *capacity -= request.amount;
                Ok(AssignmentCapacityPermit {
                    restore_tx: restore_tx.clone(),
                    amount: request.amount,
                })
            };

            if let Err(_) = request.reply_tx.send(reply) {
                return Err(anyhow::anyhow!("Failed to reply to request"));
            }
        }

        Ok(())
    }
}

/// A permit that represents a granted assignment capacity.
pub struct AssignmentCapacityPermit {
    restore_tx: mpsc::Sender<AssignmentCapacity>,
    amount: AssignmentCapacity,
}

impl Drop for AssignmentCapacityPermit {
    fn drop(&mut self) {
        let _ = self.restore_tx.try_send(self.amount);
    }
}
