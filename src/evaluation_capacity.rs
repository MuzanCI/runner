use std::sync::Arc;
use std::sync::Mutex;

use tokio::sync::Notify;
use tokio::sync::futures::Notified;

pub type EvaluationCapacity = u64;

#[derive(thiserror::Error, Debug)]
pub enum EvaluationCapacityError {
    #[error(
        "Not enough evaluation capacity. Requested [{requested}], but only [{available}] available."
    )]
    NotEnoughCapacity {
        available: EvaluationCapacity,
        requested: EvaluationCapacity,
    },
}

#[derive(Clone)]
pub struct SharedEvaluationCapacity {
    capacity: Arc<Mutex<EvaluationCapacity>>,
    notify: Arc<Notify>,
}

impl SharedEvaluationCapacity {
    pub fn new(initial_capacity: EvaluationCapacity) -> Self {
        Self {
            capacity: Arc::new(Mutex::new(initial_capacity)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Reserves evaluation capacity. To commit the capacity reservation, call [`EvaluationCapacityPermit::commit`].
    pub async fn reserve(
        &self,
        amount: EvaluationCapacity,
    ) -> anyhow::Result<EvaluationCapacityPermit> {
        let mut capacity = self.capacity.lock().unwrap();
        if *capacity < amount {
            anyhow::bail!(EvaluationCapacityError::NotEnoughCapacity {
                available: *capacity,
                requested: amount,
            });
        }

        *capacity -= amount;
        Ok(EvaluationCapacityPermit {
            shared: self.clone(),
            amount,
        })
    }

    /// Restores evaluation capacity.
    pub fn restore(&self, amount: EvaluationCapacity) {
        let mut capacity = self.capacity.lock().unwrap();
        *capacity += amount;
        self.notify.notify_waiters();
    }

    pub fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }
}

pub struct EvaluationCapacityPermit {
    shared: SharedEvaluationCapacity,
    amount: EvaluationCapacity,
}

impl Drop for EvaluationCapacityPermit {
    /// If permit is not committed when dropped, then restore the reserved capacity.
    fn drop(&mut self) {
        self.shared.restore(self.amount);
    }
}
