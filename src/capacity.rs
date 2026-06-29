use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, futures::Notified};

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
            return Err(anyhow::anyhow!(
                EvaluationCapacityError::NotEnoughCapacity {
                    available: *capacity,
                    requested: amount,
                }
            ));
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
        self.notify.notify_waiters();
    }

    pub fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }
}

pub struct EvaluationCapacityPermit {
    shared: SharedEvaluationCapacity,
    amount: EvaluationCapacity,
    committed: bool,
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
            self.shared.restore(self.amount);
        }
    }
}

pub type AssignmentCapacity = u64;

#[derive(thiserror::Error, Debug)]
pub enum AssignmentCapacityError {
    #[error(
        "Not enough assignment capacity. Requested [{requested}], but only [{available}] available."
    )]
    NotEnoughCapacity {
        available: AssignmentCapacity,
        requested: AssignmentCapacity,
    },
}

#[derive(Clone)]
pub struct SharedAssignmentCapacity {
    capacity: Arc<Mutex<AssignmentCapacity>>,
    notify: Arc<Notify>,
}

impl SharedAssignmentCapacity {
    pub fn new(initial_capacity: AssignmentCapacity) -> Self {
        Self {
            capacity: Arc::new(Mutex::new(initial_capacity)),
            notify: Arc::new(Notify::new()),
        }
    }

    /// Reserves assignment capacity. To commit the capacity reservation, call [`AssignmentCapacityPermit::commit`].
    pub async fn reserve(
        &self,
        amount: AssignmentCapacity,
    ) -> anyhow::Result<AssignmentCapacityPermit> {
        let mut capacity = self.capacity.lock().unwrap();
        if *capacity < amount {
            return Err(anyhow::anyhow!(
                AssignmentCapacityError::NotEnoughCapacity {
                    available: *capacity,
                    requested: amount,
                }
            ));
        }
        *capacity -= amount;
        Ok(AssignmentCapacityPermit {
            shared: self.clone(),
            amount,
            committed: false,
        })
    }

    /// Restores assignment capacity.
    pub fn restore(&self, amount: AssignmentCapacity) {
        let mut capacity = self.capacity.lock().unwrap();
        *capacity += amount;
        self.notify.notify_waiters();
    }

    pub fn notified(&self) -> Notified<'_> {
        self.notify.notified()
    }
}

pub struct AssignmentCapacityPermit {
    shared: SharedAssignmentCapacity,
    amount: AssignmentCapacity,
    committed: bool,
}

impl AssignmentCapacityPermit {
    /// Consumes the permit and commits the capacity reduction.
    pub fn commit(mut self) {
        self.committed = true;
    }
}

impl Drop for AssignmentCapacityPermit {
    /// If permit is not committed when dropped, then restore the reserved capacity.
    fn drop(&mut self) {
        if !self.committed {
            self.shared.restore(self.amount);
        }
    }
}
