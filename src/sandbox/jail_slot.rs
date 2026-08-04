use std::net::Ipv4Addr;
use std::sync::Arc;
use std::sync::Mutex;

pub type JailSlotId = usize;

pub struct JailSlot {
    slot_id: JailSlotId,
    free_slots: FreeJailSlots,
}

impl JailSlot {
    pub fn new(slot_id: JailSlotId, free_slots: FreeJailSlots) -> Self {
        Self {
            slot_id,
            free_slots,
        }
    }

    pub fn slot_id(&self) -> JailSlotId {
        self.slot_id
    }
}

impl Drop for JailSlot {
    fn drop(&mut self) {
        self.free_slots.restore(self.slot_id);
    }
}

#[derive(Clone)]
pub struct FreeJailSlots {
    free_slot_ids: Arc<Mutex<Vec<JailSlotId>>>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FreeSlotsError {
    #[error("num_slots must be <= 255")]
    NumSlotsTooLarge,
    #[error("no slots remaining")]
    NoSlotsRemaining,
}

impl FreeJailSlots {
    /// Initializes with a fixed number of slots.
    ///
    /// Returns an error if `num_slots` is greater than 255.
    pub fn try_new(num_slots: usize) -> Result<Self, FreeSlotsError> {
        if num_slots > 255 {
            return Err(FreeSlotsError::NumSlotsTooLarge);
        }
        let slots = (1..=num_slots).collect();
        let slots = Arc::new(Mutex::new(slots));
        Ok(Self {
            free_slot_ids: slots,
        })
    }

    /// Reserves the next [`Slot`] and returns it.
    pub fn reserve(&self) -> Result<JailSlot, FreeSlotsError> {
        let mut slot_ids = self.free_slot_ids.lock().unwrap_or_else(|e| e.into_inner());
        let slot_id = slot_ids.pop().ok_or(FreeSlotsError::NoSlotsRemaining)?;
        let slot = JailSlot::new(slot_id, self.clone());
        Ok(slot)
    }

    /// Restores a [`Slot`] to the set of free slots.
    pub fn restore(&self, slot_id: JailSlotId) {
        let mut slot_ids = self.free_slot_ids.lock().unwrap_or_else(|e| e.into_inner());
        if !slot_ids.contains(&slot_id) {
            slot_ids.push(slot_id);
        }
    }
}
