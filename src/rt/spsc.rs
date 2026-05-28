//! SPSC channel over `rtrb` with a notify counter for `idle_wait`.
//!
//! `rtrb` does not expose the producer/consumer index addresses, so we keep a
//! separate `AtomicU64` counter that the producer bumps on every successful
//! push. Consumers monitor that counter's address with `idle_wait`, giving
//! the same wake-on-write behavior the old `low-latency-utils` SPSC had via
//! `tail_ptr()`.
//!
//! The wrapper expects the SPSC contract: at most one thread calls `try_push`
//! and at most one thread calls `try_pop` at any time. The producer side uses
//! `&self` (with `UnsafeCell`) so it can live behind an `Arc` / inside
//! `Sender::trigger(&self, ...)`; this is sound under the SPSC contract.

use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[repr(align(64))]
pub struct Notifier {
    counter: AtomicU64,
}

impl Notifier {
    fn new() -> Self {
        Self {
            counter: AtomicU64::new(0),
        }
    }

    #[inline(always)]
    fn notify(&self) {
        self.counter.fetch_add(1, Ordering::Release);
    }

    /// Pointer to monitor with `idle_wait`.
    #[inline(always)]
    pub fn monitor_addr(&self) -> *const u8 {
        &self.counter as *const _ as *const u8
    }
}

pub struct Producer<T> {
    inner: UnsafeCell<rtrb::Producer<T>>,
    notify: Arc<Notifier>,
}

// SAFETY: SPSC contract — exactly one thread calls try_push at any time.
unsafe impl<T: Send> Send for Producer<T> {}
unsafe impl<T: Send> Sync for Producer<T> {}

impl<T> Producer<T> {
    /// # Safety contract
    /// Caller must guarantee single-producer access (no concurrent `try_push`).
    #[inline(always)]
    pub fn try_push(&self, value: T) -> Result<(), T> {
        let inner = unsafe { &mut *self.inner.get() };
        match inner.push(value) {
            Ok(()) => {
                self.notify.notify();
                Ok(())
            }
            Err(rtrb::PushError::Full(v)) => Err(v),
        }
    }
}

pub struct Consumer<T> {
    inner: rtrb::Consumer<T>,
    notify: Arc<Notifier>,
}

impl<T> Consumer<T> {
    #[inline(always)]
    pub fn try_pop(&mut self) -> Option<T> {
        self.inner.pop().ok()
    }

    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Address of the producer's notify counter; pass to `idle_wait`.
    #[inline(always)]
    pub fn monitor_addr(&self) -> *const u8 {
        self.notify.monitor_addr()
    }
}

pub fn channel<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    let (tx, rx) = rtrb::RingBuffer::<T>::new(capacity);
    let notify = Arc::new(Notifier::new());
    (
        Producer {
            inner: UnsafeCell::new(tx),
            notify: notify.clone(),
        },
        Consumer {
            inner: rx,
            notify,
        },
    )
}
