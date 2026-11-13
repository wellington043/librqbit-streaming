//! Shared payload-request arbitration across independent torrent sessions.
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum DownloadClass {
    #[default]
    Foreground,
    Preparation,
    Background,
}
impl DownloadClass {
    fn index(self) -> usize {
        self as usize
    }
}

#[derive(Default)]
struct Counters {
    issued: AtomicU64,
    received: AtomicU64,
    verified: AtomicU64,
    in_flight: AtomicU64,
    cancelled: AtomicU64,
    waited: AtomicU64,
}
struct Lane {
    slots: Arc<Semaphore>,
    counters: Arc<Counters>,
    next: Mutex<Instant>,
}
struct Inner {
    foreground: AtomicBool,
    healthy: AtomicBool,
    changed: Notify,
    lanes: [Lane; 3],
}
#[derive(Clone)]
pub struct DownloadArbiter {
    inner: Arc<Inner>,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LaneSnapshot {
    pub issued_bytes: u64,
    pub received_bytes: u64,
    pub verified_bytes: u64,
    pub in_flight_bytes: u64,
    pub cancelled_bytes: u64,
    pub wait_ms: u64,
}
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ArbiterSnapshot {
    pub foreground_active: bool,
    pub foreground_healthy: bool,
    pub classes: [LaneSnapshot; 3],
    pub max_in_flight_requests: usize,
}
pub struct DownloadPermit {
    _slot: OwnedSemaphorePermit,
    counters: Arc<Counters>,
    bytes: u32,
    received: bool,
    issued: bool,
}
impl DownloadPermit {
    pub(crate) fn mark_issued(&mut self) {
        if !self.issued {
            self.counters
                .issued
                .fetch_add(self.bytes as u64, Ordering::Relaxed);
            self.issued = true;
        }
    }
    pub fn received(mut self, bytes: u32) {
        self.counters
            .received
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.received = true;
    }
}
impl Drop for DownloadPermit {
    fn drop(&mut self) {
        self.counters
            .in_flight
            .fetch_sub(self.bytes as u64, Ordering::Relaxed);
        if !self.received {
            self.counters
                .cancelled
                .fetch_add(self.bytes as u64, Ordering::Relaxed);
        }
    }
}
impl Default for DownloadArbiter {
    fn default() -> Self {
        Self {
            inner: Arc::new(Inner {
                foreground: AtomicBool::new(false),
                healthy: AtomicBool::new(false),
                changed: Notify::new(),
                lanes: [64, 8, 4].map(|count| Lane {
                    slots: Arc::new(Semaphore::new(count)),
                    counters: Arc::default(),
                    next: Mutex::new(Instant::now()),
                }),
            }),
        }
    }
}
impl DownloadArbiter {
    pub fn set_foreground(&self, active: bool, healthy: bool) {
        let was_active = self.inner.foreground.swap(active, Ordering::SeqCst);
        let was_healthy = self.inner.healthy.swap(healthy, Ordering::SeqCst);
        if was_active != active || was_healthy != healthy {
            self.inner.changed.notify_waiters();
        }
    }
    pub async fn wait_allowed(&self, class: DownloadClass) {
        loop {
            let changed = self.inner.changed.notified();
            if class == DownloadClass::Foreground
                || !self.inner.foreground.load(Ordering::SeqCst)
                || self.inner.healthy.load(Ordering::SeqCst)
            {
                return;
            }
            changed.await;
        }
    }
    pub async fn acquire(&self, class: DownloadClass, bytes: u32) -> DownloadPermit {
        let start = Instant::now();
        self.wait_allowed(class).await;
        let lane = &self.inner.lanes[class.index()];
        let slot = lane
            .slots
            .clone()
            .acquire_owned()
            .await
            .expect("arbiter semaphore never closes");
        self.wait_allowed(class).await;
        if class != DownloadClass::Foreground && self.inner.foreground.load(Ordering::SeqCst) {
            // Explicit payload ceilings while playback has headroom: preparation 2 MiB/s,
            // background 1 MiB/s. No fake measured bandwidth inferred from these limits.
            let bytes_per_second = if class == DownloadClass::Preparation {
                2 * 1024 * 1024
            } else {
                1024 * 1024
            };
            let until = {
                let mut next = lane.next.lock();
                let until = (*next).max(Instant::now());
                *next = until + Duration::from_secs_f64(bytes as f64 / bytes_per_second as f64);
                until
            };
            tokio::time::sleep_until(until.into()).await;
            self.wait_allowed(class).await;
        }
        lane.counters
            .waited
            .fetch_add(start.elapsed().as_millis() as u64, Ordering::Relaxed);
        lane.counters
            .in_flight
            .fetch_add(bytes as u64, Ordering::Relaxed);
        DownloadPermit {
            _slot: slot,
            counters: lane.counters.clone(),
            bytes,
            received: false,
            issued: false,
        }
    }
    pub fn record_verified(&self, class: DownloadClass, bytes: u64) {
        self.inner.lanes[class.index()]
            .counters
            .verified
            .fetch_add(bytes, Ordering::Relaxed);
    }
    pub fn snapshot(&self) -> ArbiterSnapshot {
        ArbiterSnapshot {
            foreground_active: self.inner.foreground.load(Ordering::SeqCst),
            foreground_healthy: self.inner.healthy.load(Ordering::SeqCst),
            max_in_flight_requests: 76,
            classes: std::array::from_fn(|i| {
                let c = &self.inner.lanes[i].counters;
                LaneSnapshot {
                    issued_bytes: c.issued.load(Ordering::Relaxed),
                    received_bytes: c.received.load(Ordering::Relaxed),
                    verified_bytes: c.verified.load(Ordering::Relaxed),
                    in_flight_bytes: c.in_flight.load(Ordering::Relaxed),
                    cancelled_bytes: c.cancelled.load(Ordering::Relaxed),
                    wait_ms: c.waited.load(Ordering::Relaxed),
                }
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    #[tokio::test]
    async fn arbitration_blocks_background_until_healthy_and_releases_cancelled_slots() {
        let arbiter = DownloadArbiter::default();
        arbiter.set_foreground(true, false);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(10),
                arbiter.acquire(DownloadClass::Background, 16384)
            )
            .await
            .is_err()
        );
        let foreground = arbiter.acquire(DownloadClass::Foreground, 16384).await;
        drop(foreground);
        arbiter.set_foreground(true, true);
        let background = tokio::time::timeout(
            Duration::from_secs(1),
            arbiter.acquire(DownloadClass::Background, 16384),
        )
        .await
        .unwrap();
        assert_eq!(arbiter.snapshot().classes[2].in_flight_bytes, 16384);
        background.received(16384);
        assert_eq!(arbiter.snapshot().classes[2].in_flight_bytes, 0);
        assert_eq!(arbiter.snapshot().classes[2].received_bytes, 16384);
    }
}
