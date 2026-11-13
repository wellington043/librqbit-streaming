//! Shared payload-request arbitration across independent torrent sessions.
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Mutex as AsyncMutex, Notify, OwnedSemaphorePermit, Semaphore};

const HEALTH_LEASE: Duration = Duration::from_secs(3);

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
    next: AsyncMutex<Instant>,
}
struct ForegroundState {
    active: bool,
    healthy: bool,
    observed_at: Instant,
}
impl ForegroundState {
    fn allowed(&self, now: Instant) -> bool {
        !self.active
            || (self.healthy && now.saturating_duration_since(self.observed_at) < HEALTH_LEASE)
    }
}
struct Inner {
    foreground: Mutex<ForegroundState>,
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
                foreground: Mutex::new(ForegroundState {
                    active: false,
                    healthy: false,
                    observed_at: Instant::now(),
                }),
                changed: Notify::new(),
                lanes: [64, 8, 4].map(|count| Lane {
                    slots: Arc::new(Semaphore::new(count)),
                    counters: Arc::default(),
                    next: AsyncMutex::new(Instant::now()),
                }),
            }),
        }
    }
}
impl DownloadArbiter {
    pub fn set_foreground(&self, active: bool, healthy: bool) {
        let now = Instant::now();
        let mut state = self.inner.foreground.lock();
        let was_allowed = state.allowed(now);
        let changed = state.active != active || state.healthy != healthy;
        state.active = active;
        state.healthy = active && healthy;
        state.observed_at = now;
        let notify = changed || was_allowed != state.allowed(now);
        drop(state);
        if notify {
            self.inner.changed.notify_waiters();
        }
    }
    pub async fn wait_allowed(&self, class: DownloadClass) {
        loop {
            let changed = self.inner.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if class == DownloadClass::Foreground
                || self.inner.foreground.lock().allowed(Instant::now())
            {
                return;
            }
            changed.await;
        }
    }
    /// Run a synchronous request-enqueue operation only while the latest lease permits it.
    /// The lock makes this operation ordered with foreground transitions.
    pub(crate) fn with_request_allowed<T>(
        &self,
        class: DownloadClass,
        action: impl FnOnce() -> T,
    ) -> Option<T> {
        if class == DownloadClass::Foreground {
            return Some(action());
        }
        let state = self.inner.foreground.lock();
        if state.allowed(Instant::now()) {
            Some(action())
        } else {
            None
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
        if class != DownloadClass::Foreground {
            // Serialize reservations: cancellation while sleeping drops this lock and
            // never advances the next deadline for work that was not admitted.
            let mut next = lane.next.lock().await;
            let bytes_per_second = if class == DownloadClass::Preparation {
                2 * 1024 * 1024
            } else {
                1024 * 1024
            };
            loop {
                self.wait_allowed(class).await;
                let changed = self.inner.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let now = Instant::now();
                {
                    let state = self.inner.foreground.lock();
                    if !state.active {
                        break;
                    }
                    if !state.allowed(now) {
                        continue;
                    }
                    if *next <= now {
                        *next =
                            now + Duration::from_secs_f64(bytes as f64 / bytes_per_second as f64);
                        break;
                    }
                }
                tokio::select! {
                    _ = tokio::time::sleep_until((*next).into()) => {},
                    _ = &mut changed => {},
                }
            }
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
        let state = self.inner.foreground.lock();
        ArbiterSnapshot {
            foreground_active: state.active,
            foreground_healthy: state.active && state.allowed(Instant::now()),
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
    #[tokio::test]
    async fn cancelled_pacing_does_not_reserve_phantom_future_bandwidth() {
        let arbiter = DownloadArbiter::default();
        arbiter.set_foreground(true, true);
        let deadline = Instant::now() + Duration::from_millis(500);
        *arbiter.inner.lanes[DownloadClass::Background.index()]
            .next
            .lock()
            .await = deadline;
        for _ in 0..2 {
            assert!(
                tokio::time::timeout(
                    Duration::from_millis(20),
                    arbiter.acquire(DownloadClass::Background, 1024 * 1024)
                )
                .await
                .is_err()
            );
        }
        assert_eq!(
            *arbiter.inner.lanes[DownloadClass::Background.index()]
                .next
                .lock()
                .await,
            deadline
        );
    }

    #[tokio::test]
    async fn stale_healthy_signal_stops_new_background_until_refreshed() {
        let arbiter = DownloadArbiter::default();
        arbiter.set_foreground(true, true);
        tokio::time::sleep(Duration::from_millis(3200)).await;
        assert!(!arbiter.snapshot().foreground_healthy);
        assert!(
            tokio::time::timeout(
                Duration::from_millis(30),
                arbiter.acquire(DownloadClass::Background, 16384)
            )
            .await
            .is_err()
        );
        arbiter.set_foreground(true, true);
        assert!(
            tokio::time::timeout(
                Duration::from_secs(1),
                arbiter.acquire(DownloadClass::Background, 16384)
            )
            .await
            .is_ok()
        );
    }

    #[tokio::test]
    async fn pacing_wakes_promptly_when_foreground_closes() {
        let arbiter = DownloadArbiter::default();
        arbiter.set_foreground(true, true);
        *arbiter.inner.lanes[DownloadClass::Background.index()]
            .next
            .lock()
            .await = Instant::now() + Duration::from_secs(2);
        let waiting = {
            let arbiter = arbiter.clone();
            tokio::spawn(async move { arbiter.acquire(DownloadClass::Background, 16384).await })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        arbiter.set_foreground(false, false);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), waiting)
                .await
                .is_ok()
        );
    }

    #[test]
    fn request_gate_rechecks_health_at_the_enqueue_boundary() {
        let arbiter = DownloadArbiter::default();
        arbiter.set_foreground(true, true);
        assert_eq!(
            arbiter.with_request_allowed(DownloadClass::Preparation, || 1),
            Some(1)
        );
        arbiter.set_foreground(true, false);
        assert_eq!(
            arbiter.with_request_allowed(DownloadClass::Preparation, || panic!(
                "lower priority issued while blocked"
            )),
            None::<()>
        );
        assert_eq!(
            arbiter.with_request_allowed(DownloadClass::Foreground, || 2),
            Some(2)
        );
        arbiter.set_foreground(false, false);
        assert_eq!(
            arbiter.with_request_allowed(DownloadClass::Background, || 3),
            Some(3)
        );
    }
}
