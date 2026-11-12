// Modified for the librqbit-streaming fork.
//! Streaming request flow. Measurements describe transport responses, not verified payload.

use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use librqbit_core::{
    constants::CHUNK_SIZE,
    lengths::{ChunkInfo, ValidPieceIndex},
};
use serde::Serialize;

const INITIAL_WINDOW: usize = 4;
const SAMPLE_MAX_AGE: Duration = Duration::from_secs(10);

/// A transport-only snapshot for an opt-in streaming peer. Canceled/unsolicited
/// responses never enter the estimator. Ages and latencies use monotonic time.
#[derive(Debug, Clone, Serialize)]
pub struct PeerRequestFlowSnapshot {
    pub request_window: usize,
    /// Minimum of the configured maximum (zero is normalized to one) and reqq.
    pub request_limit: usize,
    pub inflight_requests: usize,
    pub response_latency_ms: Option<u64>,
    pub response_bytes_per_second: Option<u64>,
    pub sample_age_ms: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PeerRequestLimitError {
    #[error("streaming peer advertised a zero request limit")]
    ZeroPeerLimit,
}

pub(crate) fn streaming_request_limit(
    configured: usize,
    peer_limit: Option<u32>,
) -> Result<usize, PeerRequestLimitError> {
    match peer_limit {
        Some(0) => Err(PeerRequestLimitError::ZeroPeerLimit),
        Some(limit) => Ok(configured.max(1).min(limit as usize)),
        None => Ok(configured.max(1)),
    }
}

#[derive(Debug)]
pub(crate) struct AdaptiveRequestWindow {
    limit: usize,
    target: usize,
    issued: HashMap<ChunkInfo, Instant>,
    latency_seconds: Option<f64>,
    bytes_per_second: Option<f64>,
    last_response: Option<Instant>,
}

impl AdaptiveRequestWindow {
    pub(crate) fn new(limit: usize) -> Self {
        let limit = limit.max(1);
        Self {
            limit,
            target: INITIAL_WINDOW.min(limit),
            issued: HashMap::new(),
            latency_seconds: None,
            bytes_per_second: None,
            last_response: None,
        }
    }

    pub(crate) fn set_limit(&mut self, limit: usize) {
        self.limit = limit.max(1);
        self.target = self.target.min(self.limit);
    }

    fn stale(&self, now: Instant) -> bool {
        self.last_response
            .is_some_and(|last| now.saturating_duration_since(last) > SAMPLE_MAX_AGE)
    }

    fn reset_samples(&mut self) {
        self.target = INITIAL_WINDOW.min(self.limit);
        self.latency_seconds = None;
        self.bytes_per_second = None;
        self.last_response = None;
    }

    pub(crate) fn window(&self, now: Instant) -> usize {
        if self.stale(now) {
            INITIAL_WINDOW.min(self.limit)
        } else {
            self.target
        }
    }

    pub(crate) fn on_request(&mut self, chunk: ChunkInfo, now: Instant) {
        if self.stale(now) {
            self.reset_samples();
        }
        // The caller enforces the window under the same peer lock. Do not
        // replace a timestamp on duplicate requests or grow beyond the cap.
        if self.issued.len() < self.limit {
            self.issued.entry(chunk).or_insert(now);
        }
    }

    pub(crate) fn on_response(&mut self, chunk: &ChunkInfo, now: Instant) {
        let Some(issued) = self.issued.remove(chunk) else {
            return;
        };
        let elapsed = now.saturating_duration_since(issued);
        if elapsed > SAMPLE_MAX_AGE || self.stale(now) {
            self.reset_samples();
            return;
        }
        // A lower bound avoids division by zero and bursts at the timer's
        // precision from producing infinite/unbounded estimates.
        let latency = elapsed.as_secs_f64().max(0.001);
        let interval = self
            .last_response
            .map(|last| now.saturating_duration_since(last).as_secs_f64().max(0.001))
            .unwrap_or(latency);
        let rate = f64::from(chunk.size) / interval;
        let smooth = |old: Option<f64>, new| old.map_or(new, |old| old * 0.75 + new * 0.25);
        let latency = smooth(self.latency_seconds, latency);
        let rate = smooth(self.bytes_per_second, rate);
        self.latency_seconds = Some(latency);
        self.bytes_per_second = Some(rate);
        self.last_response = Some(now);

        // Enough data for two observed round trips, within a 250ms..2s
        // horizon. Growth is additive; a slower link can shrink immediately.
        let horizon = (latency * 2.0).clamp(0.25, 2.0);
        // Intentionally round the estimate up, then use Rust's saturating
        // float-to-integer conversion before enforcing the configured cap.
        #[allow(clippy::cast_possible_truncation)]
        let desired =
            ((rate * horizon / f64::from(CHUNK_SIZE)).ceil() as usize).clamp(1, self.limit);
        self.target = desired.min(self.target.saturating_add(1));
    }

    pub(crate) fn cancel_piece(&mut self, piece: ValidPieceIndex) {
        let before = self.issued.len();
        self.issued.retain(|chunk, _| chunk.piece_index != piece);
        if before != self.issued.len() {
            // A seek or reassignment breaks the throughput interval. Never
            // use the subsequent late response as an artificially fast sample.
            self.reset_samples();
        }
    }

    pub(crate) fn snapshot(&self, now: Instant) -> PeerRequestFlowSnapshot {
        let fresh = !self.stale(now);
        // Diagnostics intentionally discard fractional milliseconds/bytes per
        // second. Rust's float casts saturate rather than wrapping on overflow.
        #[allow(clippy::cast_possible_truncation)]
        let response_latency_ms = self
            .latency_seconds
            .filter(|_| fresh)
            .map(|s| (s * 1000.0) as u64);
        #[allow(clippy::cast_possible_truncation)]
        let response_bytes_per_second = self
            .bytes_per_second
            .filter(|_| fresh)
            .map(|rate| rate as u64);
        PeerRequestFlowSnapshot {
            request_window: self.window(now),
            request_limit: self.limit,
            inflight_requests: self.issued.len(),
            response_latency_ms,
            response_bytes_per_second,
            sample_age_ms: self.last_response.map(|last| {
                u64::try_from(now.saturating_duration_since(last).as_millis()).unwrap_or(u64::MAX)
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use librqbit_core::lengths::Lengths;

    fn chunk(offset: u32) -> ChunkInfo {
        let lengths = Lengths::new(1024 * 1024, 1024 * 1024).unwrap();
        lengths
            .iter_chunk_infos(lengths.validate_piece_index(0).unwrap())
            .nth(offset as usize)
            .unwrap()
    }

    fn respond(flow: &mut AdaptiveRequestWindow, issued: Instant, latency: Duration) {
        flow.on_request(chunk(0), issued);
        flow.on_response(&chunk(0), issued + latency);
    }

    #[test]
    fn streaming_flow_slow_peer_reduces_window_without_stopping_requests() {
        let now = Instant::now();
        let mut flow = AdaptiveRequestWindow::new(128);
        respond(&mut flow, now, Duration::from_secs(2));
        assert_eq!(flow.window(now + Duration::from_secs(2)), 1);
        assert!(
            flow.snapshot(now + Duration::from_secs(2))
                .response_latency_ms
                .unwrap()
                >= 2000
        );
    }

    #[test]
    fn streaming_flow_fast_responses_grow_within_both_limits() {
        let now = Instant::now();
        let mut flow = AdaptiveRequestWindow::new(12);
        let initial = flow.window(now);
        for i in 0..40 {
            respond(
                &mut flow,
                now + Duration::from_millis(i * 10),
                Duration::from_millis(5),
            );
        }
        assert!(flow.window(now + Duration::from_millis(400)) > initial);
        assert!(flow.window(now + Duration::from_millis(400)) <= 12);
        flow.set_limit(2);
        assert_eq!(flow.window(now + Duration::from_millis(400)), 2);
        flow.set_limit(1);
        assert_eq!(flow.window(now + Duration::from_millis(400)), 1);
    }

    #[test]
    fn streaming_flow_stale_response_and_idle_gap_cannot_keep_large_window() {
        let now = Instant::now();
        let mut flow = AdaptiveRequestWindow::new(128);
        for i in 0..40 {
            respond(
                &mut flow,
                now + Duration::from_millis(i * 10),
                Duration::from_millis(5),
            );
        }
        assert!(flow.window(now + Duration::from_secs(1)) > 4);
        assert!(flow.window(now + Duration::from_secs(31)) <= 4);
        respond(
            &mut flow,
            now + Duration::from_secs(31),
            Duration::from_secs(31),
        );
        assert!(
            flow.snapshot(now + Duration::from_secs(62))
                .response_latency_ms
                .is_none()
        );
        assert!(flow.window(now + Duration::from_secs(62)) >= 1);
    }

    #[test]
    fn streaming_flow_latency_increase_reduces_window() {
        let now = Instant::now();
        let mut flow = AdaptiveRequestWindow::new(64);
        for i in 0..40 {
            respond(
                &mut flow,
                now + Duration::from_millis(i * 10),
                Duration::from_millis(5),
            );
        }
        let fast = flow.window(now + Duration::from_secs(1));
        for i in 0..40 {
            respond(
                &mut flow,
                now + Duration::from_secs(2 + i * 3),
                Duration::from_secs(2),
            );
        }
        assert!(flow.window(now + Duration::from_secs(121)) < fast);
        assert!(flow.window(now + Duration::from_secs(121)) >= 1);
    }

    #[test]
    fn streaming_flow_cancelled_request_never_becomes_latency_sample() {
        let now = Instant::now();
        let mut flow = AdaptiveRequestWindow::new(128);
        let first = chunk(0);
        flow.on_request(first, now);
        flow.cancel_piece(first.piece_index);
        flow.on_response(&first, now + Duration::from_millis(1));
        let snapshot = flow.snapshot(now + Duration::from_millis(2));
        assert_eq!(snapshot.inflight_requests, 0);
        assert_eq!(snapshot.response_latency_ms, None);
        assert_eq!(snapshot.response_bytes_per_second, None);
    }

    #[test]
    fn streaming_flow_zero_peer_limit_is_rejected_and_configured_zero_is_normalized() {
        assert_eq!(
            streaming_request_limit(8, Some(0)),
            Err(PeerRequestLimitError::ZeroPeerLimit)
        );
        assert_eq!(streaming_request_limit(0, Some(4)), Ok(1));
        assert_eq!(streaming_request_limit(8, Some(2)), Ok(2));
        assert_eq!(streaming_request_limit(8, Some(u32::MAX)), Ok(8));
        assert_eq!(streaming_request_limit(8, None), Ok(8));
    }
}
