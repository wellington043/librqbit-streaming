// Modified for WebTorrent Desktop streaming phase 3.

use std::{
    ops::Range,
    time::{Duration, Instant},
};

use serde::Serialize;

pub(crate) const MIN_LOOKAHEAD: u64 = 1024 * 1024;
pub(crate) const MAX_LOOKAHEAD: u64 = 32 * MIN_LOOKAHEAD;
const MIN_SAMPLE: Duration = Duration::from_millis(400);
const MAX_SAMPLE_GAP: Duration = Duration::from_secs(2);
const RECOVERY_COOLDOWN: Duration = Duration::from_secs(20);

#[derive(Default)]
pub(crate) struct Demand {
    // Sorted disjoint torrent-offset ranges.
    ranges: Vec<Range<u64>>,
}

impl Demand {
    pub(crate) fn push(&mut self, mut range: Range<u64>) {
        if range.is_empty() {
            return;
        }
        let first = self.ranges.partition_point(|old| old.end < range.start);
        let last = self.ranges.partition_point(|old| old.start <= range.end);
        for old in &self.ranges[first..last] {
            range.start = range.start.min(old.start);
            range.end = range.end.max(old.end);
        }
        self.ranges.splice(first..last, [range]);
    }

    pub(crate) fn intersects(&self, range: Range<u64>) -> bool {
        !range.is_empty()
            && self
                .ranges
                .iter()
                .any(|r| r.start < range.end && r.end > range.start)
    }

    pub(crate) fn covered_bytes(&self, range: Range<u64>) -> u64 {
        self.ranges
            .iter()
            .map(|r| {
                r.end
                    .min(range.end)
                    .saturating_sub(r.start.max(range.start))
            })
            .sum()
    }

    pub(crate) fn total_bytes(&self) -> u64 {
        self.ranges
            .iter()
            .map(|range| range.end - range.start)
            .sum()
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VerifiedSample {
    pub source: &'static str,
    pub bytes_per_second: u64,
    pub verified_bytes: u64,
    pub interval_ms: u64,
    pub age_ms: u64,
    pub fresh: bool,
}

pub(crate) struct VerifiedRate {
    lookahead: u64,
    anchor: Option<Instant>,
    last_payload: Option<Instant>,
    bytes: u64,
    last_sample: Option<(Instant, u64, Duration, u64)>,
    smoothed_rate: u64,
    direction: i8,
    consecutive: u8,
}

impl Default for VerifiedRate {
    fn default() -> Self {
        Self {
            lookahead: MIN_LOOKAHEAD,
            anchor: None,
            last_payload: None,
            bytes: 0,
            last_sample: None,
            smoothed_rate: 0,
            direction: 0,
            consecutive: 0,
        }
    }
}

impl VerifiedRate {
    pub(crate) fn lookahead(&self) -> u64 {
        self.lookahead
    }

    pub(crate) fn idle(&mut self) {
        self.anchor = None;
        self.last_payload = None;
        self.bytes = 0;
        self.direction = 0;
        self.consecutive = 0;
    }

    pub(crate) fn network_payload(&mut self, now: Instant) {
        // Payload arrival identifies continuous transfer even when a large piece
        // takes several seconds to verify. Only hash-verified bytes contribute to rate.
        if self
            .last_payload
            .is_some_and(|last| now.duration_since(last) > MAX_SAMPLE_GAP)
        {
            self.idle();
        }
        self.last_payload = Some(now);
    }

    pub(crate) fn observe(&mut self, now: Instant, verified_bytes: u64) -> bool {
        if verified_bytes == 0
            || self
                .last_payload
                .is_none_or(|last| now.duration_since(last) > MAX_SAMPLE_GAP)
        {
            return false;
        }
        let Some(anchor) = self.anchor else {
            self.anchor = Some(now);
            return false;
        };

        self.bytes = self.bytes.saturating_add(verified_bytes);
        let elapsed = now.duration_since(anchor);
        if elapsed < MIN_SAMPLE {
            return false;
        }
        let bytes_per_second =
            u64::try_from(u128::from(self.bytes) * 1_000_000_000 / elapsed.as_nanos())
                .unwrap_or(u64::MAX);
        self.last_sample = Some((now, self.bytes, elapsed, bytes_per_second));
        self.anchor = Some(now);
        self.bytes = 0;

        self.smoothed_rate = if self.smoothed_rate == 0 {
            bytes_per_second
        } else {
            u64::try_from(
                (u128::from(self.smoothed_rate) * 3 + u128::from(bytes_per_second)).div_ceil(4),
            )
            .unwrap_or(u64::MAX)
        };
        let target = self
            .smoothed_rate
            .saturating_mul(2)
            .clamp(MIN_LOOKAHEAD, MAX_LOOKAHEAD);
        let margin = self.lookahead / 4;
        let direction = if target > self.lookahead + margin
            || target == MAX_LOOKAHEAD && target > self.lookahead
        {
            1
        } else if target < self.lookahead - margin
            || target == MIN_LOOKAHEAD && target < self.lookahead
        {
            -1
        } else {
            0
        };
        self.consecutive = if direction == self.direction {
            self.consecutive.saturating_add(1)
        } else {
            1
        };
        self.direction = direction;
        if direction != 0 && self.consecutive >= if direction > 0 { 2 } else { 3 } {
            self.lookahead = target;
            self.consecutive = 0;
            return true;
        }
        false
    }

    pub(crate) fn snapshot(&self, now: Instant) -> Option<VerifiedSample> {
        let (sampled_at, verified_bytes, elapsed, bytes_per_second) = self.last_sample?;
        let age = now.saturating_duration_since(sampled_at);
        Some(VerifiedSample {
            source: "networkHashVerified",
            bytes_per_second,
            verified_bytes,
            interval_ms: u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX),
            age_ms: u64::try_from(age.as_millis()).unwrap_or(u64::MAX),
            fresh: age <= MAX_SAMPLE_GAP,
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) struct AcceleratedRetry {
    pub(crate) id: u64,
    fallback_at: Instant,
}

pub(crate) fn normal_retry_deadline(
    previous: Option<AcceleratedRetry>,
    verified_on_connection: bool,
    failed_at: Instant,
    backoff: impl FnOnce() -> Option<Duration>,
) -> Option<Instant> {
    if let Some(retry) = previous.filter(|_| !verified_on_connection) {
        return Some(retry.fallback_at.max(failed_at));
    }
    backoff().map(|delay| failed_at + delay)
}

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoveryAttempt {
    pub id: u64,
    #[serde(skip)]
    failed_at: Instant,
    pub failure_to_dial_ms: u64,
    pub failure_to_handshake_ms: Option<u64>,
    pub failure_to_verified_ms: Option<u64>,
}

#[derive(Default)]
pub(crate) struct Recovery {
    verified_since_attempt: bool,
    last_attempt_at: Option<Instant>,
    eligible_transport_failures: u64,
    attempts: u64,
    last_attempt: Option<RecoveryAttempt>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RecoverySnapshot {
    pub eligible_transport_failures: u64,
    pub attempts: u64,
    pub last_attempt: Option<RecoveryAttempt>,
}

impl Recovery {
    pub(crate) fn record_verified(&mut self) {
        self.verified_since_attempt = true;
    }

    pub(crate) fn record_transport_failure(&mut self) {
        self.eligible_transport_failures = self.eligible_transport_failures.saturating_add(1);
    }

    pub(crate) fn can_accelerate(&self, now: Instant) -> bool {
        self.verified_since_attempt
            && self
                .last_attempt_at
                .is_none_or(|last| now.duration_since(last) >= RECOVERY_COOLDOWN)
    }

    pub(crate) fn accelerate(
        &mut self,
        failed_at: Instant,
        fallback_at: Instant,
        now: Instant,
    ) -> AcceleratedRetry {
        self.verified_since_attempt = false;
        self.last_attempt_at = Some(now);
        self.attempts = self.attempts.saturating_add(1);
        self.last_attempt = Some(RecoveryAttempt {
            id: self.attempts,
            failed_at,
            failure_to_dial_ms: u64::try_from(now.duration_since(failed_at).as_millis())
                .unwrap_or(u64::MAX),
            failure_to_handshake_ms: None,
            failure_to_verified_ms: None,
        });
        AcceleratedRetry {
            id: self.attempts,
            fallback_at,
        }
    }

    pub(crate) fn record_handshake(&mut self, attempt_id: u64, now: Instant) {
        if let Some(attempt) = self
            .last_attempt
            .as_mut()
            .filter(|last| last.id == attempt_id)
        {
            attempt.failure_to_handshake_ms = Some(
                u64::try_from(now.duration_since(attempt.failed_at).as_millis())
                    .unwrap_or(u64::MAX),
            );
        }
    }

    pub(crate) fn record_attempt_verified(&mut self, attempt_id: u64, now: Instant) {
        if let Some(attempt) = self
            .last_attempt
            .as_mut()
            .filter(|last| last.id == attempt_id)
        {
            attempt.failure_to_verified_ms.get_or_insert_with(|| {
                u64::try_from(now.duration_since(attempt.failed_at).as_millis()).unwrap_or(u64::MAX)
            });
        }
    }

    pub(crate) fn snapshot(&self) -> RecoverySnapshot {
        RecoverySnapshot {
            eligible_transport_failures: self.eligible_transport_failures,
            attempts: self.attempts,
            last_attempt: self.last_attempt,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn phase3_demand_uses_exact_union_for_payload_and_rounds_only_intersecting_pieces() {
        let mut demand = Demand::default();
        demand.push(5..25);
        demand.push(23..40);
        demand.push(69..71);
        assert!(demand.intersects(0..16));
        assert!(demand.intersects(32..48));
        assert!(!demand.intersects(40..69));
        assert_eq!(demand.covered_bytes(0..48), 35);
        assert_eq!(demand.covered_bytes(0..100), 37);
    }

    #[test]
    fn phase3_lookahead_changes_with_fresh_hash_checked_samples_and_preserves_estimate_during_idle()
    {
        let start = Instant::now();
        let mut rate = VerifiedRate::default();
        assert_eq!(rate.lookahead(), MIN_LOOKAHEAD);
        assert!(rate.snapshot(start).is_none());
        for i in 0..=4 {
            let now = start + Duration::from_millis(i * 500);
            rate.network_payload(now);
            rate.observe(now, 2 * MIN_LOOKAHEAD);
        }
        let sustained = start + Duration::from_secs(2);
        let sample = rate.snapshot(sustained).unwrap();
        assert!(sample.fresh);
        assert_eq!(sample.bytes_per_second, 4 * MIN_LOOKAHEAD);
        let lookahead = rate.lookahead();
        assert!(lookahead > MIN_LOOKAHEAD);
        let after_idle = sustained + Duration::from_secs(10);
        assert!(!rate.snapshot(after_idle).unwrap().fresh);
        rate.network_payload(after_idle);
        rate.observe(after_idle, MIN_LOOKAHEAD);
        assert_eq!(rate.lookahead(), lookahead);
        assert_eq!(
            rate.snapshot(after_idle).unwrap().bytes_per_second,
            4 * MIN_LOOKAHEAD
        );

        for i in 1..=6 {
            let now = after_idle + Duration::from_millis(i * 500);
            rate.network_payload(now);
            rate.observe(now, MAX_LOOKAHEAD);
        }
        assert_eq!(rate.lookahead(), MAX_LOOKAHEAD);
    }

    #[test]
    fn phase3_continuous_slow_payload_can_verify_large_pieces_seconds_apart() {
        let start = Instant::now();
        let mut rate = VerifiedRate::default();
        for second in 0..=18 {
            let now = start + Duration::from_secs(second);
            rate.network_payload(now);
            if second % 6 == 0 {
                rate.observe(now, 4 * MIN_LOOKAHEAD);
            }
        }

        let sample = rate.snapshot(start + Duration::from_secs(18)).unwrap();
        assert_eq!(sample.bytes_per_second, 4 * MIN_LOOKAHEAD / 6);
        assert!(rate.lookahead() > MIN_LOOKAHEAD);
        rate.idle();
        assert!(rate.lookahead() > MIN_LOOKAHEAD);
    }

    #[test]
    fn phase3_recovery_needs_verified_progress_and_cooldown_after_consuming_episode() {
        let start = Instant::now();
        let mut recovery = Recovery::default();
        assert!(!recovery.can_accelerate(start));
        recovery.record_verified();
        assert!(recovery.can_accelerate(start));
        recovery.accelerate(start, start + Duration::from_secs(10), start);
        assert!(!recovery.can_accelerate(start + Duration::from_secs(30)));
        recovery.record_verified();
        assert!(!recovery.can_accelerate(start + Duration::from_secs(1)));
        assert!(recovery.can_accelerate(start + Duration::from_secs(30)));
    }

    #[test]
    fn phase3_failed_acceleration_keeps_original_normal_deadline_and_backoff_step() {
        let failure = Instant::now();
        let fallback = failure + Duration::from_secs(13);
        let fast_dial = failure + Duration::from_millis(4_500);
        let mut recovery = Recovery::default();
        recovery.record_verified();
        let accelerated = recovery.accelerate(failure, fallback, fast_dial);
        recovery.record_handshake(accelerated.id, fast_dial + Duration::from_millis(50));

        let fast_failure = fast_dial + Duration::from_millis(100);
        assert_eq!(
            normal_retry_deadline(Some(accelerated), false, fast_failure, || panic!(
                "advanced normal backoff"
            )),
            Some(fallback),
        );
        assert!(!recovery.can_accelerate(fast_failure));
        let late_failure = fallback + Duration::from_secs(1);
        assert_eq!(
            normal_retry_deadline(Some(accelerated), false, late_failure, || panic!(
                "advanced normal backoff"
            )),
            Some(late_failure),
        );
        assert_eq!(
            normal_retry_deadline(Some(accelerated), true, late_failure, || Some(
                Duration::from_secs(10)
            )),
            Some(late_failure + Duration::from_secs(10)),
        );
    }
}
