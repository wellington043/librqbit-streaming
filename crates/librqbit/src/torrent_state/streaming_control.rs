// Modified for the librqbit-streaming fork.
//! Bounded caller feedback for encoded-media readers.

use std::time::{Duration, Instant};

use serde::Serialize;

use super::streaming_policy::{MAX_LOOKAHEAD, MIN_LOOKAHEAD};

/// Scheduling class. Higher classes are exhausted before lower classes.
/// Readers in one class are interleaved one piece at a time.
#[derive(Clone, Copy, Debug, Default, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum StreamPriority {
    /// Metadata inspection or speculative format probes.
    Probe,
    /// Background buffering for a future playback position.
    Prefetch,
    /// The currently consumed media stream.
    #[default]
    Playback,
}

/// Options for an explicitly controlled reader on a demand-only streaming torrent.
#[derive(Clone, Copy, Debug, Default)]
pub struct StreamingReadOptions {
    pub priority: StreamPriority,
    /// Relative to opening the reader, captured once using a monotonic clock.
    /// At most 24 hours; zero means immediately due. Within a priority, earlier
    /// deadlines order the round; its starting reader rotates on successive
    /// acquisitions to keep equal-priority readers fair. A deadline is
    /// an ordering hint, not a timeout or an automatic cancellation time.
    pub deadline: Option<Duration>,
}

/// Caller-supplied feedback for the current reader generation.
///
/// The cursor is the reader's current byte position. The library does not parse
/// media timestamps or infer codec bitrate. The requested lookahead is the
/// encoded-media consumption rate multiplied by `target_buffer - buffered`,
/// rounded up and clamped to 1–32 MiB (then clipped to the reader's range).
/// Even a full buffer retains the minimum window for reads to make progress.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PlaybackHint {
    /// Encoded-media bytes consumed per second, not network download throughput.
    /// Must be in `1..=1_073_741_824` (1 GiB/s).
    pub consumption_bytes_per_second: u64,
    /// Buffered playback duration, at most one hour.
    pub buffered: Duration,
    /// Desired buffered playback duration, nonzero and at most one hour.
    pub target_buffer: Duration,
}

/// A rejected control update leaves the reader and its demand unchanged.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum StreamingControlError {
    #[error("streaming control requires demand_only_streaming")]
    PolicyDisabled,
    #[error("playback generation {supplied} is stale; current generation is {current}")]
    StaleGeneration { current: u64, supplied: u64 },
    #[error("consumption rate must be between 1 byte/s and 1 GiB/s")]
    InvalidConsumptionRate,
    #[error("buffered duration must not exceed one hour")]
    InvalidBufferedDuration,
    #[error("target buffer must be nonzero and not exceed one hour")]
    InvalidTargetDuration,
    #[error("relative deadline must not exceed 24 hours")]
    InvalidDeadline,
    #[error("stream generation exhausted")]
    GenerationExhausted,
}

pub(crate) struct ReaderControl {
    pub(crate) priority: StreamPriority,
    pub(crate) deadline: Option<Instant>,
    pub(crate) generation: u64,
    pub(crate) playback: Option<PlaybackHint>,
}

impl ReaderControl {
    pub(crate) fn new(
        options: StreamingReadOptions,
        now: Instant,
    ) -> Result<Self, StreamingControlError> {
        let deadline = options
            .deadline
            .map(|delay| {
                if delay > Duration::from_secs(24 * 60 * 60) {
                    return Err(StreamingControlError::InvalidDeadline);
                }
                now.checked_add(delay)
                    .ok_or(StreamingControlError::InvalidDeadline)
            })
            .transpose()?;
        Ok(Self {
            priority: options.priority,
            deadline,
            generation: 0,
            playback: None,
        })
    }

    pub(crate) fn update_playback(
        &mut self,
        generation: u64,
        hint: PlaybackHint,
    ) -> Result<(), StreamingControlError> {
        if generation != self.generation {
            return Err(StreamingControlError::StaleGeneration {
                current: self.generation,
                supplied: generation,
            });
        }
        if !(1..=1024 * 1024 * 1024).contains(&hint.consumption_bytes_per_second) {
            return Err(StreamingControlError::InvalidConsumptionRate);
        }
        let max_buffer = Duration::from_secs(60 * 60);
        if hint.buffered > max_buffer {
            return Err(StreamingControlError::InvalidBufferedDuration);
        }
        if hint.target_buffer.is_zero() || hint.target_buffer > max_buffer {
            return Err(StreamingControlError::InvalidTargetDuration);
        }
        self.playback = Some(hint);
        Ok(())
    }

    pub(crate) fn seek(&mut self) -> Result<(), StreamingControlError> {
        self.generation = self
            .generation
            .checked_add(1)
            .ok_or(StreamingControlError::GenerationExhausted)?;
        self.playback = None;
        Ok(())
    }

    pub(crate) fn lookahead(&self, fallback: u64) -> u64 {
        let Some(hint) = self.playback else {
            return fallback;
        };
        let deficit = hint.target_buffer.saturating_sub(hint.buffered);
        let bytes = (u128::from(hint.consumption_bytes_per_second) * deficit.as_nanos())
            .div_ceil(1_000_000_000)
            .clamp(u128::from(MIN_LOOKAHEAD), u128::from(MAX_LOOKAHEAD));
        // The clamp above bounds the conversion independently of caller input.
        u64::try_from(bytes).expect("bounded playback window")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hint(rate: u64, buffered: u64, target: u64) -> PlaybackHint {
        PlaybackHint {
            consumption_bytes_per_second: rate,
            buffered: Duration::from_secs(buffered),
            target_buffer: Duration::from_secs(target),
        }
    }

    #[test]
    fn playback_window_uses_buffer_deficit_and_bounds() {
        let mut control =
            ReaderControl::new(StreamingReadOptions::default(), Instant::now()).unwrap();
        control
            .update_playback(0, hint(2 * 1024 * 1024, 3, 7))
            .unwrap();
        assert_eq!(control.lookahead(1024 * 1024), 8 * 1024 * 1024);
        control
            .update_playback(0, hint(2 * 1024 * 1024, 8, 7))
            .unwrap();
        assert_eq!(control.lookahead(16 * 1024 * 1024), 1024 * 1024);
        control
            .update_playback(0, hint(1024 * 1024 * 1024, 0, 3600))
            .unwrap();
        assert_eq!(control.lookahead(1024 * 1024), 32 * 1024 * 1024);
    }

    #[test]
    fn fractional_seconds_round_up_without_float_conversion() {
        let mut control =
            ReaderControl::new(StreamingReadOptions::default(), Instant::now()).unwrap();
        control
            .update_playback(
                0,
                PlaybackHint {
                    consumption_bytes_per_second: 3 * 1024 * 1024 + 1,
                    buffered: Duration::ZERO,
                    target_buffer: Duration::from_millis(500),
                },
            )
            .unwrap();
        assert_eq!(control.lookahead(1), 1_572_865);
    }

    #[test]
    fn seek_clears_feedback_and_rejects_old_generation_without_mutation() {
        let mut control =
            ReaderControl::new(StreamingReadOptions::default(), Instant::now()).unwrap();
        control
            .update_playback(0, hint(2 * 1024 * 1024, 0, 8))
            .unwrap();
        control.seek().unwrap();
        assert_eq!(control.generation, 1);
        assert_eq!(control.lookahead(1024 * 1024), 1024 * 1024);
        assert_eq!(
            control.update_playback(0, hint(2 * 1024 * 1024, 0, 8)),
            Err(StreamingControlError::StaleGeneration {
                current: 1,
                supplied: 0
            })
        );
        assert_eq!(control.lookahead(1024 * 1024), 1024 * 1024);
        control
            .update_playback(1, hint(2 * 1024 * 1024, 0, 4))
            .unwrap();
        assert_eq!(control.lookahead(1024 * 1024), 8 * 1024 * 1024);
    }

    #[test]
    fn malformed_feedback_does_not_replace_previous_window() {
        let mut control =
            ReaderControl::new(StreamingReadOptions::default(), Instant::now()).unwrap();
        control
            .update_playback(0, hint(2 * 1024 * 1024, 0, 4))
            .unwrap();
        for invalid in [
            hint(0, 0, 4),
            hint(u64::MAX, 0, 4),
            hint(1, 0, 0),
            hint(1, 3601, 4),
            hint(1, 0, 3601),
            PlaybackHint {
                target_buffer: Duration::MAX,
                ..hint(1, 0, 4)
            },
        ] {
            assert!(control.update_playback(0, invalid).is_err());
            assert_eq!(control.lookahead(1024 * 1024), 8 * 1024 * 1024);
        }
    }

    #[test]
    fn relative_deadline_is_captured_once_and_extreme_values_are_rejected() {
        let now = Instant::now();
        let control = ReaderControl::new(
            StreamingReadOptions {
                deadline: Some(Duration::from_secs(5)),
                ..Default::default()
            },
            now,
        )
        .unwrap();
        assert_eq!(control.deadline, Some(now + Duration::from_secs(5)));
        assert!(
            ReaderControl::new(
                StreamingReadOptions {
                    deadline: Some(Duration::MAX),
                    ..Default::default()
                },
                now
            )
            .is_err()
        );
        assert!(
            ReaderControl::new(
                StreamingReadOptions {
                    deadline: Some(Duration::ZERO),
                    ..Default::default()
                },
                now
            )
            .is_ok()
        );
    }

    #[test]
    fn exhausted_generation_rejects_seek_without_reusing_old_feedback_id() {
        let mut control =
            ReaderControl::new(StreamingReadOptions::default(), Instant::now()).unwrap();
        control.generation = u64::MAX;
        control
            .update_playback(u64::MAX, hint(2 * 1024 * 1024, 0, 4))
            .unwrap();
        assert_eq!(
            control.seek(),
            Err(StreamingControlError::GenerationExhausted)
        );
        assert_eq!(control.generation, u64::MAX);
        assert_eq!(control.lookahead(1024 * 1024), 8 * 1024 * 1024);
    }
}
