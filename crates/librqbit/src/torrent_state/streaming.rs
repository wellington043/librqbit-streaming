// Modified for WebTorrent Desktop streaming phase 3.

use std::{
    collections::{HashMap, HashSet, VecDeque},
    io::SeekFrom,
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Poll, Waker},
    time::{Duration, Instant},
};

use anyhow::Context;
use parking_lot::Mutex;
use serde::Serialize;

use librqbit_core::lengths::{CurrentPiece, Lengths, ValidPieceIndex};
use tokio::{
    io::{AsyncRead, AsyncSeek},
    sync::{Notify, OwnedSemaphorePermit},
};
use tracing::{debug, trace};

use crate::{ManagedTorrent, file_info::FileInfo, storage::TorrentStorage};

use super::{
    ManagedTorrentHandle, TorrentMetadata,
    streaming_control::{
        PlaybackHint, ReaderControl, StreamPriority, StreamingControlError, StreamingReadOptions,
    },
    streaming_policy::{Demand, Recovery, RecoverySnapshot, VerifiedRate, VerifiedSample},
};

type StreamId = usize;

// Retain the upstream scheduling window for torrents that did not opt in.
const PER_STREAM_BUF_DEFAULT: u64 = 32 * 1024 * 1024;

struct StreamState {
    file_id: usize,
    end_exclusive: u64,
    file_abs_offset: u64,
    position: u64,
    waker: Option<Waker>,
    control: ReaderControl,
}

impl StreamState {
    fn current_piece(&self, lengths: &Lengths) -> Option<CurrentPiece> {
        (self.position < self.end_exclusive)
            .then(|| lengths.compute_current_piece(self.position, self.file_abs_offset))
            .flatten()
    }

    fn window(&self, lookahead: u64) -> Range<u64> {
        let end = self
            .position
            .saturating_add(lookahead)
            .min(self.end_exclusive);
        self.file_abs_offset + self.position..self.file_abs_offset + end
    }

    fn piece_window(&self, lookahead: u64, lengths: &Lengths) -> Range<u32> {
        let window = self.window(lookahead);
        if window.is_empty() {
            return 0..0;
        }
        lengths.iter_pieces_within_offset(window.start, window.end - window.start)
    }
}

#[derive(Default)]
struct Policy {
    rate: VerifiedRate,
    recovery: Recovery,
    piece_rounding_payload_bytes: u64,
    obsolete_in_flight_payload_bytes: u64,
}

/// A bounded snapshot of scheduling policy for a demand-only streaming torrent.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamingPolicySnapshot {
    /// Network-derived fallback used by readers without playback feedback.
    pub lookahead_bytes: u64,
    pub verified_sample: Option<VerifiedSample>,
    pub active_readers: usize,
    pub blocked_readers: usize,
    /// Exact union of reader byte windows, before rounding to torrent pieces.
    pub demand_bytes: u64,
    pub piece_rounding_payload_bytes: u64,
    pub obsolete_in_flight_payload_bytes: u64,
    pub recovery: RecoverySnapshot,
    /// At most 64 readers, ordered by stream ID. Counts above include all readers.
    pub readers: Vec<StreamingReaderSnapshot>,
    pub readers_truncated: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StreamingReaderSnapshot {
    pub stream_id: usize,
    pub file_id: usize,
    pub generation: u64,
    pub position: u64,
    pub end_exclusive: u64,
    pub priority: StreamPriority,
    /// Zero when due; no wall-clock timestamp is exposed.
    pub deadline_remaining: Option<Duration>,
    pub lookahead_bytes: u64,
    /// Actual window length clipped to the stream's exclusive end.
    pub window_bytes: u64,
    pub playback: Option<PlaybackHint>,
}

struct StreamsState {
    streams: HashMap<StreamId, StreamState>,
    policy: Option<Policy>,
    scheduling_round: usize,
}

impl StreamsState {
    fn lookahead(&self) -> u64 {
        self.policy
            .as_ref()
            .map_or(PER_STREAM_BUF_DEFAULT, |policy| policy.rate.lookahead())
    }

    fn reader_lookahead(&self, stream: &StreamState) -> u64 {
        stream.control.lookahead(self.lookahead())
    }

    fn pause_sampling_when_idle(&mut self) {
        if self
            .streams
            .values()
            .all(|stream| stream.position == stream.end_exclusive)
            && let Some(policy) = self.policy.as_mut()
        {
            policy.rate.idle();
        }
    }

    fn demand(&self) -> Demand {
        let mut demand = Demand::default();
        for stream in self.streams.values() {
            demand.push(stream.window(self.reader_lookahead(stream)));
        }
        demand
    }
}

pub(crate) struct TorrentStreams {
    next_stream_id: AtomicUsize,
    state: Mutex<StreamsState>,
    pub(crate) demand_changed: Notify,
}

impl TorrentStreams {
    pub(crate) fn new(demand_only: bool) -> Self {
        Self {
            next_stream_id: AtomicUsize::new(0),
            state: Mutex::new(StreamsState {
                streams: HashMap::new(),
                policy: demand_only.then(Policy::default),
                scheduling_round: 0,
            }),
            demand_changed: Notify::new(),
        }
    }

    fn next_id(&self) -> usize {
        self.next_stream_id.fetch_add(1, Ordering::Relaxed)
    }

    fn register_waker(&self, stream_id: StreamId, waker: Waker) {
        if let Some(stream) = self.state.lock().streams.get_mut(&stream_id) {
            stream.waker = Some(waker);
        }
    }

    pub(crate) fn with_scheduling<R>(
        &self,
        lengths: &Lengths,
        schedule: impl FnOnce(Option<&Demand>, &mut dyn Iterator<Item = ValidPieceIndex>) -> R,
    ) -> R {
        let mut state = self.state.lock();
        let mut windows: Vec<_> = state
            .streams
            .iter()
            .map(|(id, stream)| {
                (
                    *id,
                    &stream.control,
                    stream.piece_window(state.reader_lookahead(stream), lengths),
                )
            })
            .filter(|(_, _, range)| !range.is_empty())
            .collect();

        let mut groups = VecDeque::new();
        if state.policy.is_some() {
            windows.sort_by_key(|(id, control, _)| {
                (
                    std::cmp::Reverse(control.priority),
                    control.deadline.is_none(),
                    control.deadline,
                    *id,
                )
            });
            // Deadline orders each round; rotating the starting reader preserves
            // fairness even if the caller consumes only one available piece.
            for group in windows.chunk_by_mut(|left, right| left.1.priority == right.1.priority) {
                let offset = state.scheduling_round % group.len();
                group.rotate_left(offset);
                groups.push_back(
                    group
                        .iter()
                        .map(|(_, _, range)| range.clone())
                        .collect::<VecDeque<_>>(),
                );
            }
        } else {
            // Preserve the upstream shuffle for torrents without policy opt-in.
            use rand::seq::SliceRandom;
            windows.shuffle(&mut rand::rng());
            groups.push_back(windows.iter().map(|(_, _, range)| range.clone()).collect());
        }
        drop(windows);
        state.scheduling_round = state.scheduling_round.wrapping_add(1);
        let mut seen = HashSet::new();
        let mut pieces = std::iter::from_fn(|| {
            while let Some(windows) = groups.front_mut() {
                while let Some(mut window) = windows.pop_front() {
                    if let Some(piece) = window.next() {
                        windows.push_back(window);
                        if seen.insert(piece) {
                            return lengths.validate_piece_index(piece);
                        }
                    }
                }
                groups.pop_front();
            }
            None
        });
        let demand = state.policy.as_ref().map(|_| state.demand());
        schedule(demand.as_ref(), &mut pieces)
    }

    pub(crate) fn with_piece_preemption<R>(
        &self,
        piece: ValidPieceIndex,
        lengths: &Lengths,
        schedule: impl FnOnce(bool, &mut dyn Iterator<Item = ValidPieceIndex>) -> R,
    ) -> R {
        let state = self.state.lock();
        // A piece shared by readers inherits their highest class. In particular,
        // opening a Playback reader over Prefetch must protect the whole piece.
        let class = state
            .streams
            .values()
            .filter(|stream| {
                stream
                    .piece_window(state.reader_lookahead(stream), lengths)
                    .contains(&piece.get())
            })
            .map(|stream| stream.control.priority)
            .max();
        let mut higher = state
            .streams
            .values()
            .filter(|stream| class.is_some_and(|class| stream.control.priority > class))
            .flat_map(|stream| stream.piece_window(state.reader_lookahead(stream), lengths))
            .filter_map(|index| lengths.validate_piece_index(index));
        // Keep the stream lock while the caller checks per-peer eligibility and
        // changes ownership: an overlapping reader cannot appear between them.
        schedule(class.is_some(), &mut higher)
    }

    pub(crate) fn with_blocked<R>(
        &self,
        lengths: &Lengths,
        is_missing: impl Fn(ValidPieceIndex) -> bool,
        check: impl FnOnce(HashSet<ValidPieceIndex>, &mut Recovery) -> R,
    ) -> Option<R> {
        let mut state = self.state.lock();
        let blocked = state
            .streams
            .values()
            .filter(|stream| stream.waker.is_some())
            .filter_map(|stream| stream.current_piece(lengths).map(|piece| piece.id))
            .filter(|piece| is_missing(*piece))
            .collect();
        state
            .policy
            .as_mut()
            .map(|policy| check(blocked, &mut policy.recovery))
    }

    pub(crate) fn record_payload(
        &self,
        piece: ValidPieceIndex,
        offset: u32,
        bytes: u32,
        expected: bool,
        lengths: &Lengths,
    ) {
        let mut state = self.state.lock();
        if state.policy.is_none() {
            return;
        }
        let piece_start = u64::from(piece.get()) * u64::from(lengths.default_piece_length());
        let piece_end = piece_start + u64::from(lengths.piece_length(piece));
        let start = piece_start + u64::from(offset);
        let end = start + u64::from(bytes);
        let windows = state
            .streams
            .values()
            .map(|stream| stream.window(state.reader_lookahead(stream)));
        let covered = windows
            .clone()
            .any(|window| window.start <= start && window.end >= end);
        let obsolete = !windows.clone().any(|window| {
            !window.is_empty() && window.start < piece_end && window.end > piece_start
        });
        let outside = if covered {
            0
        } else if obsolete {
            u64::from(bytes)
        } else {
            u64::from(bytes) - state.demand().covered_bytes(start..end)
        };
        let policy = state.policy.as_mut().unwrap();
        let counter = if obsolete {
            &mut policy.obsolete_in_flight_payload_bytes
        } else {
            &mut policy.piece_rounding_payload_bytes
        };
        *counter = counter.saturating_add(outside);
        if expected && !obsolete {
            policy.rate.network_payload(Instant::now());
        }
    }

    pub(crate) fn record_verified(
        &self,
        piece: ValidPieceIndex,
        bytes: u64,
        lengths: &Lengths,
        attempt: Option<u64>,
    ) {
        let mut state = self.state.lock();
        let demanded = state.streams.values().any(|stream| {
            stream
                .piece_window(state.reader_lookahead(stream), lengths)
                .contains(&piece.get())
        });
        let Some(policy) = state.policy.as_mut() else {
            return;
        };
        let now = Instant::now();
        policy.recovery.record_verified();
        if let Some(attempt) = attempt {
            policy.recovery.record_attempt_verified(attempt, now);
        }
        if demanded && policy.rate.observe(now, bytes) {
            self.demand_changed.notify_waiters();
        }
    }

    pub(crate) fn record_transport_failure(&self) {
        if let Some(policy) = self.state.lock().policy.as_mut() {
            policy.recovery.record_transport_failure();
        }
    }

    pub(crate) fn record_recovery_handshake(&self, attempt: u64) {
        if let Some(policy) = self.state.lock().policy.as_mut() {
            policy.recovery.record_handshake(attempt, Instant::now());
        }
    }

    fn snapshot(&self) -> Option<StreamingPolicySnapshot> {
        let state = self.state.lock();
        let policy = state.policy.as_ref()?;
        let now = Instant::now();
        let mut readers: Vec<_> = state.streams.iter().collect();
        readers.sort_unstable_by_key(|(id, _)| **id);
        readers.truncate(64);
        let readers = readers
            .into_iter()
            .map(|(id, stream)| {
                let lookahead_bytes = state.reader_lookahead(stream);
                let window = stream.window(lookahead_bytes);
                StreamingReaderSnapshot {
                    stream_id: *id,
                    file_id: stream.file_id,
                    generation: stream.control.generation,
                    position: stream.position,
                    end_exclusive: stream.end_exclusive,
                    priority: stream.control.priority,
                    deadline_remaining: stream
                        .control
                        .deadline
                        .map(|deadline| deadline.saturating_duration_since(now)),
                    lookahead_bytes,
                    window_bytes: window.end - window.start,
                    playback: stream.control.playback,
                }
            })
            .collect();
        Some(StreamingPolicySnapshot {
            lookahead_bytes: state.lookahead(),
            verified_sample: policy.rate.snapshot(now),
            active_readers: state
                .streams
                .values()
                .filter(|s| s.position < s.end_exclusive)
                .count(),
            blocked_readers: state.streams.values().filter(|s| s.waker.is_some()).count(),
            demand_bytes: state.demand().total_bytes(),
            piece_rounding_payload_bytes: policy.piece_rounding_payload_bytes,
            obsolete_in_flight_payload_bytes: policy.obsolete_in_flight_payload_bytes,
            recovery: policy.recovery.snapshot(),
            readers,
            readers_truncated: state.streams.len() > 64,
        })
    }

    pub(crate) fn wake_streams_on_piece_completed(
        &self,
        piece_id: ValidPieceIndex,
        lengths: &Lengths,
    ) {
        let wakers: Vec<_> = self
            .state
            .lock()
            .streams
            .values_mut()
            .filter(|stream| stream.current_piece(lengths).map(|piece| piece.id) == Some(piece_id))
            .filter_map(|stream| stream.waker.take())
            .collect();
        for waker in wakers {
            waker.wake();
        }
    }

    fn drop_stream(&self, stream_id: StreamId) {
        debug!(stream_id, "dropping stream");
        let mut state = self.state.lock();
        if state.streams.remove(&stream_id).is_some() && state.policy.is_some() {
            state.pause_sampling_when_idle();
            self.demand_changed.notify_waiters();
        }
    }

    fn set_position(
        &self,
        stream_id: StreamId,
        new_pos: u64,
        seek: bool,
        lengths: &Lengths,
    ) -> Result<(), StreamingControlError> {
        let mut state = self.state.lock();
        let fallback = state.lookahead();
        let demand_only = state.policy.is_some();
        let stream = state
            .streams
            .get_mut(&stream_id)
            .expect("registered stream");
        let previous = stream.piece_window(stream.control.lookahead(fallback), lengths);
        if seek {
            stream.control.seek()?;
        }
        stream.position = new_pos;
        stream.waker = None;
        let next = stream.piece_window(stream.control.lookahead(fallback), lengths);
        if demand_only && (seek || previous != next) {
            state.pause_sampling_when_idle();
            self.demand_changed.notify_waiters();
        }
        Ok(())
    }

    pub(crate) fn streamed_file_ids(&self) -> Vec<usize> {
        self.state
            .lock()
            .streams
            .values()
            .map(|stream| stream.file_id)
            .collect()
    }
}

pub struct FileStream {
    torrent: ManagedTorrentHandle,
    metadata: Arc<TorrentMetadata>,
    streams: Arc<TorrentStreams>,
    stream_id: usize,
    file_id: usize,
    position: u64,

    // File-relative bounds.
    range_start: u64,
    end_exclusive: u64,
    file_len: u64,
    file_torrent_abs_offset: u64,

    _blocking_permit: OwnedSemaphorePermit,
}

macro_rules! map_io_err {
    ($e:expr) => {
        $e.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    };
}

macro_rules! poll_try_io {
    ($e:expr) => {{
        let e = map_io_err!($e);
        match e {
            Ok(r) => r,
            Err(e) => {
                debug!("stream error {e:#}");
                return Poll::Ready(Err(e));
            }
        }
    }};
}

impl AsyncRead for FileStream {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        tbuf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.position == self.end_exclusive || tbuf.remaining() == 0 {
            debug!(
                stream_id = self.stream_id,
                file_id = self.file_id,
                "stream completed, EOF"
            );
            return Poll::Ready(Ok(()));
        }

        let current = poll_try_io!(
            self.metadata
                .lengths()
                .compute_current_piece(self.position, self.file_torrent_abs_offset)
                .context("invalid position")
        );

        // if the piece is not there, register to wake when it is
        // check if we have the piece for real
        let have = poll_try_io!(self.torrent.with_chunk_tracker(|ct| {
            let have = ct.get_have_pieces().as_slice()[current.id.get() as usize];
            if !have {
                self.streams
                    .register_waker(self.stream_id, cx.waker().clone());
            }
            have
        }));
        if !have {
            debug!(stream_id = self.stream_id, file_id = self.file_id, piece_id = %current.id, "poll pending, not have");
            return Poll::Pending;
        }

        // actually stream the piece
        let buf = tbuf.initialize_unfilled();
        let file_remaining = self.end_exclusive - self.position;
        let bytes_to_read: usize = poll_try_io!(
            (buf.len() as u64)
                .min(current.piece_remaining as u64)
                .min(file_remaining)
                .try_into()
        );

        let buf = &mut buf[..bytes_to_read];

        let start = Instant::now();
        poll_try_io!(poll_try_io!(self.torrent.shared.spawner.block_in_place(
            || {
                self.torrent.with_storage_and_file(
                    self.file_id,
                    |files, _fi| {
                        files.pread_exact(self.file_id, self.position, buf)?;
                        Ok::<_, anyhow::Error>(())
                    },
                    &self.metadata,
                )
            }
        )));

        trace!(
            buflen = buf.len(),
            stream_id = self.stream_id,
            file_id = self.file_id,
            read_time = ?start.elapsed(),
            "will write bytes"
        );

        self.as_mut().advance(bytes_to_read as u64);
        tbuf.advance(bytes_to_read);

        Poll::Ready(Ok(()))
    }
}

impl AsyncSeek for FileStream {
    fn start_seek(
        mut self: std::pin::Pin<&mut Self>,
        position: std::io::SeekFrom,
    ) -> std::io::Result<()> {
        let new_pos = match position {
            SeekFrom::Start(s) => Some(s),
            SeekFrom::End(e) => self.end_exclusive.checked_add_signed(e),
            SeekFrom::Current(o) => self.position.checked_add_signed(o),
        }
        .filter(|&pos| (self.range_start..=self.end_exclusive).contains(&pos))
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid seek"))?;

        map_io_err!(self.streams.set_position(
            self.stream_id,
            new_pos,
            true,
            self.metadata.lengths()
        ))?;
        self.position = new_pos;
        debug!(stream_id = self.stream_id, position = self.position, "seek");
        Ok(())
    }

    fn poll_complete(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> Poll<std::io::Result<u64>> {
        Poll::Ready(Ok(self.position))
    }
}

impl Drop for FileStream {
    fn drop(&mut self) {
        self.streams.drop_stream(self.stream_id);
    }
}

impl ManagedTorrent {
    fn with_storage_and_file<F, R>(
        &self,
        file_id: usize,
        f: F,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<R>
    where
        F: FnOnce(&dyn TorrentStorage, &FileInfo) -> R,
    {
        self.with_state(|s| {
            let files = match s {
                crate::ManagedTorrentState::Paused(p) => &*p.files,
                crate::ManagedTorrentState::Live(l) => &*l.files,
                s => anyhow::bail!("with_storage_and_file: invalid state: {}", s.name()),
            };
            let fi = metadata.file_infos.get(file_id).context("invalid file")?;
            Ok(f(files, fi))
        })
    }

    fn streams(&self) -> anyhow::Result<Arc<TorrentStreams>> {
        self.with_state(|s| match s {
            crate::ManagedTorrentState::Paused(p) => Ok(p.streams.clone()),
            crate::ManagedTorrentState::Live(l) => Ok(l.streams.clone()),
            s => anyhow::bail!("streams: invalid state {}", s.name()),
        })
    }

    fn maybe_reconnect_needed_peers_for_file(&self, file_id: usize) -> bool {
        // If we have the full file, don't bother.
        if self.is_file_finished(file_id) {
            return false;
        }
        self.with_state(|state| {
            if let crate::ManagedTorrentState::Live(l) = &state {
                l.reconnect_all_not_needed_peers();
            }
        });
        true
    }

    fn is_file_finished(&self, file_id: usize) -> bool {
        let metadata = self.metadata.load();
        let metadata = match metadata.as_ref() {
            Some(r) => r,
            None => return false,
        };
        // TODO: would be nice to remove locking
        self.with_chunk_tracker(|ct| ct.is_file_finished(&metadata.file_infos[file_id]))
            .unwrap_or(false)
    }

    /// Read the full file. Existing callers retain file-relative seek semantics.
    pub async fn stream(self: Arc<Self>, file_id: usize) -> anyhow::Result<FileStream> {
        self.open_stream(file_id, None, None).await
    }

    /// Open a file-relative range, publishing its cursor and exclusive end together.
    pub async fn stream_range(
        self: Arc<Self>,
        file_id: usize,
        start: u64,
        end_exclusive: u64,
    ) -> anyhow::Result<FileStream> {
        self.open_stream(file_id, Some(start..end_exclusive), None)
            .await
    }

    /// Open a controlled, file-relative range on a torrent added with
    /// `AddTorrentOptions::demand_only_streaming = true`.
    ///
    /// The start and exclusive end retain the same seek semantics as
    /// [`Self::stream_range`]. Relative deadlines are captured when this future
    /// starts, before waiting for initialization or an available reader slot.
    /// Invalid options return a downcastable [`StreamingControlError`].
    pub async fn stream_range_with_options(
        self: Arc<Self>,
        file_id: usize,
        start: u64,
        end_exclusive: u64,
        options: StreamingReadOptions,
    ) -> anyhow::Result<FileStream> {
        self.open_stream(file_id, Some(start..end_exclusive), Some(options))
            .await
    }

    /// Observed peer availability for the torrent pieces intersecting a
    /// file-relative byte range. At most 4096 pieces may be queried at once.
    /// Counts describe connected peers, not the complete swarm, and do not imply
    /// that data is already present or hash-verified locally.
    pub fn streaming_availability(
        &self,
        file_id: usize,
        start: u64,
        end_exclusive: u64,
    ) -> Result<super::PeerRegionAvailability, super::PeerAvailabilityError> {
        use super::PeerAvailabilityError;
        let metadata = self
            .metadata
            .load_full()
            .ok_or(PeerAvailabilityError::Unavailable)?;
        let file = metadata
            .file_infos
            .get(file_id)
            .ok_or(PeerAvailabilityError::InvalidFile)?;
        if start > end_exclusive || end_exclusive > file.len {
            return Err(PeerAvailabilityError::InvalidByteRange);
        }
        let pieces = if start == end_exclusive {
            0..0
        } else {
            let offset = file
                .offset_in_torrent
                .checked_add(start)
                .ok_or(PeerAvailabilityError::InvalidByteRange)?;
            metadata
                .lengths()
                .iter_pieces_within_offset(offset, end_exclusive - start)
        };
        self.live()
            .ok_or(PeerAvailabilityError::Unavailable)?
            .peer_region_availability(pieces)
    }

    /// Bounded diagnostics for an opted-in torrent after initialization.
    pub fn streaming_policy(&self) -> Option<StreamingPolicySnapshot> {
        self.streams().ok()?.snapshot()
    }

    async fn open_stream(
        self: Arc<Self>,
        file_id: usize,
        range: Option<Range<u64>>,
        options: Option<StreamingReadOptions>,
    ) -> anyhow::Result<FileStream> {
        if options.is_some() && !self.shared.options.demand_only_streaming {
            return Err(StreamingControlError::PolicyDisabled.into());
        }
        let control = ReaderControl::new(options.unwrap_or_default(), Instant::now())?;
        // Streams need live or paused state, so wait out the initial check
        // of existing data instead of failing while it runs.
        self.wait_until_initialized().await?;
        let metadata = self
            .metadata
            .load_full()
            .context("torrent metadata is not resolved")?;
        let (fd_len, fd_offset) = self.with_storage_and_file(
            file_id,
            |_fd, fi| (fi.len, fi.offset_in_torrent),
            &metadata,
        )?;
        let range = range.unwrap_or(0..fd_len);
        anyhow::ensure!(
            range.start <= range.end && range.end <= fd_len,
            "invalid stream range"
        );
        let streams = self.streams()?;
        let blocking_permit = self.shared().spawner.semaphore().acquire_owned().await?;
        let s = FileStream {
            stream_id: streams.next_id(),
            streams: streams.clone(),
            file_id,
            position: range.start,
            range_start: range.start,
            end_exclusive: range.end,

            file_len: fd_len,
            file_torrent_abs_offset: fd_offset,
            _blocking_permit: blocking_permit,
            torrent: self,
            metadata,
        };
        {
            let mut state = streams.state.lock();
            state.streams.insert(
                s.stream_id,
                StreamState {
                    file_id,
                    position: range.start,
                    waker: None,
                    end_exclusive: range.end,
                    file_abs_offset: fd_offset,
                    control,
                },
            );
            if state.policy.is_some() && !range.is_empty() {
                streams.demand_changed.notify_waiters();
            }
        }
        if !range.is_empty() {
            s.torrent.maybe_reconnect_needed_peers_for_file(file_id);
        }

        debug!(stream_id = s.stream_id, file_id, "started stream");

        Ok(s)
    }
}

impl FileStream {
    pub fn position(&self) -> u64 {
        self.position
    }

    /// Zero initially; incremented by every successful seek, including a seek
    /// to the current byte position. Reads and rejected seeks do not increment it.
    pub fn generation(&self) -> u64 {
        self.streams.state.lock().streams[&self.stream_id]
            .control
            .generation
    }

    /// Apply encoded-media consumption feedback for a captured generation.
    /// A seek clears feedback; feedback from earlier generations is rejected.
    /// Accepted hints notify peer requesters to cancel obsolete demand.
    /// Requires the torrent's `demand_only_streaming` opt-in.
    pub fn update_playback(
        &self,
        generation: u64,
        hint: PlaybackHint,
    ) -> Result<(), StreamingControlError> {
        let mut state = self.streams.state.lock();
        if state.policy.is_none() {
            return Err(StreamingControlError::PolicyDisabled);
        }
        state
            .streams
            .get_mut(&self.stream_id)
            .expect("registered stream")
            .control
            .update_playback(generation, hint)?;
        self.streams.demand_changed.notify_waiters();
        Ok(())
    }

    fn advance(&mut self, diff: u64) {
        let next = self.position + diff;
        self.streams
            .set_position(self.stream_id, next, false, self.metadata.lengths())
            .expect("sequential reads cannot exhaust a generation");
        self.position = next;
    }

    /// Length of the source file, independent of this reader's range and cursor.
    pub fn len(&self) -> u64 {
        self.file_len
    }

    /// Whether the source file is empty, matching [`Self::len`].
    /// This does not indicate an empty selected range or end of the reader.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::super::streaming_control::{
        PlaybackHint, ReaderControl, StreamPriority, StreamingControlError, StreamingReadOptions,
    };
    use super::*;
    use std::time::Duration;

    const MIB: u64 = 1024 * 1024;

    fn add_reader(
        streams: &TorrentStreams,
        id: usize,
        range: Range<u64>,
        priority: StreamPriority,
        deadline: Option<Duration>,
    ) {
        streams.state.lock().streams.insert(
            id,
            StreamState {
                file_id: 0,
                file_abs_offset: 0,
                position: range.start,
                end_exclusive: range.end,
                waker: None,
                control: ReaderControl::new(
                    StreamingReadOptions { priority, deadline },
                    Instant::now(),
                )
                .unwrap(),
            },
        );
    }

    fn schedule(streams: &TorrentStreams) -> Vec<u32> {
        let lengths = Lengths::new(16 * MIB, u32::try_from(MIB).unwrap()).unwrap();
        streams.with_scheduling(&lengths, |_, pieces| {
            pieces.map(|piece| piece.get()).collect()
        })
    }

    #[test]
    fn playback_precedes_probe_even_when_probe_deadline_is_earlier() {
        let streams = TorrentStreams::new(true);
        add_reader(
            &streams,
            0,
            0..MIB,
            StreamPriority::Probe,
            Some(Duration::ZERO),
        );
        add_reader(
            &streams,
            1,
            4 * MIB..5 * MIB,
            StreamPriority::Playback,
            None,
        );
        add_reader(
            &streams,
            2,
            8 * MIB..9 * MIB,
            StreamPriority::Prefetch,
            None,
        );
        assert_eq!(schedule(&streams), [4, 8, 0]);
    }

    #[test]
    fn equal_priority_interleaves_readers_and_rotates_first_pick() {
        let streams = TorrentStreams::new(true);
        add_reader(&streams, 0, 0..4 * MIB, StreamPriority::Playback, None);
        add_reader(
            &streams,
            1,
            8 * MIB..12 * MIB,
            StreamPriority::Playback,
            None,
        );
        for stream in streams.state.lock().streams.values_mut() {
            stream
                .control
                .update_playback(
                    0,
                    PlaybackHint {
                        consumption_bytes_per_second: 2 * MIB,
                        buffered: Duration::ZERO,
                        target_buffer: Duration::from_secs(2),
                    },
                )
                .unwrap();
        }
        assert_eq!(schedule(&streams), [0, 8, 1, 9, 2, 10, 3, 11]);
        assert_eq!(schedule(&streams), [8, 0, 9, 1, 10, 2, 11, 3]);
    }

    #[test]
    fn deadline_orders_first_pick_within_equal_priority() {
        let streams = TorrentStreams::new(true);
        add_reader(&streams, 0, 0..MIB, StreamPriority::Playback, None);
        add_reader(
            &streams,
            1,
            4 * MIB..5 * MIB,
            StreamPriority::Playback,
            Some(Duration::from_secs(5)),
        );
        add_reader(
            &streams,
            2,
            8 * MIB..9 * MIB,
            StreamPriority::Playback,
            Some(Duration::ZERO),
        );
        assert_eq!(schedule(&streams), [8, 4, 0]);
        // An early deadline cannot monopolize repeated one-piece acquisitions.
        assert_eq!(schedule(&streams)[0], 4);
        assert_eq!(schedule(&streams)[0], 0);
    }

    #[test]
    fn overlapping_readers_count_union_and_schedule_each_piece_once() {
        let streams = TorrentStreams::new(true);
        add_reader(&streams, 0, 0..MIB, StreamPriority::Playback, None);
        add_reader(
            &streams,
            1,
            MIB / 2..MIB + MIB / 2,
            StreamPriority::Playback,
            None,
        );
        assert_eq!(schedule(&streams), [0, 1]);
        assert_eq!(streams.snapshot().unwrap().demand_bytes, MIB + MIB / 2);
        streams.drop_stream(0);
        assert_eq!(schedule(&streams), [0, 1]);
        assert_eq!(streams.snapshot().unwrap().demand_bytes, MIB);
    }

    #[test]
    fn seek_within_same_piece_notifies_and_invalidates_generation() {
        let streams = TorrentStreams::new(true);
        let lengths = Lengths::new(16 * MIB, u32::try_from(MIB).unwrap()).unwrap();
        add_reader(&streams, 0, 0..MIB, StreamPriority::Playback, None);
        let changed = streams.demand_changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        streams.set_position(0, 1, true, &lengths).unwrap();
        assert!(futures::FutureExt::now_or_never(changed).is_some());
        let mut state = streams.state.lock();
        let stream = state.streams.get_mut(&0).unwrap();
        assert_eq!(stream.position, 1);
        assert_eq!(stream.control.generation, 1);
        assert!(matches!(
            stream.control.update_playback(
                0,
                PlaybackHint {
                    consumption_bytes_per_second: MIB,
                    buffered: Duration::ZERO,
                    target_buffer: Duration::from_secs(1)
                }
            ),
            Err(StreamingControlError::StaleGeneration { .. })
        ));
    }

    #[test]
    fn legacy_readers_keep_default_window_without_demand_only_gate() {
        let streams = TorrentStreams::new(false);
        add_reader(&streams, 0, 0..64 * MIB, StreamPriority::Playback, None);
        let lengths = Lengths::new(64 * MIB, u32::try_from(MIB).unwrap()).unwrap();
        streams.with_scheduling(&lengths, |demand, pieces| {
            assert!(demand.is_none());
            assert_eq!(
                pieces.map(|piece| piece.get()).collect::<Vec<_>>(),
                (0..32).collect::<Vec<_>>()
            );
        });
        assert!(streams.snapshot().is_none());
    }

    #[test]
    fn diagnostics_bound_reader_details_but_count_full_demand_union() {
        let streams = TorrentStreams::new(true);
        for id in 0..70 {
            add_reader(&streams, id, 0..MIB, StreamPriority::Playback, None);
        }
        let snapshot = streams.snapshot().unwrap();
        assert_eq!(snapshot.readers.len(), 64);
        assert!(snapshot.readers_truncated);
        assert_eq!(snapshot.active_readers, 70);
        assert_eq!(snapshot.demand_bytes, MIB);
        assert_eq!(snapshot.readers.first().unwrap().stream_id, 0);
        assert_eq!(snapshot.readers.last().unwrap().stream_id, 63);
    }
}
