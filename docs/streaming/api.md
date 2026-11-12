# Streaming API

Use the library with `default-features = false, features = ["rust-tls"]` and pin a
reviewed Git commit. Enable `AddTorrentOptions::demand_only_streaming` for a
torrent before opening controlled readers. Existing normal downloads remain
available when this option is false.

## Read and provide playback feedback

`ManagedTorrent::stream_range_with_options(file_id, start, end_exclusive,
StreamingReadOptions)` registers file-relative bounds atomically. Its result is
a public `FileStream` implementing Tokio `AsyncRead` and `AsyncSeek`.
Its `len()` and `is_empty()` describe the source file, independently of the
registered range and current read cursor.

```rust,ignore
use std::time::Duration;
use librqbit::{PlaybackHint, StreamPriority, StreamingReadOptions};

let mut reader = torrent.clone().stream_range_with_options(
    file_id, start, end_exclusive,
    StreamingReadOptions {
        priority: StreamPriority::Playback,
        deadline: Some(Duration::ZERO),
    },
).await?;

let generation = reader.generation();
reader.update_playback(generation, PlaybackHint {
    consumption_bytes_per_second: 1024 * 1024,
    buffered: Duration::from_secs(2),
    target_buffer: Duration::from_secs(8),
})?;
```

The rate is encoded-media consumption supplied by the caller, not network speed.
The read cursor is a byte offset; the library does not parse presentation
timestamps. The lookahead is rate times the positive buffer deficit, rounded up
and bounded to 1–32 MiB, then clipped to the reader's end. With no hint, the
verified-network policy supplies the window. A full buffer retains the minimum
window so later reads can progress.

Consumption rate must be 1 byte/s through 1 GiB/s. Target duration must be nonzero
and at most one hour; buffered duration must not exceed one hour. Invalid updates
leave existing demand unchanged. These bounds constrain the API; they do not
claim support for media at those extreme rates.

An accepted seek increments the generation, clears old feedback and notifies
requesters, even if the seek stays within the same torrent piece. A failed seek
does not increment it. Delayed feedback for an older generation returns
`StreamingControlError::StaleGeneration`. Ordinary reads preserve the generation.
The caller should regularly refresh buffer feedback; hints do not expire by time.

## Priority and deadlines

The current classes are Playback, Prefetch and Probe, in descending priority.
Use Playback for every request essential to ongoing playback, including an
essential container header. Probe is intended for speculative metadata work.
Readers at the same priority interleave pieces. Deadline sorts the starting
order, while rotation across acquisitions prevents a stable reader set from
starving equal-priority readers. Higher classes can intentionally delay lower
classes.

Deadlines are relative to opening the reader, captured by a monotonic clock, and
bounded to 24 hours. Zero means immediately due. They are ordering hints, not
timeouts or guarantees that a piece will arrive. Requests already on the wire
may still return after cancellation; accounting distinguishes that traffic from
active demand. Piece verification still requires complete pieces.

## Peer flow and diagnostics

Streaming peers adapt their request window using request/response measurements.
The configured maximum and the peer's negotiated positive `reqq` remain bounds.
The inherited configured-zero behavior normalizes to one. A zero `reqq` is
rejected for streaming rather than creating a non-progressing queue. A reduced
limit stops new requests while older requests drain. Non-streaming peers retain
the original fixed-window policy.

`ManagedTorrent::streaming_availability(file_id, start, end_exclusive)` returns
the union of pieces advertised by currently connected peers. The query is bounded
to 4,096 pieces and rejects invalid bounds with `PeerAvailabilityError`. `Unknown`
means no usable peer map, `Partial` means some connected peers have not provided a
map, and `Observed` means all currently connected peers supplied maps. None of
these represents the entire swarm or certifies local payload hashes.

`streaming_policy()` exposes demand counts, windows, recovery and at most 64
reader details. `readers_truncated` signals a shortened list; aggregate counters
still include all readers. Per-peer stats include optional `request_flow` with
window limits, outstanding requests and sample ages. The new types are publicly
reexported so embedding applications do not need private-module imports.

## Resume

`SessionOptions::fastresume` remains disabled by default. When enabled with a
persistent session, streaming torrents hash-check every claimed piece before
using saved progress. Valid pieces are retained and corrupt or missing claims
are cleared; unclaimed holes are not scanned in that path. Hash reads check for
pause between chunks of at most 64 KiB while validating saved claims. Malformed
saved state falls back to the full initial check, whose pause checks occur between
pieces. JSON and optional PostgreSQL persistence retain the streaming flag;
older records default to normal download behavior.

The normal fast-resume sampler now rejects checksum mismatch as well as read
errors. Sampling outside streaming mode retains upstream semantics and is not
equivalent to checking every claimed piece. No timestamp-based integrity bypass
has been introduced.

See `crates/librqbit/tests/streaming_contract.rs` for a compiled external-consumer
example with two loopback TCP peers and no desktop dependencies.
