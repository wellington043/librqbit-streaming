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

## Shared request scheduling (streaming.2)

Embedders may share one `DownloadArbiter` through `SessionOptions::download_arbiter`
across independent sessions. `AddTorrentOptions::download_class` selects
`Foreground`, `Preparation` or `Background`. With no arbiter, the original
session behavior remains available. The caller must keep foreground activity
and health current using `set_foreground`; the arbiter does not infer player
buffer health from transfer speed.

The three classes reserve at most 64, 8 and 4 outstanding payload requests.
Unhealthy foreground playback blocks new preparation/background issuance;
already issued requests can still complete. While foreground playback is
healthy, preparation and background have explicit ceilings of 2 MiB/s and
1 MiB/s. These are policy limits, not estimates of spare network capacity.
Snapshots distinguish issued, received, verified, cancelled and in-flight bytes.

## Reclaimable storage and selected-file import (streaming.2)

Storage implementations can reserve piece capacity before requesting payload
and provide `discard_range` for physical reclamation. The default discard
implementation refuses unsupported operations. Filesystem storage punches sparse
ranges on supported Windows/Linux filesystems and rejects destructive operations
on multiply linked descriptors. An embedder must probe filesystem support and
account for actual allocation; a logical file length does not measure disk use.

`ManagedTorrent::reclaim_cached_pieces` requires a paused demand-only torrent.
It retains active reader windows, durably publishes reduced verified bits via
the caller's checkpoint callback, invalidates in-memory coverage, and only then
discards storage. A failed checkpoint aborts before mutation; a later discard
failure leaves verification cleared. The callback must replace every canonical
checkpoint used on restart, including an all-zero bitfield. The API bounds each
batch; `last_considered_piece` lets callers advance their selection without
starving pieces beyond a retained prefix. Application-level pins and durable
downloads remain the embedder's responsibility.

`AddTorrentOptions::verify_selected_pieces` rehashes the selected file's boundary
pieces independently of unrelated files. This supports owned compact boundary
fragments during partial-download import. Existing saved claims are not trusted
without hashing; this option does not waive source identity, file ownership or
completion checks in the embedding application.


## Catalog observation controls (streaming.3)

All controls are session-scoped and disabled by default unless stated otherwise.
`SessionOptions::observation_only` forces an empty selected-file list and the
request loop sends no payload block requests, while keeping connected peer
bitfields observable. Embedders must still bound session lifetime and peers.

`tracker_observer` is an optional callback for each parsed HTTP or UDP announce
reply. Its endpoint contains only scheme, host and port; tracker path, query,
credentials and error details are excluded. Success, reply latency and count of
returned addresses are observations, not a claim that those peers are useful.
The callback is invoked before forwarding addresses and must be nonblocking.
HTTP tracker response bodies are read incrementally and rejected above 2 MiB.

`single_tracker_discovery` limits discovery to the first ordered tracker even
before magnet privacy is known. This explicit mode retains its single-tracker
behavior. Ordinary private torrents use the sequential policy below and never
add custom or global trackers. DHT and local discovery remain separate session
options. Magnet metadata resolution honors the peer limit and bounds its seen
address set. `metadata_size_limit` rejects larger announced peer metadata before
allocation; the default remains 32 MiB and the hard maximum is 32 MiB.

`payload_budget` optionally reserves each outgoing block request atomically and
never refunds retries or preempted requests. The cap bounds the sum of solicited
payload lengths; it does not count tracker messages, protocol overhead, or
unsolicited network bytes. A failed reservation cancels the session. This is
not a replacement for an embedder's process, disk, memory or network sandbox.

## Bounded catalog tracker discovery

`TrackerDiscoveryPlan::new(Vec<Vec<Url>>)` prepares a single pass through the original announce tiers: it shuffles within each tier, deduplicates URLs, and exhausts a tier before the next. A nonempty announce-list takes precedence over announce. Magnet `tr` parameters have no tier information; callers can represent them as ordered singleton tiers. Keep the original metainfo bytes and credentials; the plan is a discovery view, not a replacement metainfo.

Set `SessionOptions::tracker_attempt = Some(TrackerAttempt { url, timeout })` for a catalog attempt. The selected URL must occur in the torrent's original tracker list. This mode performs one bounded HTTP/HTTPS/UDP announce, including DNS and body reading, with no background announce retries. The tracker observer is unchanged: success (including zero peers) is distinct from failure; cancellation before a response produces no observation. Tracker announce diagnostics use scheme, host and port only.

This mode uses tracker peers exclusively, including while magnet privacy is unknown. It excludes DHT/LSD/PEX and injected initial peers. Ordinary public sessions keep their existing initial-peer behavior. When unknown metadata resolves to private, its former discovery stream and seen-peer history are discarded.

For failover, first drop the attempt's caller-owned add/metadata/media futures, then await `Session::stop()`, then drop the session. Stop closes task admission and waits for spawned initialization, peer connections, peer queues and retry timers to release their resources. Only after successful teardown may a new session use the next tracker. A stopped session is not reusable. A timeout while draining must end the operation, never advance to a new tracker.

A catalog caller should hold one overall deadline and one shared `PayloadBudget` across attempts. Advance only on an observed tracker failure; a valid response with zero peers or unusable peers is not permission to switch a private tracker. Exhausting failed candidates is an explicit tracker error, not evidence of zero availability.

This API remains a bounded one-shot catalog operation, independent from the continuous policy below. No live-network performance or private-tracker interoperability claim is implied by loopback tests. Protocol references: [BEP 12](https://www.bittorrent.org/beps/bep_0012.html) and [BEP 27](https://www.bittorrent.org/beps/bep_0027.html).


## Continuous tracker discovery

Ordinary sessions announce sequentially through the original tiers using HTTP,
HTTPS or UDP. URLs are deduplicated, members are shuffled within each tier once,
and successful trackers are promoted within their tier. Public torrents start
at the first tier on each subsequent announce. Known private torrents keep the
successful tracker until it fails; before switching, peer admission closes and
old connections and retry tasks are cancelled and drained. Queued old peers and
seen-peer history cannot enter the next generation. Incoming handshakes accepted
before the switch are rejected even if they finish after a pause/resume; the
initial generation still allows handshakes that awaited initialization. Newly
accepted inbound connections retain normal behavior; this is not authentication
of a remote peer's tracker membership. Known private torrents exclude
custom/global trackers, DHT, local discovery, initial peers and PEX.

Each announce, including DNS and body reading, has a 30-second deadline. Only a
failed announce advances immediately: success with zero peers still waits for
the tracker interval. The greater of interval/min-interval is respected, with a
five-second floor; `force_tracker_interval` may lengthen but cannot shorten it.
After a complete failed pass, retry delays increase from 10 to at most 600
seconds. Cancellation drops the current request or wait without recording a
false tracker failure. HTTP responses remain bounded to 2 MiB and diagnostics
omit credentials, paths, queries and tracker-supplied error text.

Magnet privacy is unknown before metadata. The normal DHT/local-discovery
bootstrap policy remains enabled where configured; this is not a promise that
unknown magnets are private. A public result retains its discovery stream and
announce interval. A private result drops unknown-privacy discovery and all
metadata peer history before starting the private policy. Explicit
`single_tracker_discovery` and one-shot `tracker_attempt` retain their separate
contracts above. The low-level `TrackerComms::start` API is unchanged;
`start_tiered` is the sequential interface and requires an awaited drain callback
for private callers.

Focused tests use synthetic data and loopback sockets for tiers, empty success,
backoff, cancellation and private peer teardown. They do not establish swarm
availability, public-network speedup or compatibility with a live private
tracker. No release binary is implied by these source changes.
