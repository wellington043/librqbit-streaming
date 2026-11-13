# Discovery state and payload priority

`librqbit-streaming.5` retains the upstream package version and the existing
streaming interfaces. Consumers should pin the exact source commit.

## Routing checkpoints

`DhtPersistenceConfig` supports `persist_peer_store` (true by default) and
`max_state_bytes` (16 MiB by default, also the hard ceiling). Setting the former
to false stores only routing nodes, without announced torrent peers or tokens.
Both IPv4 and IPv6 routing tables are restored. Persisted bookkeeping is not
trusted: tables are rebuilt from bounded node candidates with canonical limits,
and mismatched local node identities are rejected. Unreadable, malformed or
oversized checkpoints fall back to fresh discovery.

Checkpoint serialization is bounded before publication. A unique temporary file
is synced before replacing the destination; a failed write retains the previous
checkpoint. Temporary names use `.routing-*.tmp`. An embedder that exclusively
owns the directory may reclaim orphaned temporary files after a process crash.
The embedder must still prevent multiple sessions writing the same destination.
Its background sessions can use `persistence: None`, and offline sessions should
disable DHT entirely. An explicit port 0 avoids relying on a persisted port that
may have become unavailable. No public-source setting overrides private torrent
discovery restrictions.

## Foreground health

`DownloadArbiter::set_foreground(active, healthy)` remains the integration point.
Healthy foreground observations expire after three seconds unless refreshed.
Preparation and background payload admission waits when an active foreground
is unhealthy or stale. Requests already sent may still finish; the arbiter does
not claim to cancel all traffic already in flight.

Cancellation while waiting for pacing does not reserve future time for a request
that was never admitted. Request enqueue is ordered with foreground state
changes. Existing concurrency and payload pacing limits remain in place.

Player embedders should express buffer headroom in wall time and scale encoded
consumption by playback speed. For example, eight media seconds at 2x supply four
wall seconds. Fresh, contiguous player headroom is different from total verified
torrent progress or a scene bitrate measurement.

Focused loopback, persistence and scheduling tests validate contracts. They do
not establish first-frame latency, throughput gains in a public swarm, or physical
decoder/HDR behavior. Routing persistence follows the intent of
[BEP5](https://www.bittorrent.org/beps/bep_0005.html); private tracker discovery is
governed by [BEP27](https://www.bittorrent.org/beps/bep_0027.html).
