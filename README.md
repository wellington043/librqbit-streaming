# librqbit-streaming

A streaming-focused Rust library fork of [rqbit](https://github.com/ikatson/rqbit), based on librqbit 9.1.0. The library supplies verified torrent bytes to a media application. It does not include a player, codec implementation or transcoder.

This is an experimental fork. Local validation uses generated payloads and loopback peers. Public-swarm performance and player integration require separate validation.

## Streaming features

- Atomic file-relative range registration and demand-only downloading.
- Playback, probe and prefetch priorities, with deadline hints and fair scheduling among readers at the same priority.
- Generation-checked feedback: consumption rate, available buffer duration and target buffer duration control bounded read-ahead. A seek invalidates old hints.
- Cancellation of obsolete requests, including when a peer request window is full.
- Bounded adaptive request windows using per-peer request/response measurements.
- Region availability across observed peers, plus request-flow diagnostics.
- Guarded accelerated reconnection for formerly productive peers.
- Opt-in resume with hash verification of all claimed pieces for streaming torrents.
- Shared payload-request arbitration across foreground, preparation and background sessions.
- Sequential tracker failover with announce tiers and private-peer teardown.
- Bounded IPv4/IPv6 routing checkpoints with optional routing-only persistence.
- Paused, checkpoint-first piece reclamation with reader retention and sparse storage support.
- Independent selected-piece verification for importing existing partial downloads.
- Opt-in observation-only sessions, sanitized tracker reply observations, and bounded metadata and solicited-payload budgets.

Normal downloads and seeding remain supported. Existing `stream` and `stream_range` callers retain their interfaces. Set `AddTorrentOptions::demand_only_streaming` to opt into the streaming scheduler. Fast resume is a separate session option and remains disabled by default.

## Minimal build

Rust 1.100.0 is pinned in `rust-toolchain.toml`.

```sh
cargo build --locked --release -p librqbit --no-default-features --features rust-tls
cargo test --locked -p librqbit --no-default-features --features rust-tls --test streaming_contract
```

The default workspace selects the library. The minimal build excludes the HTTP server adapters, web UI, desktop, database adapter and optional metrics exporter. TCP, uTP, DHT, trackers and hash verification remain available. Optional adapters can still be explicitly enabled. The library workflow covers Linux and Windows host checks and an Android ARMv7 compile check.

Applications should pin a reviewed Git commit from this fork and use `default-features = false, features = ["rust-tls"]`. Cargo reports the upstream package version 9.1.0. The fork reports `librqbit-streaming.6` through `librqbit::LOCAL_REVISION`. Consumers should record the full Git SHA alongside the runtime revision. This repository does not publish under the upstream crate's crates.io identity.

Set `LIBRQBIT_COMMIT` to the full commit SHA from the
[release](https://github.com/wellington043/librqbit-streaming/releases) you choose.

```sh
cargo add --git https://github.com/wellington043/librqbit-streaming --rev "$LIBRQBIT_COMMIT" --no-default-features --features rust-tls librqbit
```

## Documentation

- [Streaming API and semantics](docs/streaming/api.md)
- [Discovery state and payload priority](docs/streaming/discovery-and-priority.md)
- [Validation and limits](docs/streaming/validation.md)
- [Maintenance and upstream provenance](docs/streaming/maintenance.md)
- [Original upstream README](README.upstream.md)
- [AI assistance disclosure](AI_DISCLOSURE.md)

## License

Apache-2.0. Upstream copyright notices are preserved in `LICENSE`, and the full license is included in `LICENSE-APACHE`. Modified source files identify the fork. This fork is not endorsed by the upstream maintainers.
