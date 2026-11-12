# librqbit-streaming

A streaming-focused Rust library fork of [rqbit](https://github.com/ikatson/rqbit), based on librqbit 9.1.0. The library supplies verified torrent bytes to a media application. It does not include a player, codec implementation or transcoder.

This is an experimental fork. The first milestone focuses on library APIs, correctness and local multi-peer transfers. Public-swarm performance, mobile support and player integration require separate validation.

## Streaming features

- Atomic file-relative range registration and demand-only downloading.
- Playback, probe and prefetch priorities, with deadline hints and fair scheduling among readers at the same priority.
- Generation-checked feedback: consumption rate, available buffer duration and target buffer duration control bounded read-ahead. A seek invalidates old hints.
- Cancellation of obsolete requests, including when a peer request window is full.
- Bounded adaptive request windows using per-peer request/response measurements.
- Region availability across observed peers, plus request-flow diagnostics.
- Guarded accelerated reconnection for formerly productive peers.
- Opt-in resume with hash verification of all claimed pieces for streaming torrents.

Normal downloads and seeding remain supported. Existing `stream` and `stream_range` callers retain their interfaces. Set `AddTorrentOptions::demand_only_streaming` to opt into the streaming scheduler. Fast resume is a separate session option and remains disabled by default.

## Minimal build

Rust 1.100.0 is pinned in `rust-toolchain.toml`.

```sh
cargo build --locked --release -p librqbit --no-default-features --features rust-tls
cargo test --locked -p librqbit --no-default-features --features rust-tls -- --test-threads=1
```

The default workspace selects the library. The minimal build excludes the HTTP server adapters, web UI, desktop, database adapter and optional metrics exporter. TCP, uTP, DHT, trackers and hash verification remain available. Optional adapters can still be explicitly enabled. The upstream desktop release workflows were removed from this fork; CI validates the library.

Applications should pin a reviewed Git commit from this fork and use `default-features = false, features = ["rust-tls"]`. The upstream package version remains 9.1.0; `librqbit::LOCAL_REVISION` identifies this fork's API revision. This repository does not publish under the upstream crate's crates.io identity.

## Documentation

- [Streaming API and semantics](docs/streaming/api.md)
- [Validation and limits](docs/streaming/validation.md)
- [Maintenance and upstream provenance](docs/streaming/maintenance.md)
- [Original upstream README](README.upstream.md)
- [AI assistance disclosure](AI_DISCLOSURE.md)

## License

Apache-2.0. Upstream copyright notices are preserved in `LICENSE`, and the full license is included in `LICENSE-APACHE`. Modified source files identify the fork. This fork is not endorsed by the upstream maintainers.
