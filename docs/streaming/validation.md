# Validation

The fork is validated independently of the previous JavaScript core. Tests use
generated payload and loopback peers. No public torrent or player is needed.

## Commands

```sh
cargo check --locked -p librqbit --no-default-features --features rust-tls,http-api,http-api-client,postgres --all-targets
cargo clippy --locked -p librqbit --no-default-features --features rust-tls --all-targets
cargo test --locked -p librqbit --no-default-features --features rust-tls -- --test-threads=1
cargo build --locked --release -p librqbit --no-default-features --features rust-tls
```

The inherited end-to-end tests were bounded with `E2E_NUM_SERVERS=4` and
`E2E_TIMEOUT=180`. Tests requiring an external torrent client remain ignored.
The new external API contract uses ephemeral loopback ports, two peers with
different upload limits, verified bytes, seek generations and a peer stopping.

## Local results, 2026-11-12

Windows x86_64 MSVC, Rust 1.100.0. Focused checks preceded one consolidated
library test run; no desktop or former-core suite was run.

| Gate | Result |
| --- | --- |
| Formatting of changed Rust files and diff whitespace | Passed |
| Compile all targets with optional HTTP, HTTP client and PostgreSQL | Passed |
| Clippy, minimal library and all targets | Passed with one advisory warning |
| Library tests | 93 passed, 0 failed, 5 ignored; 14.68 s execution |
| External-consumer TCP streaming contract | 1 passed; 4.91 s execution |
| Compiled documentation example | 1 passed |
| Minimal release library | Passed; 1 min 32 s build |

The five inherited ignored tests require public DHT, a live IP-list URL, a Miri
configuration, or another installed torrent client (TCP and uTP). Normal local
TCP, uTP, padded-file and streaming end-to-end tests passed. The Clippy advisory
is `large_enum_variant` on `PeerState`; an allocation-layout change is deferred
until profiling justifies it.

The Windows minimal release also reports unused direct-dependency warnings for
`memmap2`, `nix` and `uuid`. The first two support the optional storage examples
and Unix file I/O respectively. These manifest warnings are recorded separately
from build failures; no additional dependency cleanup was mixed into the final
validated source revision.

The focused batch passed 40 policy, resume and peer-flow tests. A whole-diff agent
review then identified a priority bypass in stealing and already-reserved work.
Six production-path regressions reproduced four failures before the fix and all
six passed afterward; a scoped independent agent re-review approved that fix.
The consolidated run includes these tests, so these counts are not additive.

The first optional-adapter compile exposed an undeclared HTTP logging feature
dependency, which was fixed in feature composition. Clippy setup and lint-only
corrections required repeating that failed gate. Successful broad tests were not
repeated. Original failures and final logs are retained locally under ignored
`target/streaming-validation`; no generated media or local paths are published.

The sparse resume fixture checks all three saved claims (48 KiB) in a 96-piece
file, preserves valid claims and rejects corrupt data without scanning unclaimed
holes. This establishes the I/O behavior for that fixture, not a universal startup
time. Hash validation is still required before serving claimed bytes.

## Evidence boundary

The minimal normal-dependency graph on Windows dropped from 252 to 243 unique
packages by feature-gating HTTP-only adapters. This is a dependency-graph count,
not a measured binary-size or runtime-speed improvement.

These checks do not establish public-swarm performance, long-session stability,
first-frame latency, HDR, Android support or production PostgreSQL behavior.
The HTTP adapter and the PostgreSQL persistence code were compile-checked as
optional features; live database behavior requires its own environment.

## Linux CI

[Streaming library run 43319303129](https://github.com/wellington043/librqbit-streaming/actions/runs/43319303129)
passed on source commit `5db5a8079ebf3e7911019c835ea049bbc3fab906`:
optional HTTP compilation, Clippy, library/external/documentation tests and the
minimal release build. PostgreSQL was covered by the local compile above, not
by this Linux workflow. Subsequent publication-evidence changes are documentation
only and do not require another source test run.
