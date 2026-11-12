# Streaming Fork Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development. User authorization already covers implementation and publication; no further approval checkpoint is required.

**Goal:** Publish a maintainable librqbit fork with streaming control, safe resume, adaptive peers and actionable diagnostics.

**Architecture:** Keep upstream history and protocol crates. Import the validated phase-3 patch, add opt-in public policy APIs, and build the library without optional application dependencies by default.

**Tech Stack:** Rust 1.100.0, librqbit 9.1.0, Tokio, existing dependency lockfile.

**Spec:** `docs/superpowers/specs/2026-11-12-streaming-fork.md`

## Global Constraints

- Work only in the new fork checkout; preserve the desktop and old core.
- No old-core comparisons or public-torrent downloads.
- Keep existing APIs compatible; new policy is opt-in.
- Never serve saved cache pieces without required hash verification.
- Bounds and cancellation apply to every request path.
- Focused tests first; one consolidated validation after the review/fix wave.
- No publishing secrets, local artifacts or media; preserve Apache attribution and disclose AI assistance.

## Review Focus

- Stale playback feedback after seek must not reprioritize the wrong generation.
- Overlapping readers and probe/prefetch traffic must not starve current playback.
- Corrupt saved payload, malformed bitfields and padding bits must not become verified data.
- Slow peers, zero/low negotiated request limits and canceled requests must preserve bounds.
- Default non-streaming callers and minimal library build must remain supported.

## Tasks

### 1. Establish fork and library profile (root)

- [x] Create public GitHub fork and isolated fresh checkout from the 9.1.0 commit.
- [x] Import the 10 modified upstream files plus streaming policy module from phase 3.
- [x] Set default workspace members to librqbit; gate unused HTTP adapter dependencies where safe.
- [x] Add fork README, provenance, AI disclosure and library-only CI.

### 2. Public streaming control (streaming agent)

Files: `crates/librqbit/src/torrent_state/streaming.rs`, `streaming_policy.rs`, new `streaming_control.rs`, `lib.rs`, `torrent_state/mod.rs`.

Interfaces: `StreamPriority`, `StreamingReadOptions`, `PlaybackHint`; `ManagedTorrent::stream_range_with_options`; `FileStream::generation`, `update_playback`; existing methods remain available. Relative deadlines are captured as monotonic deadlines; stale generation updates return a typed error. Feedback has explicit byte rate and Duration values with bounded conversion.

- [x] Add focused policy/feedback/seek tests, port applicable phase-3 regressions.
- [x] Implement priority/deadline ordering, adaptive time-based windows and cancellation notifications.
- [x] Expose public bounded policy diagnostics and document exact semantics.

### 3. Safe resume (resume agent)

Files: `torrent_state/initializing.rs`, `file_ops.rs`, supporting resume test module if needed. Avoid shared scheduler files.

- [x] Reproduce `Ok(false)` hash mismatch handling with real temporary payload.
- [x] Fix mismatch rejection and validate malformed/padded bitfields.
- [x] For streaming opt-in, check all claimed pieces and preserve only valid pieces instead of rescanning the whole sparse payload on one corrupt piece.
- [x] Verify fresh session restart, corruption, sparse/missing data and cancellation with focused tests. Keep fastresume opt-in and explain residual I/O.

### 4. Adaptive peer flow and availability (peer agent)

Files: `torrent_state/live/mod.rs`, `live/peer/mod.rs`, `live/peer/stats/snapshot.rs`, `live/peers/mod.rs`, new small policy module if useful. Coordinate public reexports through root/API agent.

- [x] Add bounded adaptive request window tests for slow/fast peers, negotiated limits, cancellation and stale samples.
- [x] Integrate measurements into request issuance/response handling, preserving default behavior outside streaming opt-in.
- [x] Expose typed bounded availability diagnostics from observed peer bitfields and per-peer transport/flow stats; mask padding and count union correctly.
- [x] Keep urgent range selection compatible with the new streaming iterator and existing steal protections.

### 5. Integration, review and publication (root)

- [x] Add an external-consumer API example and real loopback multi-peer streaming tests.
- [x] Review the complete diff once; fix concrete findings with focused checks.
- [x] Run one consolidated library validation: formatting of changed files, cargo check, clippy, library tests, external API test and minimal release build. Preserve logs outside tracked source.
- [ ] Commit only fork-owned files; push the feature branch and make it the fork default after verification.
- [ ] Verify the public repo's parent, branch, commit and CI state; report proven limits and follow-up integration work.
