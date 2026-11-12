# Streaming library fork

The user authorized a public fork and implementation of the proposed streaming improvements, focusing on the fork before further desktop/core integration. The reference is now Rust itself; no WebTorrent Core comparison or default-player changes belong to this milestone.

## Architecture and scope

Fork `ikatson/rqbit` at the published librqbit 9.1.0 commit `4309d38769ef93f6ee4f01a1ab147009a197fa8f`. Preserve the upstream monorepo/history and import the existing, validated phase-3 streaming patch. Make the default workspace build library-focused. Keep optional applications out of the default build rather than delete useful history or protocol crates. Preserve TCP, uTP, DHT, trackers, seeding and hash verification.

Add a public streaming policy API with per-reader priority, a relative deadline and generation-checked playback feedback (consumption bytes/second, available buffer duration, target duration). Scheduling must use these hints with bounded byte windows, cancellation of obsolete demand and fairness within equal priorities. Existing `stream` and `stream_range` callers keep their behavior unless using the streaming opt-in.

Correct fast-resume hash mismatch handling. For streaming torrents, revalidate every piece claimed by persisted state before serving it, retaining valid pieces and making invalid pieces downloadable. Persisted state never bypasses content verification. This can avoid scanning holes in a sparse cache, but is not zero-I/O resume. Keep fastresume opt-in.

Adapt per-peer request windows from measured request/response latency and throughput, bounded by the configured maximum and the peer's negotiated limit. Prioritize urgent demanded pieces, preserve cancellation and hashing ownership, and expose bounded diagnostics for peer availability and request windows. Report the union and missing count for a requested region, not an average piece count masquerading as availability. Observed peers are only a partial view of the swarm.

## Validation and publication

Use focused regression tests during implementation and one consolidated library validation at the end. Include real loopback TCP transfers, heterogeneous peers, seeks/cancellation and resume corruption. Tests must not need public torrents, the old core, players or a server. Record exact toolchain, commands and results. Run cargo check and clippy, plus library tests and minimal-feature release build once in the consolidated command; do not run the entire upstream desktop workspace.

Publish reviewed source, documentation, licenses and CI to the public fork. Preserve attribution and disclose AI assistance. Do not publish private desktop files, caches, tokens or media. No upstream PR or messages are authorized. The existing desktop worktree and historical phase reports stay untouched.

## Boundaries

Codec/HDR handling, player UX, Android bindings, desktop rollout and full public-swarm performance claims are subsequent milestones. Library API and transport correctness are the delivery here. Remove dependencies from the minimal profile only when demonstrably unnecessary; do not remove interoperability to make a benchmark smaller.
