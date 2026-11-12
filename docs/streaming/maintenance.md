# Maintaining the streaming fork

This repository is a public fork of [ikatson/rqbit](https://github.com/ikatson/rqbit).
Its first streaming branch starts at the source commit used by published
`librqbit 9.1.0`: `4309d38769ef93f6ee4f01a1ab147009a197fa8f`.

## Scope

The library supplies verified bytes to a caller. A player or media coordinator
supplies consumption-rate and buffer hints; container parsing, codecs, rendering,
HDR, UI and platform bindings remain outside the torrent engine. The existing
desktop application is not part of this fork's release or test matrix.

The upstream monorepo is retained so protocol fixes remain easy to integrate.
Only librqbit is a default workspace member. Required protocol crates still build
transitively. Optional HTTP, UI, metrics and database adapters are opt-in; deleting
their source would not reduce the minimal library binary.

## Baseline and local changes

The initial import is the previously validated `webtorrent-phase3.1` patch:
atomic range registration, demand gates across reservation/stealing/issuance,
verified-network adaptive lookahead, abandoned-reservation release, and guarded
early reconnect. It modified ten upstream source files and added
`torrent_state/streaming_policy.rs`. Its original published crate archive has
SHA-256 `4e35a06e481e854f69141928d067406397b3ad7a47bfdc095bc31325f44f5d13`.

Further work is committed as library changes on top of this exact base. Keep
upstream as a separate remote and branch, and review upstream changes before
integrating them. Do not automatically track an unpinned moving branch in a
shipping application. Consumers should pin a reviewed commit.

## Verification

Use focused tests for each change and the consolidated library workflow before
publishing. Performance comparisons use this fork's earlier reviewed Rust
revision, never the former JavaScript engine. Do not equate a scheduler unit test
or local transfer with first-frame latency in a player or performance in a public
swarm. Keep test media generated locally and network tests on loopback.

Preserve ordinary download and seeding behavior outside the streaming opt-in.
Check every scheduling path when changing demand rules, including stealing,
requests already in flight and hashing. The peer availability view covers
observed connected peers, not every peer that could eventually be discovered.

Fast resume remains opt-in. Streaming resume validates claimed pieces; it does
not trust file size or timestamps as proof of unchanged content, and it is not a
zero-I/O resume mechanism. Playback-level error recovery belongs to the consumer.

## Attribution and contributions

Upstream copyright and the Apache-2.0 license remain applicable. Modified files
carry notices. See `AI_DISCLOSURE.md` for this fork's development disclosure.
Contributions sent upstream must separately comply with upstream's contribution
and AI policies; publishing this fork does not submit an upstream contribution.
