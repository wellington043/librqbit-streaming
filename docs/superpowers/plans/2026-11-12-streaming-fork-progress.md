# Streaming fork progress

Ruling: the user's current instruction authorizes the fork, implementation and public push. Prior analysis supplied the architecture; proceed without another approval question.

Ruling: use a fresh dedicated checkout rather than change the dirty desktop worktree. Preserve upstream history and use `codex/streaming-foundation` from the exact 9.1.0 commit.

Ruling: limit builds and CI to the library and required protocol crates. Deleting UI source does not reduce the linked library; excluding it from default builds is the useful optimization.

Ruling: perform focused red/green checks and one consolidated validation, following the user's repeated instruction; do not run full workspace suites after every task. Parallel agents own disjoint files. Root coordinates Cargo runs to avoid build contention.

Ruling: retain the currently supported torrent protocol. The upstream instruction about BEP52 applies when implementing BEP52; this milestone does not rewrite metainfo or drop existing v1 compatibility.

Ruling: fast resume for streaming rechecks claimed pieces and never promises zero I/O. File metadata alone is insufficient proof of unchanged contents.

Public fork created and verified as a public fork of ikatson/rqbit. Existing phase-3 modifications imported; desktop source unchanged. AI assistance will be disclosed; no upstream contribution is being submitted.

Tasks 2-4 implemented. Root formatted changed Rust files and ran one focused batch: 40 passed, 0 failed, 52 filtered, 2.10 seconds. One external-consumer contract with two rate-limited loopback TCP seeds passed in 4.92 seconds. Its first compile exposed a test-only option nesting typo, corrected before that run; no product fix was needed for that compile error.

Minimal normal dependency graph: 252 -> 243 unique packages on Windows, with nine HTTP-adapter packages absent. Cargo.lock unchanged. Optional HTTP/PostgreSQL dependencies fetched from the existing lockfile; no dependency upgrade.

Whole-diff independent review found one P1: priority was not enforced for already reserved lower-priority work or the initial stealing pass. Publication is held until one coherent fix wave adds production-path regressions, passes a scoped review, then the single consolidated validation. No old-core test or comparison is part of this milestone.

Ruling: consolidate source review once across the three disjoint implementation tasks, then one focused fix review, to avoid repeated broad reviews and suites. The complete review and each worker report are retained locally under ignored target/streaming-validation.

The P1 is fixed. Six production-path priority regressions ran red (four failed, two passed), then green (six passed). The scoped independent re-review approved the fix with no new blocking finding. Reservation, stealing, full-queue reconciliation and per-chunk issuance now respect eligible higher-priority work without canceling overlapping Playback demand.

Consolidated validation: the optional-adapter compile exposed an inherited implicit feature dependency: HTTP logging used tracing-subscriber-utils without enabling it. The http-api feature now declares that dependency. The repeated failed compile gate passed with HTTP, HTTP client and PostgreSQL enabled. Clippy initially lacked its toolchain component; after installation it identified an intentionally reversed test range and numeric-conversion warnings. Narrow fixes preserve the invalid-input test and use checked/saturating conversions. Clippy then passed with one advisory large_enum_variant warning for PeerState; no allocation-layout redesign was included. Successful focused batches were not repeated.

The single consolidated Windows library test run passed: 93 library tests, one external-consumer TCP contract and one documentation compilation; five inherited environment-specific tests ignored. Both inherited TCP and uTP end-to-end tests passed. The minimal release build passed in 1m32s. Formatting and diff whitespace passed. The final Clippy, test and release stages recorded the same unchanged source fingerprint. The optional HTTP/PostgreSQL compile preceded the final lint-only conversions. Cargo.lock remains unchanged. See docs/streaming/validation.md for advisory warnings and evidence limits.
