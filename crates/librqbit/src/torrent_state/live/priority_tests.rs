//! Deterministic requester/response-path tests. The writer channel is the wire
//! boundary: responses are gated explicitly instead of using network sleeps.
use super::*;
use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, FileStream, ManagedTorrent, Session,
    SessionOptions, StreamPriority, StreamingReadOptions, create_torrent,
    spawn_utils::BlockingSpawner,
};
use tokio::{io::AsyncReadExt, task::JoinHandle, time::timeout};

const PIECE_LEN: u32 = 1024 * 1024;

struct Harness {
    _source: tempfile::TempDir,
    _cache: tempfile::TempDir,
    session: Arc<Session>,
    torrent: Arc<ManagedTorrent>,
    handler: Arc<PeerHandler>,
    rx: PeerRx,
    payload: Vec<u8>,
    requester: Option<JoinHandle<crate::Result<()>>>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        if let Some(task) = self.requester.take() {
            task.abort();
        }
    }
}

impl Harness {
    async fn new(advertised: &[u32]) -> anyhow::Result<Self> {
        let source = tempfile::tempdir()?;
        let cache = tempfile::tempdir()?;
        let payload: Vec<_> = (0..2 * PIECE_LEN as usize)
            .map(|i| u8::try_from((i ^ (i >> 9)) % 251).expect("fixture byte is below 251"))
            .collect();
        let file = source.path().join("priority.bin");
        std::fs::write(&file, &payload)?;
        let created = create_torrent(
            &file,
            CreateTorrentOptions {
                piece_length: Some(PIECE_LEN),
                ..Default::default()
            },
            &BlockingSpawner::new(1),
        )
        .await?;
        let session = Session::new_with_opts(
            cache.path().into(),
            SessionOptions {
                dht: None,
                disable_trackers: true,
                disable_local_service_discovery: true,
                ..Default::default()
            },
        )
        .await?;
        let torrent = session
            .add_torrent(
                AddTorrent::from_bytes(created.as_bytes()?),
                Some(AddTorrentOptions {
                    demand_only_streaming: true,
                    peer_opts: Some(PeerConnectionOptions {
                        max_request_window: Some(2),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .context("torrent handle")?;
        torrent.wait_until_initialized().await?;
        let state = torrent.live().context("live torrent")?;
        let addr: SocketAddr = "127.0.0.1:18888".parse()?;
        state.peers.add_if_not_seen(addr).unwrap();
        let (tx, rx) = unbounded_channel();
        let counters = state
            .peers
            .with_peer_mut(addr, "test peer", |peer| {
                peer.incoming_connection(
                    Id20::default(),
                    tx.clone(),
                    &state.peers,
                    ConnectionKind::Tcp,
                )
                .unwrap();
                peer.stats.counters.clone()
            })
            .unwrap();
        let mut bitfield = make_piece_bitfield(&state.lengths);
        for piece in advertised {
            bitfield.set(*piece as usize, true);
        }
        state.peers.update_bitfield(addr, bitfield, 2).unwrap();
        let handler = Arc::new(PeerHandler {
            download_permits: Mutex::new(HashMap::new()),
            addr,
            incoming: true,
            verified_at_start: 0,
            recovery_attempt: None,
            on_bitfield_notify: Default::default(),
            flow_control: Mutex::new(PeerFlowControl::new(2)),
            tx,
            counters,
            first_message_received: AtomicBool::new(false),
            cancel_token: state.cancellation_token.child_token(),
            client_name_and_version: "priority test".to_owned(),
            state,
        });
        handler.on_i_am_unchoked();
        Ok(Self {
            _source: source,
            _cache: cache,
            session,
            torrent,
            handler,
            rx,
            payload,
            requester: None,
        })
    }

    async fn reader(&self, piece: u32, priority: StreamPriority) -> anyhow::Result<FileStream> {
        self.torrent
            .clone()
            .stream_range_with_options(
                0,
                u64::from(piece * PIECE_LEN),
                u64::from((piece + 1) * PIECE_LEN),
                StreamingReadOptions {
                    priority,
                    deadline: None,
                },
            )
            .await
    }

    fn run(&mut self) {
        let handler = self.handler.clone();
        self.requester = Some(tokio::spawn(async move {
            handler.task_peer_chunk_requester().await
        }));
    }

    async fn next(&mut self) -> anyhow::Result<WriterRequest> {
        timeout(Duration::from_secs(2), self.rx.recv())
            .await
            .context("requester made no progress")?
            .context("writer closed")
    }

    async fn request(&mut self) -> anyhow::Result<Request> {
        loop {
            if let WriterRequest::Message(Message::Request(req)) = self.next().await? {
                return Ok(req);
            }
        }
    }

    async fn respond(&self, request: Request) -> anyhow::Result<()> {
        let offset = request.index as usize * PIECE_LEN as usize + request.begin as usize;
        self.handler
            .on_received_piece(Piece::from_data(
                request.index,
                request.begin,
                &self.payload[offset..offset + request.length as usize],
            ))
            .await
    }

    fn issue(&self, piece: ValidPieceIndex, chunk: usize) -> crate::Result<StreamIssue> {
        let chunk = self
            .handler
            .state
            .lengths
            .iter_chunk_infos(piece)
            .nth(chunk)
            .unwrap();
        self.handler
            .issue_stream_chunk(chunk, Request::new(piece.get(), chunk.offset, chunk.size))
    }

    fn is_have(&self, piece: u32) -> bool {
        let state = &self.handler.state;
        state
            .lock_read("test have")
            .get_chunks()
            .unwrap()
            .is_piece_have(state.lengths.validate_piece_index(piece).unwrap())
    }

    async fn stop(&mut self) {
        if let Some(task) = self.requester.take() {
            task.abort();
            let _ = task.await;
        }
        self.session.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn priority_regression_full_queue_preempts_open_prefetch_and_resumes_verified_reads()
-> anyhow::Result<()> {
    let mut h = Harness::new(&[0, 1]).await?;
    let mut lower = h.reader(0, StreamPriority::Prefetch).await?;
    h.run();
    let first = h.request().await?;
    let second = h.request().await?;
    assert_eq!((first.index, second.index), (0, 0));
    // Receive one real chunk, then gate the peer with a full queue again.
    h.respond(first).await?;
    let third = h.request().await?;
    assert_eq!(third.index, 0);
    assert!(!h.is_have(0), "a partial piece must not become readable");
    let mut playback = h.reader(1, StreamPriority::Playback).await?;
    let mut cancelled = HashSet::new();
    let first_high = loop {
        match h.next().await? {
            WriterRequest::Message(Message::Cancel(req)) => {
                cancelled.insert((req.index, req.begin));
            }
            WriterRequest::Message(Message::Request(req)) => {
                assert_eq!(
                    req.index, 1,
                    "must preempt before feeding the remaining low-priority chunks"
                );
                break req;
            }
            _ => {}
        }
    };
    assert_eq!(
        cancelled,
        HashSet::from([(0, second.begin), (0, third.begin)])
    );
    // Late canceled payload must be ignored, including maliciously wrong data.
    let bogus = vec![255; second.length as usize];
    h.handler
        .on_received_piece(Piece::from_data(0, second.begin, &bogus))
        .await?;
    h.handler
        .on_received_piece(Piece::from_data(0, third.begin, &bogus))
        .await?;
    assert!(!h.is_have(0));
    h.respond(first_high).await?;
    let mut probe = [255; 1];
    assert!(
        timeout(Duration::from_millis(20), playback.read(&mut probe))
            .await
            .is_err()
    );
    assert_eq!(probe, [255], "partial/unverified data must not be served");
    while !h.is_have(1) {
        let request = h.request().await?;
        assert_eq!(request.index, 1);
        h.respond(request).await?;
    }
    let mut verified = Vec::new();
    playback.read_to_end(&mut verified).await?;
    assert_eq!(verified, h.payload[PIECE_LEN as usize..]);
    drop(playback);
    while !h.is_have(0) {
        let request = h.request().await?;
        assert_eq!(request.index, 0);
        h.respond(request).await?;
    }
    let mut resumed = Vec::new();
    lower.read_to_end(&mut resumed).await?;
    assert_eq!(resumed, h.payload[..PIECE_LEN as usize]);
    drop(lower);
    h.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn priority_regression_issuance_preempts_before_queue_is_full() -> anyhow::Result<()> {
    let mut h = Harness::new(&[0, 1]).await?;
    let lower = h.reader(0, StreamPriority::Probe).await?;
    let piece = h.handler.acquire_next_piece()?.unwrap();
    assert!(matches!(h.issue(piece, 0)?, StreamIssue::Issued));
    let higher = h.reader(1, StreamPriority::Playback).await?;
    assert!(matches!(h.issue(piece, 1)?, StreamIssue::Abandon));
    assert_eq!(h.handler.acquire_next_piece()?.unwrap().get(), 1);
    assert_eq!(h.handler.requested_inflight_count(), Some(0));
    drop((lower, higher));
    h.stop().await;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn priority_regression_overlap_and_peer_eligibility_keep_useful_owned_piece()
-> anyhow::Result<()> {
    for advertised in [&[0u32][..], &[0u32, 1][..]] {
        let mut h = Harness::new(advertised).await?;
        let lower = h.reader(0, StreamPriority::Prefetch).await?;
        let piece = h.handler.acquire_next_piece()?.unwrap();
        assert!(matches!(h.issue(piece, 0)?, StreamIssue::Issued));
        let overlap = if advertised.len() == 2 {
            Some(h.reader(0, StreamPriority::Playback).await?)
        } else {
            None
        };
        let higher = h.reader(1, StreamPriority::Playback).await?;
        h.handler.release_obsolete_stream_requests();
        assert_eq!(h.handler.requested_inflight_count(), Some(1));
        assert!(matches!(h.issue(piece, 1)?, StreamIssue::Issued));
        assert_eq!(
            h.handler
                .state
                .lock_read("owner")
                .get_pieces()?
                .get_inflight(piece)
                .unwrap()
                .peer,
            h.handler.addr
        );
        drop((lower, overlap, higher));
        h.stop().await;
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn priority_regression_preemption_respects_write_hash_lock_and_ownership()
-> anyhow::Result<()> {
    let mut h = Harness::new(&[0, 1]).await?;
    let lower = h.reader(0, StreamPriority::Prefetch).await?;
    let piece = h.handler.acquire_next_piece()?.unwrap();
    assert!(matches!(h.issue(piece, 0)?, StreamIssue::Issued));
    let higher = h.reader(1, StreamPriority::Playback).await?;
    {
        let state = h.handler.state.clone();
        let _write_in_progress = state.per_piece_locks[0].read();
        h.handler.release_obsolete_stream_requests();
        assert_eq!(h.handler.requested_inflight_count(), Some(1));
        assert!(matches!(h.issue(piece, 1)?, StreamIssue::Retry));
        assert_eq!(
            state
                .lock_read("owner")
                .get_pieces()?
                .get_inflight(piece)
                .unwrap()
                .peer,
            h.handler.addr
        );
    }
    h.handler.release_obsolete_stream_requests();
    assert_eq!(h.handler.requested_inflight_count(), Some(0));
    assert!(
        h.handler
            .state
            .lock_read("owner")
            .get_pieces()?
            .get_inflight(piece)
            .is_none()
    );
    assert!(matches!(h.issue(piece, 1)?, StreamIssue::Abandon));
    drop((lower, higher));
    h.stop().await;
    Ok(())
}
