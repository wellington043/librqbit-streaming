// Modified for WebTorrent Desktop streaming phase 3.

pub mod request_flow;
pub mod stats;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::{Arc, atomic::Ordering};
use std::time::Instant;

use librqbit_core::hash_id::Id20;
use librqbit_core::lengths::{ChunkInfo, ValidPieceIndex};
use peer_binary_protocol::{Message, Request};

use tokio::sync::{
    Notify,
    mpsc::{UnboundedReceiver, UnboundedSender, unbounded_channel},
};
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::peer_connection::WriterRequest;
use crate::stream_connect::ConnectionKind;
use crate::type_aliases::BF;

use self::request_flow::{AdaptiveRequestWindow, PeerRequestFlowSnapshot};
use super::PeerStates;

pub(crate) type InflightRequest = ChunkInfo;
pub(crate) type PeerRx = UnboundedReceiver<WriterRequest>;
pub(crate) type PeerTx = UnboundedSender<WriterRequest>;

#[derive(Debug)]
pub(crate) struct Peer {
    pub addr: SocketAddr,
    state: PeerState,
    pub stats: stats::atomic::PeerStats,
    pub outgoing_address: Option<SocketAddr>,
    pub stream_retry: Option<CancellationToken>,
}

impl Peer {
    pub fn new_live_for_incoming_connection(
        addr: SocketAddr,
        peer_id: Id20,
        tx: PeerTx,
        counters: &PeerStates,
        connection_kind: ConnectionKind,
    ) -> Self {
        let state = PeerState::Live(LivePeerState::new(peer_id, tx, true, connection_kind));
        for counter in [&counters.session_stats, &counters.stats] {
            counter.inc(&state);
        }
        Self {
            addr,
            state,
            stats: Default::default(),
            outgoing_address: None,
            stream_retry: None,
        }
    }

    pub fn new_with_outgoing_address(addr: SocketAddr) -> Self {
        Self {
            addr,
            outgoing_address: Some(addr),
            stream_retry: None,
            stats: Default::default(),
            state: Default::default(),
        }
    }

    pub(crate) fn reconnect_not_needed_peer(
        &mut self,
        counters: &PeerStates,
    ) -> Option<SocketAddr> {
        if let PeerState::NotNeeded = self.get_state() {
            match self.outgoing_address {
                None => None,
                Some(socket_addr) if self.addr == socket_addr => {
                    self.set_state(PeerState::Queued, counters);
                    Some(socket_addr)
                }
                Some(socket_addr) => {
                    debug!(
                        peer = %self.addr,
                        outgoing_addr = %socket_addr,
                        "peer will by retried on different address",
                    );
                    Some(socket_addr)
                }
            }
        } else {
            None
        }
    }
}

#[derive(Debug, Default)]
pub(crate) enum PeerState {
    #[default]
    // Will be tried to be connected as soon as possible.
    Queued,
    Connecting(PeerTx),
    Live(LivePeerState),
    // There was an error, and it's waiting for exponential backoff.
    Dead,
    // We don't need to do anything with the peer any longer.
    // The peer has the full torrent, and we have the full torrent, so no need
    // to keep talking to it.
    NotNeeded,
}

impl std::fmt::Display for PeerState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

impl PeerState {
    pub fn name(&self) -> &'static str {
        match self {
            PeerState::Queued => "queued",
            PeerState::Connecting(_) => "connecting",
            PeerState::Live(_) => "live",
            PeerState::Dead => "dead",
            PeerState::NotNeeded => "not needed",
        }
    }

    pub fn take_live_no_counters(self) -> Option<LivePeerState> {
        match self {
            PeerState::Live(l) => Some(l),
            _ => None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum IncomingConnectionResult {
    #[error("peer already active")]
    AlreadyActive,
}

impl Peer {
    pub fn get_state(&self) -> &PeerState {
        &self.state
    }

    pub fn take_state(&mut self, counters: &PeerStates) -> PeerState {
        self.set_state(Default::default(), counters)
    }

    pub fn destroy(self, counters: &PeerStates) {
        for counter in [&counters.session_stats, &counters.stats] {
            counter.dec(&self.state);
        }
        if let (Some(addr), PeerState::Live(..)) = (self.outgoing_address, &self.state) {
            counters.live_outgoing_peers.write().remove(&addr);
        }
    }

    pub fn set_state(&mut self, new: PeerState, counters: &PeerStates) -> PeerState {
        if !matches!(new, PeerState::Dead)
            && let Some(retry) = self.stream_retry.take()
        {
            retry.cancel();
        }
        for counter in [&counters.session_stats, &counters.stats] {
            counter.incdec(&self.state, &new);
        }
        if let Some(addr) = self.outgoing_address {
            if matches!(&self.state, PeerState::Live(..)) {
                counters.live_outgoing_peers.write().remove(&addr);
            }
            if matches!(&new, PeerState::Live(..))
                && self
                    .stats
                    .counters
                    .outgoing_connections
                    .load(Ordering::Relaxed)
                    > 0
            {
                counters.live_outgoing_peers.write().insert(addr);
            }
        }

        std::mem::replace(&mut self.state, new)
    }

    pub fn get_live(&self) -> Option<&LivePeerState> {
        match &self.state {
            PeerState::Live(l) => Some(l),
            _ => None,
        }
    }

    pub fn get_live_mut(&mut self) -> Option<&mut LivePeerState> {
        match &mut self.state {
            PeerState::Live(l) => Some(l),
            _ => None,
        }
    }

    pub fn idle_to_connecting(&mut self, counters: &PeerStates) -> Option<(PeerRx, PeerTx)> {
        match &self.state {
            PeerState::Queued | PeerState::NotNeeded => {
                let (tx, rx) = unbounded_channel();
                let tx_2 = tx.clone();
                self.set_state(PeerState::Connecting(tx), counters);
                Some((rx, tx_2))
            }
            _ => None,
        }
    }

    pub fn incoming_connection(
        &mut self,
        peer_id: Id20,
        tx: PeerTx,
        counters: &PeerStates,
        connection_kind: ConnectionKind,
    ) -> Result<(), IncomingConnectionResult> {
        if matches!(&self.state, PeerState::Connecting(..) | PeerState::Live(..)) {
            return Err(IncomingConnectionResult::AlreadyActive);
        }
        match self.take_state(counters) {
            PeerState::Queued | PeerState::Dead | PeerState::NotNeeded => {
                self.set_state(
                    PeerState::Live(LivePeerState::new(peer_id, tx, true, connection_kind)),
                    counters,
                );
            }
            PeerState::Connecting(..) | PeerState::Live(..) => unreachable!(),
        }
        Ok(())
    }

    pub fn connecting_to_live(
        &mut self,
        peer_id: Id20,
        counters: &PeerStates,
        conn_kind: ConnectionKind,
    ) -> Option<&mut LivePeerState> {
        if let PeerState::Connecting(_) = &self.state {
            let tx = match self.take_state(counters) {
                PeerState::Connecting(tx) => tx,
                _ => unreachable!(),
            };
            self.set_state(
                PeerState::Live(LivePeerState::new(peer_id, tx, false, conn_kind)),
                counters,
            );
            self.get_live_mut()
        } else {
            None
        }
    }

    pub fn set_not_needed(&mut self, counters: &PeerStates) -> PeerState {
        self.set_state(PeerState::NotNeeded, counters)
    }
}

#[derive(Debug)]
pub(crate) struct LivePeerState {
    #[allow(dead_code)]
    peer_id: Id20,

    pub client_name: Option<String>,

    pub peer_interested: bool,
    pub stream_unchoked: bool,

    // This is used to track the pieces the peer has.
    pub bitfield: BF,

    // Whether the bitfield above covers the whole torrent, i.e. the peer is a seeder.
    // Cached so that the aggregate seeder counter can be maintained on transitions
    // instead of scanning every peer's bitfield on each stats call. Kept in sync
    // with the bitfield by PeerStates::update_seeder_flag.
    pub seeder: bool,

    // When the peer sends us data this is used to track if we asked for it.
    inflight_requests: HashSet<InflightRequest>,

    // Allocated only for torrents using the streaming opt-in.
    request_flow: Option<AdaptiveRequestWindow>,

    // Bounded tolerance for chunks that arrive after we cancel requests.
    // This is intentionally approximate: we track a count instead of storing every
    // canceled chunk, so a peer is not disconnected for racing our cancel messages.
    late_cancelled_request_tolerance: u32,

    request_slots_changed: Arc<Notify>,

    // The main channel to send requests to peer.
    pub tx: PeerTx,

    pub connection_kind: ConnectionKind,
}

impl LivePeerState {
    pub fn new(
        peer_id: Id20,
        tx: PeerTx,
        initial_interested: bool,
        connection_kind: ConnectionKind,
    ) -> Self {
        LivePeerState {
            peer_id,
            client_name: None,
            peer_interested: initial_interested,
            stream_unchoked: false,
            bitfield: BF::default(),
            seeder: false,
            inflight_requests: Default::default(),
            request_flow: None,
            late_cancelled_request_tolerance: 0,
            request_slots_changed: Default::default(),
            tx,
            connection_kind,
        }
    }

    pub fn has_full_torrent(&self, total_pieces: usize) -> bool {
        self.bitfield.get(0..total_pieces).is_some_and(|s| s.all())
    }

    pub fn request_slots_changed(&self) -> Arc<Notify> {
        self.request_slots_changed.clone()
    }

    pub fn requested_inflight_count(&self) -> usize {
        self.inflight_requests.len()
    }

    pub fn add_inflight_request(&mut self, chunk: ChunkInfo) -> bool {
        let added = self.inflight_requests.insert(chunk);
        if added && let Some(flow) = self.request_flow.as_mut() {
            flow.on_request(chunk, Instant::now());
        }
        added
    }

    pub fn streaming_request_window(&mut self, limit: usize, now: Instant) -> usize {
        let flow = self
            .request_flow
            .get_or_insert_with(|| AdaptiveRequestWindow::new(limit));
        flow.set_limit(limit);
        flow.window(now)
    }

    pub fn request_flow_snapshot(&self) -> Option<PeerRequestFlowSnapshot> {
        self.request_flow.as_ref().map(|flow| {
            let mut snapshot = flow.snapshot(Instant::now());
            snapshot.inflight_requests = self.inflight_requests.len();
            snapshot
        })
    }

    pub fn remove_inflight_request(&mut self, chunk: &ChunkInfo) -> RemoveInflightRequestResult {
        if self.inflight_requests.remove(chunk) {
            if let Some(flow) = self.request_flow.as_mut() {
                flow.on_response(chunk, Instant::now());
            }
            self.request_slots_changed.notify_waiters();
            return RemoveInflightRequestResult::Expected;
        }

        if self.late_cancelled_request_tolerance > 0 {
            // This may be any unexpected chunk, not necessarily the exact canceled
            // one, but the tolerance is bounded by cancels we sent.
            self.late_cancelled_request_tolerance -= 1;
            RemoveInflightRequestResult::LateCanceled
        } else {
            RemoveInflightRequestResult::Unexpected
        }
    }

    pub fn cancel_inflight_requests_for_piece(&mut self, piece: ValidPieceIndex) {
        if let Some(flow) = self.request_flow.as_mut() {
            flow.cancel_piece(piece);
        }
        let tx = &self.tx;
        let late_cancelled_request_tolerance = &mut self.late_cancelled_request_tolerance;
        let before = self.inflight_requests.len();
        self.inflight_requests.retain(|req| {
            if req.piece_index == piece {
                let _ = tx.send(WriterRequest::Message(Message::Cancel(Request {
                    index: piece.get(),
                    begin: req.offset,
                    length: req.size,
                })));
                *late_cancelled_request_tolerance =
                    late_cancelled_request_tolerance.saturating_add(1);
                false
            } else {
                true
            }
        });

        if self.inflight_requests.len() != before {
            self.request_slots_changed.notify_waiters();
        }
    }

    pub fn inflight_requests(&self) -> impl Iterator<Item = &InflightRequest> {
        self.inflight_requests.iter()
    }

    pub fn inflight_requests_debug(&self) -> &HashSet<InflightRequest> {
        &self.inflight_requests
    }
}

pub(crate) enum RemoveInflightRequestResult {
    Expected,
    LateCanceled,
    Unexpected,
}

#[cfg(test)]
mod streaming_flow_tests {
    use super::*;
    use librqbit_core::lengths::Lengths;
    use std::time::Duration;

    #[tokio::test]
    async fn streaming_flow_cancellation_frees_slots_and_ignores_late_sample() {
        let (tx, mut rx) = unbounded_channel();
        let mut live = LivePeerState::new(Id20::default(), tx, false, ConnectionKind::Tcp);
        let lengths = Lengths::new(32768, 32768).unwrap();
        let piece = lengths.validate_piece_index(0).unwrap();
        let chunks: Vec<_> = lengths.iter_chunk_infos(piece).collect();
        live.streaming_request_window(2, Instant::now());
        for chunk in &chunks {
            assert!(live.add_inflight_request(*chunk));
        }
        let notify = live.request_slots_changed();
        let changed = notify.notified();
        live.cancel_inflight_requests_for_piece(piece);
        tokio::time::timeout(Duration::from_millis(50), changed)
            .await
            .unwrap();
        assert_eq!(live.requested_inflight_count(), 0);
        let mut cancelled = HashSet::new();
        for _ in &chunks {
            let WriterRequest::Message(Message::Cancel(req)) = rx.try_recv().unwrap() else {
                panic!("expected cancel")
            };
            cancelled.insert((req.index, req.begin, req.length));
        }
        for chunk in &chunks {
            assert!(cancelled.contains(&(0, chunk.offset, chunk.size)));
            assert!(matches!(
                live.remove_inflight_request(chunk),
                RemoveInflightRequestResult::LateCanceled
            ));
        }
        assert!(
            live.request_flow_snapshot()
                .unwrap()
                .response_latency_ms
                .is_none()
        );
        assert!(matches!(
            live.remove_inflight_request(&chunks[0]),
            RemoveInflightRequestResult::Unexpected
        ));
        assert!(live.add_inflight_request(chunks[0]));
    }

    #[test]
    fn non_streaming_peer_does_not_enable_adaptive_measurements() {
        let (tx, _rx) = unbounded_channel();
        let mut live = LivePeerState::new(Id20::default(), tx, false, ConnectionKind::Tcp);
        let lengths = Lengths::new(16384, 16384).unwrap();
        let chunk = lengths
            .iter_chunk_infos(lengths.validate_piece_index(0).unwrap())
            .next()
            .unwrap();
        assert!(live.add_inflight_request(chunk));
        assert!(matches!(
            live.remove_inflight_request(&chunk),
            RemoveInflightRequestResult::Expected
        ));
        assert!(live.request_flow_snapshot().is_none());
    }
}
