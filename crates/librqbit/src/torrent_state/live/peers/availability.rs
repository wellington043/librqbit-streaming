// Modified for the librqbit-streaming fork.
//! Bounded availability from the connected peers observed at snapshot time.

use std::ops::Range;

use serde::Serialize;

use super::PeerStates;

/// Maximum pieces inspected per query. Query larger files a region at a time.
pub const MAX_AVAILABILITY_PIECES: u32 = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PeerAvailabilityView {
    /// No connected peer has supplied a usable piece map.
    Unknown,
    /// Some connected peers have not supplied a usable piece map.
    Partial,
    /// All currently connected peers have supplied a usable piece map.
    /// This is still only an observed subset of the swarm.
    Observed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct PeerRegionAvailability {
    /// Absolute torrent piece indices, end exclusive.
    pub piece_range: Range<u32>,
    /// Union of pieces advertised by known connected peers in this region.
    pub available_pieces: u32,
    /// Pieces absent from that union. Unknown peers may still have them.
    pub missing_pieces: u32,
    pub connected_peers: usize,
    pub peers_with_known_bitfield: usize,
    pub view: PeerAvailabilityView,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum PeerAvailabilityError {
    #[error("torrent metadata or live peer state is unavailable")]
    Unavailable,
    #[error("file index is invalid")]
    InvalidFile,
    #[error("byte range is invalid for this file")]
    InvalidByteRange,
    #[error("piece range is invalid for this torrent")]
    InvalidPieceRange,
    #[error("availability region exceeds {max_pieces} pieces")]
    RegionTooLarge { max_pieces: u32 },
}

impl PeerStates {
    pub(crate) fn region_availability(
        &self,
        piece_range: Range<u32>,
        total_pieces: u32,
    ) -> Result<PeerRegionAvailability, PeerAvailabilityError> {
        if piece_range.start > piece_range.end || piece_range.end > total_pieces {
            return Err(PeerAvailabilityError::InvalidPieceRange);
        }
        let count = piece_range.end - piece_range.start;
        if count > MAX_AVAILABILITY_PIECES {
            return Err(PeerAvailabilityError::RegionTooLarge {
                max_pieces: MAX_AVAILABILITY_PIECES,
            });
        }
        // One bit per requested piece, never a clone of any whole bitfield or
        // allocation sized by the entire torrent. Peer locks are held one at
        // a time; this is a best-effort observation while peers can churn.
        let mut union = bitvec::bitvec![u8, bitvec::order::Msb0; 0; count as usize];
        let mut connected_peers = 0;
        let mut peers_with_known_bitfield = 0;
        for peer in self.states.iter() {
            let Some(live) = peer.get_live() else {
                continue;
            };
            connected_peers += 1;
            if live.bitfield.len() < total_pieces as usize {
                continue;
            }
            peers_with_known_bitfield += 1;
            // Restrict to valid piece indices even if the peer set padding.
            let range = piece_range.start as usize..piece_range.end as usize;
            for (index, have) in live.bitfield[range].iter().by_vals().enumerate() {
                if have {
                    union.set(index, true);
                }
            }
        }
        let available_pieces = u32::try_from(union.count_ones())
            .expect("availability region contains at most 4096 pieces");
        let view = match (peers_with_known_bitfield, connected_peers) {
            (0, _) => PeerAvailabilityView::Unknown,
            (known, connected) if known == connected => PeerAvailabilityView::Observed,
            _ => PeerAvailabilityView::Partial,
        };
        Ok(PeerRegionAvailability {
            piece_range,
            available_pieces,
            missing_pieces: count - available_pieces,
            connected_peers,
            peers_with_known_bitfield,
            view,
        })
    }
}
