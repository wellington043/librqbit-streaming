use super::{ManagedTorrent, ManagedTorrentState};
use serde::Serialize;

#[derive(Debug, thiserror::Error)]
pub enum CacheReclaimError {
    #[error("cache reclamation requires a paused demand-only torrent")]
    NotPausedCache,
    #[error("invalid or oversized piece reclamation batch")]
    InvalidBatch,
    #[error("cache checkpoint or storage reclamation failed: {0:#}")]
    Storage(#[from] anyhow::Error),
}

#[derive(Default, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheReclaimReport {
    pub last_considered_piece: Option<u32>,
    pub invalidated_pieces: u64,
    pub invalidated_bytes: u64,
    pub retained_pieces: u64,
}

impl ManagedTorrent {
    /// Clear verified state before physically reclaiming cache ranges. Caller must pause first,
    /// and publish_checkpoint MUST durably replace every checkpoint used across restarts.
    /// Its error aborts before any state or payload change. On storage failure bits stay cleared.
    /// Readers and seeks are held behind the same barrier while their windows are pinned.
    pub fn reclaim_cached_pieces(
        &self,
        candidates: &[u32],
        retain_behind_bytes: u64,
        publish_checkpoint: impl FnOnce(&[u8]) -> anyhow::Result<()>,
    ) -> Result<CacheReclaimReport, CacheReclaimError> {
        if candidates.len() > 4096 || retain_behind_bytes > 1024 * 1024 * 1024 {
            return Err(CacheReclaimError::InvalidBatch);
        }
        let mut g = self.locked.write();
        if !self.shared.options.demand_only_streaming {
            return Err(CacheReclaimError::NotPausedCache);
        }
        let ManagedTorrentState::Paused(p) = &mut g.state else {
            return Err(CacheReclaimError::NotPausedCache);
        };
        let lengths = *p.metadata.lengths();
        let mut pieces = candidates
            .iter()
            .map(|id| {
                lengths
                    .validate_piece_index(*id)
                    .ok_or(CacheReclaimError::InvalidBatch)
            })
            .collect::<Result<Vec<_>, _>>()?;
        pieces.sort_by_key(|piece| piece.get());
        pieces.dedup();
        let streams = p.streams.clone();
        streams.with_retained_pieces(&lengths, retain_behind_bytes, |retained| {
            let mut report = CacheReclaimReport::default();
            pieces.retain(|piece| {
                if retained.contains(&piece.get()) {
                    report.retained_pieces += 1;
                    return false;
                }
                true
            });
            pieces.truncate((64 * 1024 * 1024 / lengths.default_piece_length() as usize).max(1));
            report.last_considered_piece = pieces.last().map(|piece| piece.get());
            if pieces.is_empty() {
                return Ok(report);
            }
            let mut bits = p.chunk_tracker.get_have_pieces().as_bytes().to_vec();
            for piece in &pieces {
                bits[piece.get() as usize / 8] &= !(0x80 >> (piece.get() % 8));
            }
            publish_checkpoint(&bits)?;
            for piece in &pieces {
                if p.chunk_tracker
                    .invalidate_verified_piece(*piece, &p.metadata.file_infos)
                {
                    report.invalidated_pieces += 1;
                    report.invalidated_bytes += lengths.piece_length(*piece) as u64;
                }
            }
            p.chunk_tracker.get_have_pieces_mut().flush(false)?;
            let mut spans: Vec<(u64, u64)> = Vec::new();
            for piece in pieces {
                let start = piece.get() as u64 * lengths.default_piece_length() as u64;
                let end = start + lengths.piece_length(piece) as u64;
                if let Some(last) = spans.last_mut().filter(|last| last.1 == start) {
                    last.1 = end;
                } else {
                    spans.push((start, end));
                }
            }
            for (start, end) in spans {
                for (file_id, file) in p.metadata.file_infos.iter().enumerate() {
                    let from = start.max(file.offset_in_torrent);
                    let to = end.min(file.offset_in_torrent + file.len);
                    if from < to && !file.attrs.padding {
                        p.files
                            .discard_range(file_id, from - file.offset_in_torrent, to - from)?;
                    }
                }
            }
            Ok(report)
        })
    }
}
