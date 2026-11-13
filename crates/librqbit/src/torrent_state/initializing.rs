// Modified for WebTorrent Desktop streaming phase 3.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};

use anyhow::{Context, bail};

use rand::{RngExt, seq::IteratorRandom};
use size_format::SizeFormatterBinary as SF;
use tracing::{info, trace, warn};

use crate::{
    api::TorrentIdOrHash,
    bitv::BitV,
    bitv_factory::BitVFactory,
    chunk_tracker::{ChunkTracker, compute_selected_pieces},
    file_ops::FileOps,
    type_aliases::{BF, FileStorage},
};

const MAX_FASTRESUME_CHECKS: usize = 64;

use super::{ManagedTorrentShared, TorrentMetadata, paused::TorrentStatePaused};

#[cfg(test)]
#[path = "resume_tests.rs"]
mod resume_tests;

pub struct TorrentStateInitializing {
    pub(crate) files: FileStorage,
    pub(crate) shared: Arc<ManagedTorrentShared>,
    pub(crate) metadata: Arc<TorrentMetadata>,
    pub(crate) only_files: Option<Vec<usize>>,
    pub(crate) checked_bytes: AtomicU64,
    pause_requested: AtomicBool,
    check_running: AtomicBool,
    previously_errored: bool,
}

impl TorrentStateInitializing {
    pub fn new(
        shared: Arc<ManagedTorrentShared>,
        metadata: Arc<TorrentMetadata>,
        only_files: Option<Vec<usize>>,
        files: FileStorage,
        previously_errored: bool,
    ) -> Self {
        Self {
            shared,
            metadata,
            only_files,
            files,
            checked_bytes: AtomicU64::new(0),
            pause_requested: AtomicBool::new(false),
            check_running: AtomicBool::new(false),
            previously_errored,
        }
    }

    pub fn get_checked_bytes(&self) -> u64 {
        self.checked_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn request_pause(&self) {
        self.pause_requested.store(true, Ordering::Relaxed);
    }

    pub(crate) fn clear_pause_request(&self) {
        self.pause_requested.store(false, Ordering::Relaxed);
    }

    pub(crate) fn is_pause_requested(&self) -> bool {
        self.pause_requested.load(Ordering::Relaxed)
    }

    pub(crate) fn try_start_check(&self) -> bool {
        self.check_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn finish_check(&self) {
        self.check_running.store(false, Ordering::Release);
    }

    // A rejected disk-backed value may still flush on Drop. Clear its snapshot
    // before dropping it, then remove the persisted value before a full check.
    async fn discard_fastresume(&self, bitv_factory: &dyn BitVFactory, mut hp: Box<dyn BitV>) {
        hp.as_slice_mut().fill(false);
        drop(hp);
        if let Err(e) = bitv_factory.clear(self.shared.info_hash.into()).await {
            warn!(id=?self.shared.id, info_hash = ?self.shared.info_hash, "error clearing bitfield: {e:#}");
        }
        self.checked_bytes.store(0, Ordering::Relaxed);
    }

    async fn validate_fastresume(
        &self,
        bitv_factory: &dyn BitVFactory,
        have_pieces: Option<Box<dyn BitV>>,
    ) -> anyhow::Result<Option<Box<dyn BitV>>> {
        if self.is_pause_requested() {
            bail!("initial check paused");
        }
        let Some(mut hp) = have_pieces else {
            return Ok(None);
        };
        let actual = hp.as_bytes().len();
        let lengths = self.metadata.lengths();
        let expected = lengths.piece_bitfield_bytes();
        if actual != expected || hp.as_slice().len() != expected * 8 {
            warn!(
                actual,
                expected,
                "the bitfield loaded isn't of correct length, ignoring it, will do full check"
            );
            self.discard_fastresume(bitv_factory, hp).await;
            return Ok(None);
        }

        let padding = &mut hp.as_slice_mut()[lengths.total_pieces() as usize..];
        let mut changed = padding.any();
        padding.fill(false);
        let strict = self.shared.options.demand_only_streaming;
        self.checked_bytes.store(0, Ordering::Relaxed);

        let is_broken = self
            .shared
            .spawner
            .block_in_place_with_semaphore(|| -> anyhow::Result<bool> {
                if self.is_pause_requested() {
                    bail!("initial check paused");
                }
                let fo = FileOps::new(&self.metadata.info, &self.files, &self.metadata.file_infos);

                let mut to_validate = if strict {
                    // Persisted claims are hints, not proof. Hash every claimed
                    // piece, but avoid reading unclaimed holes in a sparse cache.
                    hp.as_slice().to_owned()
                } else {
                    let mut selected = BF::from_boxed_slice(vec![0u8; expected].into());
                    let mut queue = hp.as_slice().to_owned();

                    // Preserve the legacy sampling policy outside streaming opt-in.
                    for fi in self.metadata.file_infos.iter() {
                        let offset = fi.piece_range_usize().start;
                        for piece_id in hp
                            .as_slice()
                            .get(fi.piece_range_usize())
                            .into_iter()
                            .flat_map(|s| s.iter_ones())
                            .map(|pid| pid + offset)
                            .take(1)
                        {
                            selected.set(piece_id, true);
                            queue.set(piece_id, false);
                        }
                    }
                    let queue = queue
                        .iter_ones()
                        .sample(&mut rand::rng(), MAX_FASTRESUME_CHECKS);
                    for (tmp_id, piece_id) in queue.into_iter().enumerate() {
                        let denom: u32 = (tmp_id + 1).min(50).try_into().unwrap();
                        if rand::rng().random_ratio(1, denom) {
                            selected.set(piece_id, true);
                        }
                    }
                    selected.into_bitvec()
                };
                // Keep iteration bounded even for a custom BitV implementation.
                to_validate.truncate(lengths.total_pieces() as usize);
                let to_validate_count = to_validate.count_ones();
                let mut invalid_pieces = 0usize;
                let mut checked_bytes = 0u64;
                for (id, piece_id) in to_validate.iter_ones().enumerate() {
                    if self.is_pause_requested() {
                        bail!("initial check paused");
                    }
                    let piece = lengths
                        .validate_piece_index(piece_id.try_into()?)
                        .context("invalid fastresume piece index")?;
                    // A checksum mismatch is Ok(false), not an I/O error.
                    let valid = fo.check_piece_with_cancel(piece, &self.pause_requested);
                    if self.is_pause_requested() {
                        bail!("initial check paused");
                    }
                    if !matches!(valid, Ok(true)) {
                        if !strict {
                            return Ok(true);
                        }
                        hp.as_slice_mut().set(piece_id, false);
                        changed = true;
                        invalid_pieces += 1;
                    }
                    checked_bytes += u64::from(lengths.piece_length(piece));
                    let progress = if strict {
                        checked_bytes
                    } else {
                        #[allow(clippy::cast_possible_truncation)]
                        let progress = (lengths.total_length() as f64 / to_validate_count as f64
                            * (id + 1) as f64) as u64;
                        progress.min(lengths.total_length())
                    };
                    self.checked_bytes.store(progress, Ordering::Relaxed);
                }
                if self.is_pause_requested() {
                    bail!("initial check paused");
                }
                info!(
                    torrent = ?self.shared.id,
                    strict,
                    checked_pieces = to_validate_count,
                    invalid_pieces,
                    checked_bytes,
                    "validated persisted piece claims"
                );
                Ok(false)
            })
            .await?;

        if is_broken {
            warn!(
                id = ?self.shared.id,
                info_hash = ?self.shared.info_hash,
                "data corrupted, ignoring fastresume data"
            );
            self.discard_fastresume(bitv_factory, hp).await;
            return Ok(None);
        }

        if changed {
            // Keep this same value and its flusher alive in the chunk tracker.
            // Replacing it could let an old Drop snapshot restore invalid claims.
            if let Err(e) = hp.flush(false) {
                warn!(torrent = ?self.shared.id, "error flushing corrected resume bitfield: {e:#}");
            }
        }
        Ok(Some(hp))
    }

    pub async fn check(&self) -> anyhow::Result<TorrentStatePaused> {
        let id: TorrentIdOrHash = self.shared.info_hash.into();
        let bitv_factory = self
            .shared
            .session
            .upgrade()
            .context("session is dead")?
            .bitv_factory
            .clone();
        let have_pieces = if self.previously_errored {
            if let Err(e) = bitv_factory.clear(id).await {
                warn!(id=?self.shared.id, info_hash = ?self.shared.info_hash, error=?e, "error clearing bitfield");
            }
            None
        } else {
            bitv_factory
                .load(id)
                .await
                .context("error loading have_pieces")?
        };

        let have_pieces = if self.shared.options.verify_selected_pieces {
            if let Some(previous) = have_pieces {
                self.discard_fastresume(&*bitv_factory, previous).await;
            }
            let selected = compute_selected_pieces(
                self.metadata.lengths(),
                |index| {
                    self.only_files
                        .as_ref()
                        .is_none_or(|files| files.contains(&index))
                },
                &self.metadata.file_infos,
            );
            let bits = self
                .shared
                .spawner
                .block_in_place_with_semaphore(|| -> anyhow::Result<BF> {
                    let lengths = self.metadata.lengths();
                    let mut bits =
                        BF::from_boxed_slice(vec![0u8; lengths.piece_bitfield_bytes()].into());
                    let ops =
                        FileOps::new(&self.metadata.info, &self.files, &self.metadata.file_infos);
                    for index in selected.iter_ones() {
                        if self.is_pause_requested() {
                            bail!("initial check paused");
                        }
                        let piece = lengths
                            .validate_piece_index(index.try_into()?)
                            .context("invalid selected piece")?;
                        if matches!(
                            ops.check_piece_with_cancel(piece, &self.pause_requested),
                            Ok(true)
                        ) {
                            bits.set(index, true);
                        }
                        self.checked_bytes
                            .fetch_add(lengths.piece_length(piece) as u64, Ordering::Relaxed);
                    }
                    if self.is_pause_requested() {
                        bail!("initial check paused");
                    }
                    Ok(bits)
                })
                .await?;
            Some(bitv_factory.store_initial_check(id, bits).await?)
        } else {
            self.validate_fastresume(&*bitv_factory, have_pieces)
                .await?
        };

        let have_pieces = match have_pieces {
            Some(h) => h,
            None => {
                info!("Doing initial checksum validation, this might take a while...");
                let have_pieces = self
                    .shared
                    .spawner
                    .block_in_place_with_semaphore(|| {
                        FileOps::new(&self.metadata.info, &self.files, &self.metadata.file_infos)
                            .initial_check(&self.checked_bytes, &self.pause_requested)
                    })
                    .await?;
                bitv_factory
                    .store_initial_check(id, have_pieces)
                    .await
                    .context("error storing initial check bitfield")?
            }
        };

        let selected_pieces = compute_selected_pieces(
            self.metadata.lengths(),
            |idx| {
                self.only_files
                    .as_ref()
                    .map(|o| o.contains(&idx))
                    .unwrap_or(true)
            },
            &self.metadata.file_infos,
        );

        let chunk_tracker = ChunkTracker::new(
            have_pieces.into_dyn(),
            selected_pieces,
            *self.metadata.lengths(),
            &self.metadata.file_infos,
        )
        .context("error creating chunk tracker")?;

        let hns = chunk_tracker.get_hns();

        info!(
            torrent=?self.shared.id,
            "Initial check results: have {}, needed {}, total selected {}",
            SF::new(hns.have_bytes),
            SF::new(hns.needed_bytes),
            SF::new(hns.selected_bytes)
        );

        // Ensure file lengths are correct, and reopen read-only.
        self.shared
            .spawner
            .block_in_place_with_semaphore(|| {
                for (idx, fi) in self.metadata.file_infos.iter().enumerate() {
                    if self
                        .only_files
                        .as_ref()
                        .map(|v| v.contains(&idx))
                        .unwrap_or(true)
                    {
                        let now = Instant::now();
                        if fi.attrs.padding {
                            continue;
                        }
                        if let Err(err) = self.files.ensure_file_length(idx, fi.len) {
                            warn!(
                                id=?self.shared.id, info_hash = ?self.shared.info_hash,
                                "Error setting length for file {:?} to {}: {:#?}",
                                fi.relative_filename, fi.len, err
                            );
                        } else {
                            trace!(
                                "Set length for file {:?} to {} in {:?}",
                                fi.relative_filename,
                                SF::new(fi.len),
                                now.elapsed()
                            );
                        }
                    }
                }
                Ok::<_, anyhow::Error>(())
            })
            .await?;

        let paused = TorrentStatePaused {
            shared: self.shared.clone(),
            metadata: self.metadata.clone(),
            files: self.files.take()?,
            chunk_tracker,
            streams: Arc::new(super::streaming::TorrentStreams::new(
                self.shared.options.demand_only_streaming,
            )),
        };
        Ok(paused)
    }
}
