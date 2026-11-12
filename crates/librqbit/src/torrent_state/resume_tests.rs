// Modified for the librqbit-streaming fork.
use std::{path::PathBuf, sync::Arc, time::Duration};

use crate::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, Session, SessionOptions,
    SessionPersistenceConfig,
    bitv_factory::NonPersistentBitVFactory,
    create_torrent,
    spawn_utils::BlockingSpawner,
    storage::{StorageFactory, StorageFactoryExt, filesystem::FilesystemStorageFactory},
    torrent_state::ManagedTorrentOptions,
};

use super::*;

const PIECE_LEN: usize = 16_384;

struct Fixture {
    root: tempfile::TempDir,
    session: Arc<Session>,
    init: TorrentStateInitializing,
    torrent_bytes: bytes::Bytes,
}

fn session_options(folder: PathBuf, fastresume: bool) -> SessionOptions {
    SessionOptions {
        dht: None,
        disable_trackers: true,
        disable_local_service_discovery: true,
        persistence: Some(SessionPersistenceConfig::Json {
            folder: Some(folder),
        }),
        fastresume,
        ..Default::default()
    }
}

impl Fixture {
    async fn new(pieces: usize, streaming: bool) -> Self {
        Self::with_piece_length(pieces, streaming, PIECE_LEN).await
    }

    async fn with_piece_length(pieces: usize, streaming: bool, piece_length: usize) -> Self {
        let root = tempfile::tempdir().unwrap();
        let payload_path = root.path().join("payload.bin");
        let payload: Vec<u8> = (0..pieces)
            .flat_map(|index| {
                std::iter::repeat_n(u8::try_from(index % 251 + 1).unwrap(), piece_length)
            })
            .collect();
        std::fs::write(&payload_path, payload).unwrap();
        let spawner = BlockingSpawner::new(1);
        let torrent = create_torrent(
            &payload_path,
            CreateTorrentOptions {
                piece_length: Some(u32::try_from(piece_length).unwrap()),
                ..Default::default()
            },
            &spawner,
        )
        .await
        .unwrap();
        let torrent_bytes = torrent.as_bytes().unwrap();
        let session = Session::new_with_opts(
            root.path().to_owned(),
            session_options(root.path().join("session"), true),
        )
        .await
        .unwrap();
        let shared = Arc::new(ManagedTorrentShared {
            id: 0,
            info_hash: torrent.info_hash(),
            spawner,
            trackers: Default::default(),
            peer_id: librqbit_core::Id20::new([0; 20]),
            span: tracing::Span::none(),
            options: ManagedTorrentOptions {
                demand_only_streaming: streaming,
                allow_overwrite: true,
                output_folder: root.path().to_owned(),
                ..Default::default()
            },
            connector: session.connector.clone(),
            storage_factory: FilesystemStorageFactory::default().boxed(),
            session: Arc::downgrade(&session),
            magnet_name: None,
            client_name_and_version: "resume-test".to_owned(),
        });
        let metadata = Arc::new(
            TorrentMetadata::new(
                torrent.meta.info.data.validate().unwrap(),
                torrent_bytes.clone(),
                torrent.meta.info.raw_bytes.0,
            )
            .unwrap(),
        );
        let files = shared
            .storage_factory
            .create_and_init(&shared, &metadata)
            .unwrap();
        let init = TorrentStateInitializing::new(shared, metadata, None, files, false);
        Self {
            root,
            session,
            init,
            torrent_bytes,
        }
    }

    fn bits(&self, claims: &[usize]) -> BF {
        let mut bits = BF::from_boxed_slice(
            vec![0; self.init.metadata.lengths().piece_bitfield_bytes()].into(),
        );
        for &piece in claims {
            bits.set(piece, true);
        }
        bits
    }

    async fn save(&self, bits: BF) {
        drop(
            self.session
                .bitv_factory
                .store_initial_check(self.init.shared.info_hash.into(), bits)
                .await
                .unwrap(),
        );
    }

    fn bitv_path(&self) -> PathBuf {
        self.root
            .path()
            .join("session")
            .join(format!("{:?}.bitv", self.init.shared.info_hash))
    }
}

#[tokio::test]
async fn resume_sampling_rejects_real_checksum_mismatch_returning_ok_false() {
    let f = Fixture::new(1, false).await;
    f.init.files.pwrite_all(0, 0, &[0; PIECE_LEN]).unwrap();
    let piece = f.init.metadata.lengths().validate_piece_index(0).unwrap();
    assert!(
        !FileOps::new(
            &f.init.metadata.info,
            &f.init.files,
            &f.init.metadata.file_infos
        )
        .check_piece(piece)
        .unwrap()
    );
    let resumed = f
        .init
        .validate_fastresume(&NonPersistentBitVFactory {}, Some(f.bits(&[0]).into_dyn()))
        .await
        .unwrap();
    assert!(
        resumed.is_none(),
        "Ok(false) must invalidate sampled fastresume"
    );
}

#[tokio::test]
async fn resume_streaming_keeps_verified_claims_without_scanning_unclaimed_payload() {
    let mut f = Fixture::new(96, true).await;
    // A large sparse cache: two claimed good pieces, one corrupt claim, and one
    // complete but unclaimed piece which a full initial scan would incorrectly adopt.
    std::fs::write(f.root.path().join("payload.bin"), []).unwrap();
    f.init
        .files
        .ensure_file_length(0, (96 * PIECE_LEN) as u64)
        .unwrap();
    for index in [0, 10, 95] {
        f.init
            .files
            .pwrite_all(
                0,
                (index * PIECE_LEN) as u64,
                &vec![u8::try_from(index + 1).unwrap(); PIECE_LEN],
            )
            .unwrap();
    }
    f.save(f.bits(&[0, 80, 95])).await;
    let read_bytes = Arc::new(AtomicU64::new(0));
    f.init.files = Box::new(GatedStorage {
        inner: f.init.files.take().unwrap(),
        first_read: AtomicBool::new(false),
        read_bytes: read_bytes.clone(),
        gate: None,
    });
    let paused = f.init.check().await.unwrap();
    assert_eq!(
        paused
            .chunk_tracker
            .get_have_pieces()
            .as_slice()
            .iter_ones()
            .collect::<Vec<_>>(),
        [0, 95]
    );
    assert_eq!(
        f.init.get_checked_bytes(),
        (3 * PIECE_LEN) as u64,
        "only claimed payload should be hashed"
    );
    assert_eq!(read_bytes.load(Ordering::Relaxed), (3 * PIECE_LEN) as u64);
    drop(paused);
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if std::fs::read(f.bitv_path()).unwrap() == f.bits(&[0, 95]).as_raw_slice() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("corrected claims must survive disk bitfield flush/drop");
}

#[tokio::test]
async fn resume_streaming_clears_missing_and_truncated_payload_claims() {
    let f = Fixture::new(3, true).await;
    std::fs::write(f.root.path().join("payload.bin"), [1u8; PIECE_LEN]).unwrap();
    f.save(f.bits(&[0, 1, 2])).await;
    let paused = f.init.check().await.unwrap();
    assert_eq!(
        paused
            .chunk_tracker
            .get_have_pieces()
            .as_slice()
            .iter_ones()
            .collect::<Vec<_>>(),
        [0]
    );
}

#[tokio::test]
async fn resume_missing_payload_file_does_not_keep_saved_claims() {
    let mut f = Fixture::new(3, true).await;
    let path = f.root.path().join("payload.bin");
    std::fs::remove_file(path).unwrap();
    // Reopen like a fresh session: the filesystem backend creates a missing file.
    f.init.files = f
        .init
        .shared
        .storage_factory
        .create_and_init(&f.init.shared, &f.init.metadata)
        .unwrap();
    f.save(f.bits(&[0, 1, 2])).await;
    let paused = f.init.check().await.unwrap();
    assert_eq!(paused.chunk_tracker.get_have_pieces().as_bytes(), &[0]);
}

#[test]
fn resume_serialized_policy_is_opt_in_and_backwards_compatible() {
    let legacy = serde_json::json!({
        "info_hash": "0000000000000000000000000000000000000000",
        "trackers": [], "output_folder": "payload", "only_files": null, "is_paused": true
    });
    let st: crate::session_persistence::SerializedTorrent =
        serde_json::from_value(legacy.clone()).unwrap();
    assert!(!st.into_add_torrent().unwrap().1.demand_only_streaming);
    let mut streaming = legacy;
    streaming["demand_only_streaming"] = true.into();
    let st: crate::session_persistence::SerializedTorrent =
        serde_json::from_value(streaming).unwrap();
    assert!(st.into_add_torrent().unwrap().1.demand_only_streaming);
}

#[tokio::test]
async fn resume_masks_out_of_range_padding_bits() {
    let f = Fixture::new(3, true).await;
    f.save(BF::from_boxed_slice(vec![0xff].into())).await;
    let paused = f.init.check().await.unwrap();
    assert_eq!(paused.chunk_tracker.get_have_pieces().as_bytes(), &[0xe0]);
}

#[tokio::test]
async fn resume_rejects_malformed_bitfield_length_and_recovers_with_full_check() {
    for bytes in [vec![], vec![0xff, 0xff]] {
        let f = Fixture::new(3, true).await;
        f.save(BF::from_boxed_slice(bytes.into())).await;
        let paused = f.init.check().await.unwrap();
        assert_eq!(paused.chunk_tracker.get_have_pieces().as_bytes(), &[0xe0]);
        drop(paused);
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if std::fs::read(f.bitv_path()).unwrap() == [0xe0] {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("old invalid bitfield flusher must not replace recovered state");
    }
}

#[tokio::test]
async fn resume_paused_validation_never_returns_claimed_data() {
    let f = Fixture::new(3, true).await;
    f.save(f.bits(&[0, 1, 2])).await;
    f.init.request_pause();
    assert!(
        f.init.check().await.is_err(),
        "fastresume must respect pause before serving claims"
    );
    assert_eq!(f.init.get_checked_bytes(), 0);
    f.init.clear_pause_request();
    let paused = f.init.check().await.unwrap();
    assert_eq!(paused.chunk_tracker.get_have_pieces().as_bytes(), &[0xe0]);
}

// Forward every operation to real files, only pausing after the first real read
// so cancellation is deterministic rather than dependent on disk speed.
struct GatedStorage {
    inner: FileStorage,
    first_read: AtomicBool,
    read_bytes: Arc<AtomicU64>,
    gate: Option<ReadGate>,
}

struct ReadGate {
    started: std::sync::mpsc::SyncSender<()>,
    release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
}

impl crate::storage::TorrentStorage for GatedStorage {
    fn init(
        &mut self,
        shared: &ManagedTorrentShared,
        metadata: &TorrentMetadata,
    ) -> anyhow::Result<()> {
        self.inner.init(shared, metadata)
    }
    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        self.inner.pread_exact(file_id, offset, buf)?;
        self.read_bytes
            .fetch_add(buf.len() as u64, Ordering::Relaxed);
        if let Some(gate) = &self.gate
            && !self.first_read.swap(true, Ordering::Relaxed)
        {
            gate.started.send(())?;
            gate.release
                .lock()
                .unwrap()
                .recv_timeout(Duration::from_secs(3))?;
        }
        Ok(())
    }
    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        self.inner.pwrite_all(file_id, offset, buf)
    }
    fn remove_file(&self, file_id: usize, path: &std::path::Path) -> anyhow::Result<()> {
        self.inner.remove_file(file_id, path)
    }
    fn remove_directory_if_empty(&self, path: &std::path::Path) -> anyhow::Result<()> {
        self.inner.remove_directory_if_empty(path)
    }
    fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
        self.inner.ensure_file_length(file_id, length)
    }
    fn take(&self) -> anyhow::Result<FileStorage> {
        self.inner.take()
    }
}

#[tokio::test]
async fn resume_cancellation_stops_hashing_after_a_bounded_read() {
    let mut f = Fixture::with_piece_length(1, true, 262_144).await;
    f.save(f.bits(&[0])).await;
    let (started_tx, started_rx) = std::sync::mpsc::sync_channel(1);
    let (release_tx, release_rx) = std::sync::mpsc::sync_channel(1);
    let read_bytes = Arc::new(AtomicU64::new(0));
    f.init.files = Box::new(GatedStorage {
        inner: f.init.files.take().unwrap(),
        first_read: AtomicBool::new(false),
        read_bytes: read_bytes.clone(),
        gate: Some(ReadGate {
            started: started_tx,
            release: std::sync::Mutex::new(release_rx),
        }),
    });
    let init = Arc::new(f.init);
    let cancel = init.clone();
    let worker = std::thread::spawn(move || {
        started_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        cancel.request_pause();
        release_tx.send(()).unwrap();
    });
    let result = init.check().await;
    worker.join().unwrap();
    assert!(
        result.is_err(),
        "an interrupted check must never publish a chunk tracker"
    );
    assert_eq!(read_bytes.load(Ordering::Relaxed), 65_536);
    init.clear_pause_request();
    let paused = init.check().await.unwrap();
    assert_eq!(
        paused
            .chunk_tracker
            .get_have_pieces()
            .as_slice()
            .count_ones(),
        1
    );
}

#[tokio::test]
async fn resume_fresh_session_restores_streaming_policy_and_rechecks_corrupt_data() {
    let f = Fixture::new(3, true).await;
    let handle = f
        .session
        .add_torrent(
            AddTorrent::TorrentFileBytes(f.torrent_bytes.clone()),
            Some(AddTorrentOptions {
                overwrite: true,
                paused: true,
                demand_only_streaming: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    handle.wait_until_initialized().await.unwrap();
    let info_hash = handle.info_hash();
    f.session.stop().await;
    drop(handle);
    f.init
        .files
        .pwrite_all(0, PIECE_LEN as u64, &[0; PIECE_LEN])
        .unwrap();
    let root_path = f.root.path().to_owned();
    drop(f.session);
    let restarted = Session::new_with_opts(
        root_path.clone(),
        session_options(root_path.join("session"), true),
    )
    .await
    .unwrap();
    let restored = restarted
        .get(info_hash.into())
        .expect("saved torrent must auto-restore");
    restored.wait_until_initialized().await.unwrap();
    assert!(
        restored.shared.options.demand_only_streaming,
        "restart must preserve strict streaming validation"
    );
    let have = restored
        .with_chunk_tracker(|ct| ct.get_have_pieces().as_bytes().to_owned())
        .unwrap();
    assert_eq!(have, [0xa0]);
    restarted.stop().await;
}

#[tokio::test]
async fn resume_remains_opt_in_even_with_persisted_bitfield() {
    let f = Fixture::new(3, true).await;
    f.save(f.bits(&[0])).await;
    let restarted = Session::new_with_opts(
        f.root.path().to_owned(),
        session_options(f.root.path().join("session"), false),
    )
    .await
    .unwrap();
    let handle = restarted
        .add_torrent(
            AddTorrent::TorrentFileBytes(f.torrent_bytes.clone()),
            Some(AddTorrentOptions {
                overwrite: true,
                paused: true,
                demand_only_streaming: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    handle.wait_until_initialized().await.unwrap();
    assert_eq!(
        handle
            .with_chunk_tracker(|ct| ct.get_have_pieces().as_bytes().to_owned())
            .unwrap(),
        [0xe0]
    );
    restarted.stop().await;
}
