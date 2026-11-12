// Modified for the librqbit-streaming fork.
//! Public API and real TCP streaming contracts. No player or public swarm needed.
use std::{io::SeekFrom, net::Ipv4Addr, sync::Arc, time::Duration};

use anyhow::{Context, ensure};
use librqbit::{
    AddTorrent, AddTorrentOptions, CreateTorrentOptions, ListenerMode, ListenerOptions,
    PlaybackHint, Session, SessionOptions, StreamPriority, StreamingControlError,
    StreamingReadOptions, create_torrent, limits::LimitsConfig, spawn_utils::BlockingSpawner,
};
use tokio::{
    io::{AsyncReadExt, AsyncSeekExt},
    time::timeout,
};

const MIB: usize = 1024 * 1024;

fn offline_options() -> SessionOptions {
    SessionOptions {
        dht: None,
        disable_trackers: true,
        disable_local_service_discovery: true,
        ..Default::default()
    }
}

async fn seed(
    directory: &std::path::Path,
    torrent: bytes::Bytes,
    upload_bps: u32,
) -> anyhow::Result<Arc<Session>> {
    let session = Session::new_with_opts(
        directory.to_path_buf(),
        SessionOptions {
            listen: Some(ListenerOptions {
                listen_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                mode: ListenerMode::TcpOnly,
                ..Default::default()
            }),
            ratelimits: LimitsConfig {
                upload_bps: std::num::NonZeroU32::new(upload_bps),
                ..Default::default()
            },
            ..offline_options()
        },
    )
    .await?;
    session
        .add_torrent(
            AddTorrent::from_bytes(torrent),
            Some(AddTorrentOptions {
                overwrite: true,
                ..Default::default()
            }),
        )
        .await?
        .into_handle()
        .context("seed handle")?
        .wait_until_completed()
        .await?;
    Ok(session)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn streaming_contract_multi_peer_seek_and_remaining_peer() -> anyhow::Result<()> {
    timeout(Duration::from_secs(40), async {
        let source = tempfile::tempdir()?;
        let cache = tempfile::tempdir()?;
        let contents: Vec<u8> = (0..8 * MIB)
            .map(|i| ((i ^ (i >> 10)) % 251) as u8)
            .collect();
        let media = source.path().join("media.bin");
        std::fs::write(&media, &contents)?;
        let created = create_torrent(
            &media,
            CreateTorrentOptions {
                piece_length: Some(64 * 1024),
                ..Default::default()
            },
            &BlockingSpawner::new(1),
        )
        .await?;
        let torrent: bytes::Bytes = created.as_bytes()?;
        let slow = seed(source.path(), torrent.clone(), 128 * 1024).await?;
        let fast = seed(source.path(), torrent.clone(), 1024 * 1024).await?;
        let client = Session::new_with_opts(cache.path().into(), offline_options()).await?;
        let handle = client
            .add_torrent(
                AddTorrent::from_bytes(torrent),
                Some(AddTorrentOptions {
                    demand_only_streaming: true,
                    initial_peers: Some(vec![
                        slow.listen_addr().context("slow listener")?,
                        fast.listen_addr().context("fast listener")?,
                    ]),
                    peer_opts: Some(librqbit::PeerConnectionOptions {
                        max_request_window: Some(32),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            )
            .await?
            .into_handle()
            .context("client handle")?;
        handle.wait_until_initialized().await?;
        ensure!(
            handle.stats().progress_bytes == 0,
            "no demand must not fetch payload"
        );

        let prefetch = handle
            .clone()
            .stream_range_with_options(
                0,
                (4 * MIB) as u64,
                (5 * MIB) as u64,
                StreamingReadOptions {
                    priority: StreamPriority::Prefetch,
                    deadline: None,
                },
            )
            .await?;
        let mut playback = handle
            .clone()
            .stream_range_with_options(
                0,
                0,
                contents.len() as u64,
                StreamingReadOptions {
                    priority: StreamPriority::Playback,
                    deadline: Some(Duration::ZERO),
                },
            )
            .await?;
        let hint = PlaybackHint {
            consumption_bytes_per_second: 128 * 1024,
            buffered: Duration::ZERO,
            target_buffer: Duration::from_secs(4),
        };
        playback.update_playback(playback.generation(), hint)?;
        let mut bytes = vec![0; 128 * 1024];
        playback.read_exact(&mut bytes).await?;
        assert_eq!(bytes, contents[..bytes.len()]);
        ensure!(
            !handle.stats().finished,
            "streaming must not require the whole file"
        );

        timeout(Duration::from_secs(5), async {
            loop {
                let available = handle.streaming_availability(0, 0, MIB as u64)?;
                if available.connected_peers >= 2 && available.missing_pieces == 0 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;

        let old_generation = playback.generation();
        playback.seek(SeekFrom::Start((6 * MIB) as u64)).await?;
        ensure!(playback.generation() > old_generation);
        assert!(matches!(
            playback.update_playback(old_generation, hint),
            Err(StreamingControlError::StaleGeneration { .. })
        ));
        playback.update_playback(playback.generation(), hint)?;
        let generation = playback.generation();
        assert!(
            playback
                .seek(SeekFrom::Start(contents.len() as u64 + 1))
                .await
                .is_err()
        );
        assert_eq!(
            playback.generation(),
            generation,
            "failed seeks preserve generation"
        );
        playback.read_exact(&mut bytes).await?;
        assert_eq!(bytes, contents[6 * MIB..6 * MIB + bytes.len()]);
        drop(prefetch);
        drop(playback);
        assert_eq!(
            handle.streaming_policy().context("policy")?.active_readers,
            0
        );

        fast.stop().await;
        // The surviving slower peer must still deliver a new range after a peer exits.
        let start = 7 * MIB + 768 * 1024;
        let mut tail = handle
            .clone()
            .stream_range_with_options(
                0,
                start as u64,
                contents.len() as u64,
                StreamingReadOptions {
                    priority: StreamPriority::Playback,
                    deadline: Some(Duration::ZERO),
                },
            )
            .await?;
        let mut rest = Vec::new();
        tail.read_to_end(&mut rest).await?;
        assert_eq!(rest, contents[start..]);
        drop(tail);
        tokio::join!(client.stop(), slow.stop());
        Ok::<_, anyhow::Error>(())
    })
    .await
    .context("streaming contract timed out")?
}
