use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
fn owned_metainfo(private: bool) -> Vec<u8> {
    let mut bytes =
        b"d4:infod6:lengthi1e4:name9:owned.bin12:piece lengthi16384e6:pieces20:".to_vec();
    bytes.extend([0; 20]);
    if private {
        bytes.extend(b"7:privatei1e");
    }
    bytes.extend(b"ee");
    bytes
}
#[tokio::test]
async fn stop_fences_live_socket_queued_peer_and_delayed_connection() {
    let first = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let delayed = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let temp = tempfile::tempdir().unwrap();
    let session = Session::new_with_opts(
        temp.path().into(),
        SessionOptions {
            dht: None,
            disable_local_service_discovery: true,
            disable_trackers: true,
            observation_only: true,
            peer_limit: Some(1),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(owned_metainfo(false)),
            Some(AddTorrentOptions {
                only_files: Some(vec![]),
                initial_peers: Some(vec![
                    first.local_addr().unwrap(),
                    second.local_addr().unwrap(),
                ]),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    handle.wait_until_initialized().await.unwrap();
    let (mut old_peer, _) = tokio::time::timeout(Duration::from_secs(2), first.accept())
        .await
        .unwrap()
        .unwrap();
    let mut handshake = [0; 68];
    old_peer.read_exact(&mut handshake).await.unwrap();
    let live = handle.live().unwrap();
    let delayed_addr = delayed.local_addr().unwrap();
    live.spawn(tracing::Span::none(), "owned delayed retry", async move {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _socket = tokio::net::TcpStream::connect(delayed_addr)
            .await
            .map_err(|e| crate::Error::Anyhow(e.into()))?;
        Ok(())
    });
    session.stop().await;
    assert_eq!(session.tasks.len(), 0);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), old_peer.read(&mut handshake))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(180), second.accept())
            .await
            .is_err(),
        "queued old peer connected after teardown"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), delayed.accept())
            .await
            .is_err(),
        "retry from old session connected after teardown"
    );
}
#[tokio::test]
async fn unknown_magnet_private_resolution_discards_initial_peer_history() {
    let bytes = owned_metainfo(true);
    let parsed = librqbit_core::torrent_metainfo::torrent_from_bytes(&bytes).unwrap();
    let info = parsed.info.raw_bytes.as_ref().to_vec();
    let hash = parsed.info_hash.as_string();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut handshake = [0; 68];
        socket.read_exact(&mut handshake).await.unwrap();
        handshake[48..].fill(77);
        socket.write_all(&handshake).await.unwrap();
        let mut frame = vec![20, 0];
        frame
            .extend(format!("d1:md11:ut_metadatai1ee13:metadata_sizei{}ee", info.len()).as_bytes());
        socket
            .write_all(&(frame.len() as u32).to_be_bytes())
            .await
            .unwrap();
        socket.write_all(&frame).await.unwrap();
        loop {
            let size = socket.read_u32().await.unwrap();
            let mut message = vec![0; size as usize];
            socket.read_exact(&mut message).await.unwrap();
            if message.starts_with(&[20, 1]) {
                break;
            }
        }
        let mut data = vec![20, peer_binary_protocol::MY_EXTENDED_UT_METADATA];
        data.extend(format!("d8:msg_typei1e5:piecei0e10:total_sizei{}ee", info.len()).as_bytes());
        data.extend(info);
        socket
            .write_all(&(data.len() as u32).to_be_bytes())
            .await
            .unwrap();
        socket.write_all(&data).await.unwrap();
        let mut tail = [0; 1024];
        while socket.read(&mut tail).await.unwrap_or(0) > 0 {}
    });
    let temp = tempfile::tempdir().unwrap();
    let session = Session::new_with_opts(
        temp.path().into(),
        SessionOptions {
            dht: None,
            disable_local_service_discovery: true,
            disable_trackers: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        session.add_torrent(
            AddTorrent::from_url(format!("magnet:?xt=urn:btih:{hash}")),
            Some(AddTorrentOptions {
                list_only: true,
                initial_peers: Some(vec![addr]),
                ..Default::default()
            }),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let AddTorrentResponse::ListOnly(listed) = result else {
        panic!("expected metadata")
    };
    assert!(listed.info.info().private);
    assert!(
        listed.seen_peers.is_empty(),
        "unknown-privacy peers leaked into private result"
    );
    session.stop().await;
    server.await.unwrap();
}
