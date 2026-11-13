use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn incoming_fixture(switch: bool, resume: bool, fresh: bool) -> bool {
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
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
        .await
        .unwrap();
    let (socket, mut addr) = listener.accept().await.unwrap();
    let mut accepted_at = std::time::Instant::now();
    let (mut read, mut write) = socket.into_split();
    // This socket predates initialization, which must remain admissible initially.
    let handle = session
        .add_torrent(AddTorrent::from_bytes(private_metainfo(&[])), None)
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    handle.wait_until_initialized().await.unwrap();
    if fresh {
        if switch {
            handle.live().unwrap().switch_private_tracker().await;
        }
        if resume {
            session.pause(&handle).await.unwrap();
            session.unpause(&handle).await.unwrap();
        }
        drop(read);
        drop(write);
        drop(client);
        client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (socket, new_addr) = listener.accept().await.unwrap();
        addr = new_addr;
        accepted_at = std::time::Instant::now();
        (read, write) = socket.into_split();
    }
    let pending = session.clone().check_incoming_connection(
        addr,
        accepted_at,
        ConnectionKind::Tcp,
        Box::new(read),
        Box::new(write),
    );
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(Duration::from_millis(20), &mut pending)
            .await
            .is_err()
    );
    if switch && !fresh {
        handle.live().unwrap().switch_private_tracker().await;
    }
    if resume && !fresh {
        session.pause(&handle).await.unwrap();
        session.unpause(&handle).await.unwrap();
    }
    let mut handshake = vec![19];
    handshake.extend(b"BitTorrent protocol");
    handshake.extend([0; 8]);
    handshake.extend(handle.info_hash().0);
    handshake.extend([7; 20]);
    client.write_all(&handshake).await.unwrap();
    let (live, checked) = tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .unwrap()
        .unwrap();
    let admitted = live.add_incoming_peer(checked).unwrap();
    session.stop().await;
    matches!(
        admitted,
        crate::torrent_state::live::AddIncomingPeerResult::Added
    )
}

#[tokio::test]
async fn continuous_private_inbound_rejects_handshake_from_previous_tracker() {
    assert!(
        !incoming_fixture(true, false, false).await,
        "old private handshake crossed tracker generations"
    );
}
#[tokio::test]
async fn continuous_private_inbound_keeps_cutoff_across_pause_resume() {
    assert!(
        !incoming_fixture(true, true, false).await,
        "pause/resume erased private handshake cutoff"
    );
}
#[tokio::test]
async fn continuous_private_inbound_keeps_initialization_compatibility() {
    assert!(
        incoming_fixture(false, false, false).await,
        "initial generation rejected legitimate pre-initialization socket"
    );
}

#[tokio::test]
async fn continuous_private_inbound_new_connection_after_switch_and_resume_is_admitted() {
    assert!(
        incoming_fixture(true, true, true).await,
        "new inbound connection was blanket-blocked after tracker switch"
    );
}

fn private_metainfo(urls: &[String]) -> Vec<u8> {
    let mut bytes = b"d13:announce-listl".to_vec();
    for url in urls {
        bytes.extend(format!("l{}:{}e", url.len(), url).bytes());
    }
    bytes.extend(b"e4:infod6:lengthi1e4:name9:owned.bin12:piece lengthi16384e6:pieces20:");
    bytes.extend([0; 20]);
    bytes.extend(b"7:privatei1eee");
    bytes
}

fn response_peers(peers: &[SocketAddr], interval: u32) -> Vec<u8> {
    let mut body = format!("d8:intervali{interval}e5:peers{}:", peers.len() * 6).into_bytes();
    for peer in peers {
        let SocketAddr::V4(peer) = peer else {
            panic!("fixture uses IPv4")
        };
        body.extend(peer.ip().octets());
        body.extend(peer.port().to_be_bytes());
    }
    body.push(b'e');
    body
}

async fn reply(socket: &mut tokio::net::TcpStream, body: &[u8]) {
    socket
        .write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    socket.write_all(body).await.unwrap();
}

#[tokio::test]
async fn continuous_private_failover_drains_socket_and_discards_queued_old_peer() {
    let first_tracker = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second_tracker = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let old_peer = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let queued_peer = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let new_peer = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let urls = [&first_tracker, &second_tracker]
        .map(|l| format!("http://{}/announce", l.local_addr().unwrap()));
    let first_body = response_peers(
        &[
            old_peer.local_addr().unwrap(),
            queued_peer.local_addr().unwrap(),
        ],
        5,
    );
    let next_body = response_peers(&[new_peer.local_addr().unwrap()], 60);
    let (connected_tx, connected_rx) = tokio::sync::oneshot::channel();
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
    let old_task = tokio::spawn(async move {
        let (mut socket, _) = old_peer.accept().await.unwrap();
        let mut handshake = [0; 68];
        socket.read_exact(&mut handshake).await.unwrap();
        connected_tx.send(()).unwrap();
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(10), socket.read(&mut handshake))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        closed_tx.send(()).unwrap();
    });
    let first_task = tokio::spawn(async move {
        let (mut socket, _) = first_tracker.accept().await.unwrap();
        let mut request = [0; 4096];
        socket.read(&mut request).await.unwrap();
        reply(&mut socket, &first_body).await;
        drop(socket);
        let (mut socket, _) = first_tracker.accept().await.unwrap();
        socket.read(&mut request).await.unwrap();
        socket.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
    });
    let second_task = tokio::spawn(async move {
        let (mut socket, _) = second_tracker.accept().await.unwrap();
        let mut request = [0; 4096];
        socket.read(&mut request).await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), closed_rx)
            .await
            .expect("old socket was not drained before new peers")
            .unwrap();
        reply(&mut socket, &next_body).await;
    });
    let temp = tempfile::tempdir().unwrap();
    let session = Session::new_with_opts(
        temp.path().into(),
        SessionOptions {
            dht: None,
            disable_local_service_discovery: true,
            observation_only: true,
            peer_limit: Some(1),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let handle = session
        .add_torrent(AddTorrent::from_bytes(private_metainfo(&urls)), None)
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    handle.wait_until_initialized().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), connected_rx)
        .await
        .unwrap()
        .unwrap();
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(8), new_peer.accept())
        .await
        .expect("new private tracker peer never connected")
        .unwrap();
    let mut handshake = [0; 68];
    socket.read_exact(&mut handshake).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(150), queued_peer.accept())
            .await
            .is_err(),
        "old queued peer crossed tracker generations"
    );
    session.stop().await;
    assert_eq!(session.tasks.len(), 0);
    old_task.await.unwrap();
    first_task.await.unwrap();
    second_task.await.unwrap();
}

#[tokio::test]
async fn private_tracker_failure_advances_to_next_tier() {
    let first = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let second = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let urls: Vec<url::Url> = [&first, &second]
        .map(|l| {
            format!("http://{}/announce", l.local_addr().unwrap())
                .parse()
                .unwrap()
        })
        .to_vec();
    let first_task = tokio::spawn(async move {
        let (mut s, _) = first.accept().await.unwrap();
        let mut b = [0; 2048];
        s.read(&mut b).await.unwrap();
        s.write_all(
            b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
    });
    let second_task = tokio::spawn(async move {
        let (mut s, _) = second.accept().await.unwrap();
        let mut b = [0; 2048];
        s.read(&mut b).await.unwrap();
        let body = b"d8:intervali60e5:peers6:\x7f\x00\x00\x01\x1a\xe1e";
        s.write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
        s.write_all(body).await.unwrap();
    });
    let temp = tempfile::tempdir().unwrap();
    let session = Session::new_with_opts(
        temp.path().into(),
        SessionOptions {
            dht: None,
            disable_local_service_discovery: true,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let tiers = urls.iter().cloned().map(|u| vec![u]).collect();
    let mut peers = session
        .make_peer_rx(
            Id20::new([3; 20]),
            urls,
            true,
            None,
            vec![],
            true,
            Some(tiers),
        )
        .unwrap();
    let peer = tokio::time::timeout(Duration::from_secs(2), peers.next()).await;
    drop(peers);
    session.stop().await;
    first_task.abort();
    second_task.abort();
    assert_eq!(
        peer.expect("failed private tracker must fail over"),
        Some("127.0.0.1:6881".parse().unwrap())
    );
}
