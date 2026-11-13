use std::collections::HashSet;
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::net::SocketAddrV6;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use anyhow::bail;
use backon::ExponentialBuilder;
use backon::Retryable;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::Either;
use futures::stream::BoxStream;
use futures::stream::FuturesUnordered;
use tracing::Instrument;
use tracing::debug;
use tracing::debug_span;
use tracing::trace;
use tracing::trace_span;
use url::Url;

use crate::tracker_comms_http;
use crate::tracker_comms_udp;
use crate::tracker_comms_udp::UdpTrackerClient;
use librqbit_core::hash_id::Id20;

/// One bounded tracker announce outcome; endpoint never contains credentials, path or query.
#[derive(Clone, Debug)]
pub struct TrackerObservation {
    pub info_hash: Id20,
    pub endpoint: String,
    pub succeeded: bool,
    pub latency: Duration,
    pub peers_returned: Option<usize>,
}
pub type TrackerObserver = Arc<dyn Fn(TrackerObservation) + Send + Sync>;
fn sanitized_tracker_endpoint(url: &Url) -> String {
    let host = match url.host() {
        Some(url::Host::Domain(v)) => v.to_owned(),
        Some(url::Host::Ipv4(v)) => v.to_string(),
        Some(url::Host::Ipv6(v)) => format!("[{v}]"),
        None => return String::new(),
    };
    match url.port() {
        Some(port) => format!("{}://{}:{}", url.scheme(), host, port),
        None => format!("{}://{}", url.scheme(), host),
    }
}
/// A single tracker-only catalog attempt. The URL is used intact on the wire.
/// Callers must drain the previous session before using another tracker.
#[derive(Clone)]
pub struct TrackerAttempt {
    pub url: Url,
    pub timeout: Duration,
}
impl std::fmt::Debug for TrackerAttempt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TrackerAttempt")
            .field("endpoint", &sanitized_tracker_endpoint(&self.url))
            .field("timeout", &self.timeout)
            .finish()
    }
}

/// BEP 12 candidate order for one bounded catalog pass. Tiers stay ordered;
/// only members within a tier are shuffled. Original metainfo is never edited.
pub struct TrackerDiscoveryPlan {
    candidates: std::vec::IntoIter<Url>,
}
impl TrackerDiscoveryPlan {
    pub fn new(mut tiers: Vec<Vec<Url>>) -> Self {
        use rand::seq::SliceRandom;
        let mut seen = HashSet::new();
        for tier in &mut tiers {
            tier.retain(|url| seen.insert(url.clone()));
            tier.shuffle(&mut rand::rng());
        }
        Self {
            candidates: tiers.into_iter().flatten().collect::<Vec<_>>().into_iter(),
        }
    }
}
impl Iterator for TrackerDiscoveryPlan {
    type Item = Url;
    fn next(&mut self) -> Option<Url> {
        self.candidates.next()
    }
}

const MAX_TRACKER_RESPONSE_BYTES: usize = 2 * 1024 * 1024;

async fn read_tracker_response_bounded(mut response: reqwest::Response) -> anyhow::Result<Vec<u8>> {
    if response
        .content_length()
        .is_some_and(|n| n > MAX_TRACKER_RESPONSE_BYTES as u64)
    {
        bail!("tracker response exceeds limit");
    }
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if bytes.len().saturating_add(chunk.len()) > MAX_TRACKER_RESPONSE_BYTES {
            bail!("tracker response exceeds limit");
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

pub struct TrackerComms {
    info_hash: Id20,
    peer_id: Id20,
    stats: Box<dyn TorrentStatsProvider>,
    force_tracker_interval: Option<Duration>,
    tx: Sender,
    // This MUST be set as trackers don't work with 0 port.
    announce_port: u16,
    reqwest_client: reqwest::Client,
    key: u32,
    observer: Option<TrackerObserver>,
}

#[derive(Default)]
pub enum TrackerCommsStatsState {
    #[default]
    None,
    Initializing,
    Paused,
    Live,
}

#[derive(Default)]
pub struct TrackerCommsStats {
    pub uploaded_bytes: u64,
    pub downloaded_bytes: u64,
    pub total_bytes: u64,
    pub torrent_state: TrackerCommsStatsState,
}

impl TrackerCommsStats {
    pub fn get_left_to_download_bytes(&self) -> u64 {
        let total = self.total_bytes;
        let down = self.downloaded_bytes;
        if total >= down {
            return total - down;
        }
        0
    }

    pub fn is_completed(&self) -> bool {
        self.downloaded_bytes >= self.total_bytes
    }
}

pub trait TorrentStatsProvider: Send + Sync {
    fn get(&self) -> TrackerCommsStats;
}

impl TorrentStatsProvider for () {
    fn get(&self) -> TrackerCommsStats {
        Default::default()
    }
}

type Sender = tokio::sync::mpsc::Sender<SocketAddr>;

enum SupportedTracker {
    Udp(Url),
    Http(Url),
}

impl std::fmt::Debug for SupportedTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SupportedTracker::Udp(u) => f.write_str(&sanitized_tracker_endpoint(u)),
            SupportedTracker::Http(u) => f.write_str(&sanitized_tracker_endpoint(u)),
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum UdpTrackerResolveResult {
    One(SocketAddr),
    Two(SocketAddrV4, SocketAddrV6),
}

async fn udp_tracker_to_socket_addrs(
    host: url::Host<&str>,
    port: u16,
) -> anyhow::Result<UdpTrackerResolveResult> {
    let res = match host {
        url::Host::Domain(name) => {
            // Use the first IPv4 and the first IPv6 addresses only.

            let mut v4: Option<SocketAddrV4> = None;
            let mut v6: Option<SocketAddrV6> = None;
            for addr in tokio::net::lookup_host((name, port))
                .await
                .with_context(|| format!("error looking up hostname {name}"))?
            {
                match (v4, v6, addr) {
                    (None, _, SocketAddr::V4(addr)) => v4 = Some(addr),
                    (_, None, SocketAddr::V6(addr)) => v6 = Some(addr),
                    _ => continue,
                }
            }
            let res = match (v4, v6) {
                (Some(v4), Some(v6)) => UdpTrackerResolveResult::Two(v4, v6),
                (Some(v4), None) => UdpTrackerResolveResult::One(v4.into()),
                (None, Some(v6)) => UdpTrackerResolveResult::One(v6.into()),
                _ => anyhow::bail!("zero addresses returned looking up {name}"),
            };
            trace!(?res, "resolved");
            res
        }
        url::Host::Ipv4(addr) => UdpTrackerResolveResult::One((addr, port).into()),
        url::Host::Ipv6(addr) => UdpTrackerResolveResult::One((addr, port).into()),
    };
    Ok(res)
}

impl TrackerComms {
    /// Performs exactly one bounded announce, including DNS and response body.
    /// Dropping this stream cancels its request; it never spawns a retry task.
    #[allow(clippy::too_many_arguments)]
    pub fn start_attempt(
        info_hash: Id20,
        peer_id: Id20,
        attempt: TrackerAttempt,
        stats: Box<dyn TorrentStatsProvider>,
        announce_port: u16,
        reqwest_client: reqwest::Client,
        udp_client: UdpTrackerClient,
        observer: Option<TrackerObserver>,
    ) -> BoxStream<'static, SocketAddr> {
        async_stream::stream! {
            let (tx, _rx) = tokio::sync::mpsc::channel(1);
            let comms = Self { info_hash, peer_id, stats, force_tracker_interval: None,
                tx, announce_port, reqwest_client, key: rand::random(), observer };
            let started = Instant::now();
            let response = tokio::time::timeout(attempt.timeout, async {
                match attempt.url.scheme() {
                    "http" | "https" => comms.fetch_http(&attempt.url,
                        Some(tracker_comms_http::TrackerRequestEvent::Started)).await,
                    "udp" => {
                        let host = attempt.url.host().context("missing tracker host")?;
                        let port = attempt.url.port().context("missing tracker port")?;
                        match udp_tracker_to_socket_addrs(host, port).await? {
                            UdpTrackerResolveResult::One(addr) => comms.fetch_udp(addr, &udp_client).await,
                            UdpTrackerResolveResult::Two(v4, v6) => {
                                match comms.fetch_udp(v4.into(), &udp_client).await {
                                    Ok(value) => Ok(value),
                                    Err(_) => comms.fetch_udp(v6.into(), &udp_client).await,
                                }
                            }
                        }
                    }
                    _ => anyhow::bail!("unsupported tracker scheme"),
                }
            }).await;
            match response {
                Ok(Ok((_, peers))) => {
                    comms.observe(&attempt.url, started, Some(peers.len()));
                    for peer in peers { yield peer; }
                }
                _ => comms.observe(&attempt.url, started, None),
            }
        }.boxed()
    }

    // TODO: fix too many args
    #[allow(clippy::too_many_arguments)]
    pub fn start(
        info_hash: Id20,
        peer_id: Id20,
        trackers: HashSet<Url>,
        stats: Box<dyn TorrentStatsProvider>,
        force_interval: Option<Duration>,
        announce_port: u16,
        reqwest_client: reqwest::Client,
        udp_client: UdpTrackerClient,
    ) -> Option<BoxStream<'static, SocketAddr>> {
        Self::start_with_observer(
            info_hash,
            peer_id,
            trackers,
            stats,
            force_interval,
            announce_port,
            reqwest_client,
            udp_client,
            None,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn start_with_observer(
        info_hash: Id20,
        peer_id: Id20,
        trackers: HashSet<Url>,
        stats: Box<dyn TorrentStatsProvider>,
        force_interval: Option<Duration>,
        announce_port: u16,
        reqwest_client: reqwest::Client,
        udp_client: UdpTrackerClient,
        observer: Option<TrackerObserver>,
    ) -> Option<BoxStream<'static, SocketAddr>> {
        let trackers = trackers
            .into_iter()
            .filter_map(|t| match t.scheme() {
                "http" | "https" => Some(SupportedTracker::Http(t)),
                "udp" => Some(SupportedTracker::Udp(t)),
                _ => {
                    debug!(
                        "unsupported tracker URL: {}",
                        sanitized_tracker_endpoint(&t)
                    );
                    None
                }
            })
            .collect::<Vec<_>>();
        if trackers.is_empty() {
            debug!(?info_hash, "trackers list is empty");
            return None;
        }

        tracing::trace!(?trackers);

        let (tx, mut rx) = tokio::sync::mpsc::channel::<SocketAddr>(16);

        let s = async_stream::stream! {
            use futures::StreamExt;
            let comms = Arc::new(Self {
                info_hash,
                peer_id,
                stats,
                force_tracker_interval: force_interval,
                tx,
                announce_port,
                reqwest_client,
                key: rand::random(),
                observer,
            });
            let mut futures = FuturesUnordered::new();
            for tracker in trackers {
                futures.push(comms.add_tracker(tracker, &udp_client))
            }
            while !(futures.is_empty()) {
                tokio::select! {
                    addr = rx.recv() => {
                        if let Some(addr) = addr {
                            yield addr;
                        }
                    }
                    e = futures.next(), if !futures.is_empty() => {
                        if let Some(Err(e)) = e {
                            debug!("error: {e}");
                        }
                    }
                }
            }
        };

        Some(s.boxed())
    }

    fn add_tracker(
        &self,
        url: SupportedTracker,
        client: &UdpTrackerClient,
    ) -> Either<
        impl std::future::Future<Output = anyhow::Result<()>> + '_ + Send,
        impl std::future::Future<Output = anyhow::Result<()>> + '_ + Send,
    > {
        let info_hash = self.info_hash;
        match url {
            SupportedTracker::Udp(url) => {
                let span = debug_span!(parent: None, "udp_tracker", tracker = %sanitized_tracker_endpoint(&url), info_hash = ?info_hash);
                self.task_single_tracker_monitor_udp(url, client.clone())
                    .instrument(span)
                    .right_future()
            }
            SupportedTracker::Http(url) => {
                let span = debug_span!(
                    parent: None,
                    "http_tracker",
                    tracker = %sanitized_tracker_endpoint(&url),
                    info_hash = ?info_hash
                );
                self.task_single_tracker_monitor_http(url)
                    .instrument(span)
                    .left_future()
            }
        }
    }

    async fn task_single_tracker_monitor_http(&self, tracker_url: Url) -> anyhow::Result<()> {
        trace!(endpoint=%sanitized_tracker_endpoint(&tracker_url), "starting monitor");
        let mut event = Some(tracker_comms_http::TrackerRequestEvent::Started);

        loop {
            let interval = (|| self.tracker_one_request_http(&tracker_url, event))
                .retry(
                    ExponentialBuilder::new()
                        .without_max_times()
                        .with_jitter()
                        .with_factor(2.)
                        .with_min_delay(Duration::from_secs(10))
                        .with_max_delay(Duration::from_secs(600)),
                )
                .notify(|err, retry_in| debug!(?retry_in, "error calling tracker: {err:#}"))
                .await
                .context("this shouldn't fail")?;

            event = None;
            let interval = self.force_tracker_interval.unwrap_or(interval);
            debug!("sleeping for {:?} after calling tracker", interval);
            tokio::time::sleep(interval).await;
        }
    }

    async fn tracker_one_request_http(
        &self,
        tracker_url: &Url,
        event: Option<tracker_comms_http::TrackerRequestEvent>,
    ) -> anyhow::Result<Duration> {
        let started = Instant::now();
        let result = self
            .tracker_one_request_http_inner(tracker_url, event, started)
            .await;
        if result.is_err() {
            self.observe(tracker_url, started, None);
        }
        result
    }

    fn observe(&self, url: &Url, started: Instant, peers_returned: Option<usize>) {
        if let Some(observer) = &self.observer {
            observer(TrackerObservation {
                info_hash: self.info_hash,
                endpoint: sanitized_tracker_endpoint(url),
                succeeded: peers_returned.is_some(),
                latency: started.elapsed(),
                peers_returned,
            });
        }
    }

    async fn tracker_one_request_http_inner(
        &self,
        tracker_url: &Url,
        event: Option<tracker_comms_http::TrackerRequestEvent>,
        started: Instant,
    ) -> anyhow::Result<Duration> {
        let (interval, peers) = self.fetch_http(tracker_url, event).await?;
        self.observe(tracker_url, started, Some(peers.len()));
        for peer in peers {
            if self.tx.send(peer).await.is_err() {
                break;
            }
        }
        Ok(interval)
    }

    async fn fetch_http(
        &self,
        tracker_url: &Url,
        event: Option<tracker_comms_http::TrackerRequestEvent>,
    ) -> anyhow::Result<(Duration, Vec<SocketAddr>)> {
        let stats = self.stats.get();
        let request = tracker_comms_http::TrackerRequest {
            info_hash: &self.info_hash,
            peer_id: &self.peer_id,
            port: self.announce_port,
            uploaded: stats.uploaded_bytes,
            downloaded: stats.downloaded_bytes,
            left: stats.get_left_to_download_bytes(),
            compact: true,
            no_peer_id: false,
            event,
            ip: None,
            numwant: None,
            key: Some(self.key),
            trackerid: None,
        };

        let mut url = tracker_url.clone();

        let mut queries = request.as_querystring();
        if let Some(url_query) = url.query() {
            queries.push_str(&format!("&{}", url_query));
        }
        url.set_query(Some(&queries));

        let response: reqwest::Response = self
            .reqwest_client
            .get(url)
            .send()
            .await
            .map_err(|_| anyhow::anyhow!("tracker request failed"))?;
        if !response.status().is_success() {
            anyhow::bail!("tracker responded with {:?}", response.status());
        }
        let bytes = read_tracker_response_bounded(response)
            .await
            .map_err(|_| anyhow::anyhow!("tracker response could not be read"))?;
        if let Ok((error, _)) =
            bencode::from_bytes_with_rest::<tracker_comms_http::TrackerError>(&bytes)
        {
            // Tracker-supplied text can contain a passkey; never write it to logs.
            let _ = error.failure_reason;
            anyhow::bail!("tracker returned failure")
        };
        let response = bencode::from_bytes_with_rest::<tracker_comms_http::TrackerResponse>(&bytes)
            .map_err(|_| anyhow::anyhow!("invalid tracker response"))?
            .0;

        Ok((
            Duration::from_secs(response.min_interval.unwrap_or(response.interval)),
            response.iter_peers().collect(),
        ))
    }

    async fn task_single_tracker_monitor_udp(
        &self,
        url: Url,
        client: UdpTrackerClient,
    ) -> anyhow::Result<()> {
        if url.scheme() != "udp" {
            bail!("expected UDP tracker scheme");
        }
        let (host, port) = (
            url.host().context("missing host")?,
            url.port().context("missing port")?,
        );

        let mut sleep_interval: Option<Duration> = None;
        let mut prev_addrs: Option<UdpTrackerResolveResult> = None;
        loop {
            if let Some(i) = sleep_interval {
                trace!(interval=?sleep_interval, "sleeping");
                tokio::time::sleep(i).await;
            }

            // This should retry forever until the addrs are resolved.
            let addrs = (async || {
                udp_tracker_to_socket_addrs(host.clone(), port)
                    .instrument(trace_span!("resolve", ?host))
                    .await
                    .or_else(|err| prev_addrs.ok_or(err))
            })
            .retry(
                ExponentialBuilder::new()
                    .without_max_times()
                    .with_max_delay(Duration::from_secs(60))
                    .with_jitter(),
            )
            .notify(|err, retry| debug!(retry_in=?retry, "error resolving tracker: {err:#}"))
            .await
            .context("this shouldn't happen: failed resolving tracker addrs")?;

            prev_addrs = Some(addrs);

            match addrs {
                UdpTrackerResolveResult::One(addr) => {
                    match self
                        .tracker_one_request_udp(addr, &client, &url)
                        .instrument(trace_span!("udp request", ?addr))
                        .await
                    {
                        Ok(sleep) => sleep_interval = Some(sleep),
                        Err(_) => {
                            sleep_interval = Some(sleep_interval.unwrap_or(Duration::from_secs(60)))
                        }
                    }
                }
                UdpTrackerResolveResult::Two(v4, v6) => {
                    let (r4, r6) = tokio::join!(
                        self.tracker_one_request_udp(v4.into(), &client, &url)
                            .instrument(trace_span!("udp request", addr=?v4)),
                        self.tracker_one_request_udp(v6.into(), &client, &url)
                            .instrument(trace_span!("udp request", addr=?v6))
                    );
                    sleep_interval = Some(
                        r4.or(r6)
                            .ok()
                            .or(sleep_interval)
                            .unwrap_or(Duration::from_secs(60)),
                    )
                }
            }
        }
    }

    async fn tracker_one_request_udp(
        &self,
        addr: SocketAddr,
        client: &UdpTrackerClient,
        url: &Url,
    ) -> anyhow::Result<Duration> {
        let started = Instant::now();
        let result = self
            .tracker_one_request_udp_inner(addr, client, url, started)
            .await;
        if result.is_err() {
            self.observe(url, started, None);
        }
        result
    }

    async fn tracker_one_request_udp_inner(
        &self,
        addr: SocketAddr,
        client: &UdpTrackerClient,
        url: &Url,
        started: Instant,
    ) -> anyhow::Result<Duration> {
        let (interval, peers) = self.fetch_udp(addr, client).await?;
        self.observe(url, started, Some(peers.len()));
        for peer in peers {
            if self.tx.send(peer).await.is_err() {
                break;
            }
        }
        Ok(interval)
    }

    async fn fetch_udp(
        &self,
        addr: SocketAddr,
        client: &UdpTrackerClient,
    ) -> anyhow::Result<(Duration, Vec<SocketAddr>)> {
        use tracker_comms_udp::*;

        let stats = self.stats.get();
        let request = AnnounceFields {
            info_hash: self.info_hash,
            peer_id: self.peer_id,
            downloaded: stats.downloaded_bytes,
            left: stats.get_left_to_download_bytes(),
            uploaded: stats.uploaded_bytes,
            event: match stats.torrent_state {
                TrackerCommsStatsState::None => EVENT_NONE,
                TrackerCommsStatsState::Initializing => EVENT_STARTED,
                TrackerCommsStatsState::Paused => EVENT_STOPPED,
                TrackerCommsStatsState::Live => {
                    if stats.is_completed() {
                        EVENT_COMPLETED
                    } else {
                        EVENT_STARTED
                    }
                }
            },
            key: self.key,
            port: self.announce_port,
        };

        let response = client
            .announce(addr, request)
            .await
            .map_err(|_| anyhow::anyhow!("UDP tracker request failed"))?;
        Ok((
            Duration::from_secs(response.interval.max(5) as u64),
            response.addrs,
        ))
    }
}

#[cfg(test)]
mod catalog_observation_tests {
    #[test]
    fn supported_tracker_debug_never_exposes_credentials_or_passkey() {
        let url =
            url::Url::parse("https://user:secret@tracker.example/passkey/announce?token=private")
                .unwrap();
        let debug = format!("{:?}", super::SupportedTracker::Http(url));
        assert_eq!(debug, "https://tracker.example");
    }
    use super::*;

    #[tokio::test]
    async fn observes_owned_http_announce_without_leaking_path_or_query() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut buffer = [0; 2048];
            let _ = stream.read(&mut buffer).unwrap();
            let body =
                b"d8:intervali60e5:peers12:\x7f\x00\x00\x01\x1a\xe1\x7f\x00\x00\x01\x1a\xe2e";
            let header = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(header.as_bytes()).unwrap();
            stream.write_all(body).unwrap();
        });
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let observed = Arc::new(std::sync::Mutex::new(Vec::new()));
        let out = observed.clone();
        let comms = TrackerComms {
            info_hash: Id20::new([0; 20]),
            peer_id: Id20::new([1; 20]),
            stats: Box::new(()),
            force_tracker_interval: None,
            tx,
            announce_port: 4240,
            reqwest_client: reqwest::Client::new(),
            key: 0,
            observer: Some(Arc::new(move |v| out.lock().unwrap().push(v))),
        };
        let url = Url::parse(&format!("http://{addr}/secret/announce?passkey=sensitive")).unwrap();
        let request = comms.tracker_one_request_http(&url, None);
        tokio::pin!(request);
        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut request)
                .await
                .is_err()
        );
        assert_eq!(
            observed.lock().unwrap().len(),
            1,
            "reply must be observed before peer channel is drained"
        );
        assert_eq!(rx.recv().await.unwrap().port(), 6881);
        request.await.unwrap();
        server.join().unwrap();
        let values = observed.lock().unwrap();
        assert_eq!(values.len(), 1);
        assert!(values[0].succeeded);
        assert_eq!(values[0].peers_returned, Some(2));
        assert_eq!(values[0].endpoint, format!("http://{addr}"));
    }

    #[tokio::test]
    async fn rejects_oversized_http_tracker_response_before_reading_body() {
        use std::io::{Read, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().unwrap();
            let mut request = [0; 1024];
            let _ = stream.read(&mut request).unwrap();
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\nContent-Length: 2097153\r\nConnection: close\r\n\r\n",
                )
                .unwrap();
        });
        let response = reqwest::get(format!("http://{addr}/announce"))
            .await
            .unwrap();
        assert!(read_tracker_response_bounded(response).await.is_err());
        server.join().unwrap();
    }

    #[test]
    fn tracker_endpoint_removes_credentials_path_query_and_fragment() {
        let url = Url::parse(
            "https://user:secret@tracker.example:9443/passkey/announce?token=private#frag",
        )
        .unwrap();
        assert_eq!(
            sanitized_tracker_endpoint(&url),
            "https://tracker.example:9443"
        );
    }
}

#[cfg(test)]
mod catalog_attempt_tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn bounded_attempt_timeout_observes_one_failure_and_closes_socket() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!(
            "http://{}/passkey?secret=value",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0; 4096];
            assert!(sock.read(&mut buf).await.unwrap() > 0);
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
        });
        let token = tokio_util::sync::CancellationToken::new();
        let _guard = token.clone().drop_guard();
        let udp = UdpTrackerClient::new(token, None).await.unwrap();
        let observations = Arc::new(std::sync::Mutex::new(Vec::new()));
        let collector = observations.clone();
        let stream = TrackerComms::start_attempt(
            Id20::new([1; 20]),
            Id20::new([2; 20]),
            TrackerAttempt {
                url,
                timeout: Duration::from_millis(100),
            },
            Box::new(()),
            4240,
            reqwest::Client::new(),
            udp,
            Some(Arc::new(move |v| collector.lock().unwrap().push(v))),
        );
        assert!(stream.collect::<Vec<_>>().await.is_empty());
        server.await.unwrap();
        let events = observations.lock().unwrap();
        assert_eq!(events.len(), 1);
        assert!(!events[0].succeeded);
        assert_eq!(events[0].peers_returned, None);
        assert!(!format!("{:?}", events[0]).contains("passkey"));
    }

    #[tokio::test]
    async fn dropped_attempt_cancels_request_without_reporting_tracker_failure() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = Url::parse(&format!(
            "http://{}/announce",
            listener.local_addr().unwrap()
        ))
        .unwrap();
        let (received_tx, received_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = [0; 4096];
            assert!(sock.read(&mut buf).await.unwrap() > 0);
            received_tx.send(()).unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), sock.read(&mut buf))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
            let _ = sock.shutdown().await;
        });
        let token = tokio_util::sync::CancellationToken::new();
        let _guard = token.clone().drop_guard();
        let udp = UdpTrackerClient::new(token, None).await.unwrap();
        let observations = Arc::new(std::sync::Mutex::new(Vec::new()));
        let collector = observations.clone();
        let mut stream = TrackerComms::start_attempt(
            Id20::new([1; 20]),
            Id20::new([2; 20]),
            TrackerAttempt {
                url,
                timeout: Duration::from_secs(10),
            },
            Box::new(()),
            4240,
            reqwest::Client::new(),
            udp,
            Some(Arc::new(move |v| collector.lock().unwrap().push(v))),
        );
        tokio::select! { _=received_rx=>{}, _=stream.next()=>panic!("unexpected tracker response") }
        drop(stream);
        server.await.unwrap();
        assert!(observations.lock().unwrap().is_empty());
    }
}
