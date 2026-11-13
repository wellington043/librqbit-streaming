use super::*;

/// Called before changing private tracker. The owner must close peer admission,
/// disconnect and drain the preceding tracker's peers before this future resolves.
pub type TrackerSwitch = Arc<dyn Fn() -> futures::future::BoxFuture<'static, ()> + Send + Sync>;

struct TieredTrackers {
    tiers: Vec<Vec<Url>>,
    tier: usize,
    member: usize,
    failures: usize,
    rounds: u32,
    count: usize,
}

impl TieredTrackers {
    fn new(mut tiers: Vec<Vec<Url>>) -> Option<Self> {
        use rand::seq::SliceRandom;
        let mut seen = HashSet::new();
        for tier in &mut tiers {
            tier.retain(|url| {
                matches!(url.scheme(), "http" | "https" | "udp") && seen.insert(url.clone())
            });
            tier.shuffle(&mut rand::rng());
        }
        tiers.retain(|t| !t.is_empty());
        let count = tiers.iter().map(Vec::len).sum();
        (count > 0).then_some(Self {
            tiers,
            tier: 0,
            member: 0,
            failures: 0,
            rounds: 0,
            count,
        })
    }

    fn current(&self) -> &Url {
        &self.tiers[self.tier][self.member]
    }

    fn success(&mut self, private: bool) {
        // BEP 12 promotes within the successful tier, and public torrents restart
        // at the first tier next interval. BEP 27 keeps a private working tracker
        // (including zero peers) until it fails.
        let successful = self.tiers[self.tier].remove(self.member);
        self.tiers[self.tier].insert(0, successful);
        self.member = 0;
        if !private {
            self.tier = 0;
        }
        self.failures = 0;
        self.rounds = 0;
    }

    fn failure(&mut self) -> Option<Duration> {
        self.member += 1;
        if self.member == self.tiers[self.tier].len() {
            self.member = 0;
            self.tier = (self.tier + 1) % self.tiers.len();
        }
        self.failures += 1;
        if self.failures < self.count {
            return None;
        }
        self.failures = 0;
        let delay = Duration::from_secs((10u64 << self.rounds.min(6)).min(600));
        self.rounds = self.rounds.saturating_add(1);
        Some(delay)
    }
}

fn announce_delay(interval: Duration, force: Option<Duration>) -> Duration {
    interval
        .max(force.unwrap_or_default())
        .max(Duration::from_secs(5))
}

impl TrackerComms {
    /// Continuous, sequential discovery. Futures and sockets belong to the
    /// returned stream; dropping it cancels requests, waits and switch callbacks.
    /// Private torrents must supply `before_switch`: they retain a successful
    /// tracker. Public torrents pass None and restart at tier zero each interval.
    #[allow(clippy::too_many_arguments)]
    pub fn start_tiered(
        info_hash: Id20,
        peer_id: Id20,
        tiers: Vec<Vec<Url>>,
        stats: Box<dyn TorrentStatsProvider>,
        force_interval: Option<Duration>,
        announce_port: u16,
        reqwest_client: reqwest::Client,
        udp_client: UdpTrackerClient,
        observer: Option<TrackerObserver>,
        before_switch: Option<TrackerSwitch>,
    ) -> Option<BoxStream<'static, SocketAddr>> {
        let mut plan = TieredTrackers::new(tiers)?;
        Some(async_stream::stream! {
            let (tx, _rx) = tokio::sync::mpsc::channel(1);
            let comms = Self { info_hash, peer_id, stats, force_tracker_interval: force_interval,
                tx, announce_port, reqwest_client, key: rand::random(), observer };
            let mut previous = None;
            let mut started = false;
            let mut completed = false;
            loop {
                let url = plan.current().clone();
                if previous.as_ref().is_some_and(|old| old != &url) {
                    if let Some(switch) = &before_switch { switch().await; }
                    started = false;
                    completed = false;
                }
                previous = Some(url.clone());
                let stats = comms.stats.get();
                let event = if !started { Some(tracker_comms_http::TrackerRequestEvent::Started) }
                    else if !completed && stats.total_bytes > 0 && stats.is_completed() {
                        Some(tracker_comms_http::TrackerRequestEvent::Completed)
                    } else { None };
                let began = Instant::now();
                let result = tokio::time::timeout(Duration::from_secs(30), comms.fetch_tiered(&url, &udp_client, event)).await;
                match result {
                    Ok(Ok((interval, peers))) => {
                        comms.observe(&url, began, Some(peers.len()));
                        started = true;
                        completed |= matches!(event, Some(tracker_comms_http::TrackerRequestEvent::Completed));
                        plan.success(before_switch.is_some());
                        // Account for consumer backpressure in the announce interval.
                        let next = tokio::time::Instant::now().checked_add(announce_delay(interval, force_interval));
                        for peer in peers { yield peer; }
                        if let Some(next) = next { tokio::time::sleep_until(next).await; }
                        else { std::future::pending::<()>().await; }
                    }
                    _ => {
                        comms.observe(&url, began, None);
                        if let Some(delay) = plan.failure() { tokio::time::sleep(delay).await; }
                    }
                }
            }
        }.boxed())
    }

    async fn fetch_tiered(
        &self,
        url: &Url,
        udp: &UdpTrackerClient,
        event: Option<tracker_comms_http::TrackerRequestEvent>,
    ) -> anyhow::Result<(Duration, Vec<SocketAddr>)> {
        if matches!(url.scheme(), "http" | "https") {
            return self.fetch_http(url, event).await;
        }
        let host = url.host().context("missing tracker host")?;
        let port = url.port().context("missing tracker port")?;
        let udp_event = match event {
            Some(tracker_comms_http::TrackerRequestEvent::Started) => {
                tracker_comms_udp::EVENT_STARTED
            }
            Some(tracker_comms_http::TrackerRequestEvent::Completed) => {
                tracker_comms_udp::EVENT_COMPLETED
            }
            Some(tracker_comms_http::TrackerRequestEvent::Stopped) => {
                tracker_comms_udp::EVENT_STOPPED
            }
            None => tracker_comms_udp::EVENT_NONE,
        };
        match udp_tracker_to_socket_addrs(host, port).await? {
            UdpTrackerResolveResult::One(addr) => {
                self.fetch_udp_event(addr, udp, Some(udp_event)).await
            }
            UdpTrackerResolveResult::Two(v4, v6) => {
                match self.fetch_udp_event(v4.into(), udp, Some(udp_event)).await {
                    Ok(value) => Ok(value),
                    Err(_) => self.fetch_udp_event(v6.into(), udp, Some(udp_event)).await,
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    fn url(n: usize) -> Url {
        format!("http://tracker{n}.example/announce")
            .parse()
            .unwrap()
    }

    #[test]
    fn continuous_tiers_success_is_sticky_and_failure_preserves_tier_order() {
        let mut plan = TieredTrackers::new(vec![
            vec![url(1)],
            vec![url(2), url(3)],
            vec![url(4), url(1)],
        ])
        .unwrap();
        assert_eq!(plan.current(), &url(1));
        assert_eq!(plan.failure(), None);
        let second_tier = plan.current().clone();
        assert!([url(2), url(3)].contains(&second_tier));
        plan.success(true);
        assert_eq!(plan.current(), &second_tier);
        assert_eq!(plan.failure(), None);
        assert!([url(2), url(3)].contains(plan.current()));
        assert_ne!(plan.current(), &second_tier);
        assert_eq!(plan.failure(), None);
        assert_eq!(plan.current(), &url(4));
        assert_eq!(plan.failure(), None);
        assert_eq!(plan.current(), &url(1));
        assert_eq!(plan.failure(), Some(Duration::from_secs(10)));
        assert_eq!(plan.current(), &second_tier);
    }

    #[test]
    fn continuous_backoff_is_bounded_and_success_resets_failure_rounds() {
        let mut plan = TieredTrackers::new(vec![vec![url(1)]]).unwrap();
        for expected in [10, 20, 40, 80, 160, 320, 600, 600] {
            assert_eq!(plan.failure(), Some(Duration::from_secs(expected)));
        }
        plan.success(true);
        assert_eq!(plan.failure(), Some(Duration::from_secs(10)));
    }

    #[test]
    fn continuous_public_returns_to_first_tier_on_next_announce() {
        let mut plan = TieredTrackers::new(vec![vec![url(1)], vec![url(2)]]).unwrap();
        plan.failure();
        assert_eq!(plan.current(), &url(2));
        plan.success(false);
        assert_eq!(
            plan.current(),
            &url(1),
            "public announces restart at the first tier after the interval"
        );
    }

    #[test]
    fn continuous_never_shortens_tracker_interval_or_accepts_zero_delay() {
        assert_eq!(announce_delay(Duration::ZERO, None), Duration::from_secs(5));
        assert_eq!(
            announce_delay(Duration::from_secs(90), Some(Duration::from_secs(1))),
            Duration::from_secs(90)
        );
        assert_eq!(
            announce_delay(Duration::from_secs(30), Some(Duration::from_secs(60))),
            Duration::from_secs(60)
        );
        assert!(
            TieredTrackers::new(vec![vec![
                "wss://tracker.example/announce".parse().unwrap()
            ]])
            .is_none()
        );
    }

    async fn tracker() -> (tokio::net::TcpListener, Url) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!(
            "http://{}/secret?passkey=private",
            listener.local_addr().unwrap()
        )
        .parse()
        .unwrap();
        (listener, url)
    }

    async fn respond(listener: &tokio::net::TcpListener, body: &[u8]) {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = [0; 4096];
        socket.read(&mut request).await.unwrap();
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

    async fn stream(
        tiers: Vec<Vec<Url>>,
        switch: Option<TrackerSwitch>,
        observer: Option<TrackerObserver>,
    ) -> (BoxStream<'static, SocketAddr>, tokio_util::sync::DropGuard) {
        let token = tokio_util::sync::CancellationToken::new();
        let guard = token.clone().drop_guard();
        let udp = UdpTrackerClient::new(token, None).await.unwrap();
        (
            TrackerComms::start_tiered(
                Id20::new([1; 20]),
                Id20::new([2; 20]),
                tiers,
                Box::new(()),
                None,
                4240,
                reqwest::Client::new(),
                udp,
                observer,
                switch,
            )
            .unwrap(),
            guard,
        )
    }

    #[tokio::test]
    async fn continuous_zero_peers_does_not_advance_or_reannounce_early() {
        let (first, first_url) = tracker().await;
        let (second, second_url) = tracker().await;
        let observations = Arc::new(std::sync::Mutex::new(Vec::new()));
        let out = observations.clone();
        let (mut peers, _guard) = stream(
            vec![vec![first_url], vec![second_url]],
            None,
            Some(Arc::new(move |v| out.lock().unwrap().push(v))),
        )
        .await;
        let server = tokio::spawn(async move {
            respond(&first, b"d8:intervali60e12:min intervali1e5:peers0:e").await;
            assert!(
                tokio::time::timeout(Duration::from_millis(250), first.accept())
                    .await
                    .is_err()
            );
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(350), peers.next())
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), second.accept())
                .await
                .is_err()
        );
        let values = observations.lock().unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].peers_returned, Some(0));
        assert!(!format!("{:?}", values[0]).contains("passkey"));
        drop(values);
        drop(peers);
        server.await.unwrap();
    }

    #[tokio::test]
    async fn continuous_private_switch_waits_for_owner_drain_before_next_announce() {
        let (first, first_url) = tracker().await;
        let (second, second_url) = tracker().await;
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let switch: TrackerSwitch = {
            let entered = entered.clone();
            let release = release.clone();
            Arc::new(move || {
                let entered = entered.clone();
                let release = release.clone();
                async move {
                    entered.notify_one();
                    release.notified().await;
                }
                .boxed()
            })
        };
        let (mut peers, _guard) =
            stream(vec![vec![first_url], vec![second_url]], Some(switch), None).await;
        let first_task = tokio::spawn(async move {
            respond(&first, b"d14:failure reason6:failede").await;
        });
        let pump = tokio::spawn(async move { peers.next().await });
        tokio::time::timeout(Duration::from_secs(2), entered.notified())
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), second.accept())
                .await
                .is_err()
        );
        release.notify_one();
        respond(
            &second,
            b"d8:intervali60e5:peers6:\x7f\x00\x00\x01\x1a\xe1e",
        )
        .await;
        assert_eq!(pump.await.unwrap(), Some("127.0.0.1:6881".parse().unwrap()));
        first_task.await.unwrap();
    }

    #[tokio::test]
    async fn continuous_drop_cancels_stalled_request_without_false_failure_or_next_tracker() {
        let (first, first_url) = tracker().await;
        let (second, second_url) = tracker().await;
        let observations = Arc::new(std::sync::Mutex::new(Vec::new()));
        let out = observations.clone();
        let (mut peers, _guard) = stream(
            vec![vec![first_url], vec![second_url]],
            None,
            Some(Arc::new(move |v| out.lock().unwrap().push(v))),
        )
        .await;
        let (received_tx, received_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = first.accept().await.unwrap();
            let mut request = [0; 4096];
            socket.read(&mut request).await.unwrap();
            received_tx.send(()).unwrap();
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(2), socket.read(&mut request))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
        });
        tokio::select! { _ = received_rx => {}, _ = peers.next() => panic!("unexpected peer") }
        drop(peers);
        server.await.unwrap();
        assert!(observations.lock().unwrap().is_empty());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), second.accept())
                .await
                .is_err()
        );
    }
}
