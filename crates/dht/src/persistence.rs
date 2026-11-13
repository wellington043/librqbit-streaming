use anyhow::Context;
use futures::FutureExt;
use futures::future::BoxFuture;
use librqbit_core::directories::get_configuration_directory;
use librqbit_core::spawn_utils::spawn_with_cancel;
use librqbit_dualstack_sockets::BindDevice;
use serde::{Deserialize, Serialize};
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

use tracing::{debug_span, error, info, trace, warn};

use crate::peer_store::PeerStore;
use crate::routing_table::RoutingTable;
use crate::{Dht, DhtConfig, DhtState};

/// Configuration for DHT persistence (periodic dump of routing table and peer store).
/// When provided, the DHT state will be serialized to disk periodically.
pub struct DhtPersistenceConfig {
    /// How often to dump state. Defaults to 60s.
    pub dump_interval: Option<Duration>,
    /// Path to the JSON file. Uses OS-specific default if None.
    pub config_filename: Option<PathBuf>,
    /// Persist announced peers and tokens as well as routing nodes. Disable for
    /// a routing-only desktop checkpoint; defaults to true for compatibility.
    pub persist_peer_store: bool,
    /// Maximum serialized checkpoint bytes, capped at 16 MiB. Oversized or
    /// unreadable checkpoints are ignored and never prevent DHT startup.
    pub max_state_bytes: usize,
}

const MAX_STATE_BYTES: usize = 16 * 1024 * 1024;

impl Default for DhtPersistenceConfig {
    fn default() -> Self {
        Self {
            dump_interval: None,
            config_filename: None,
            persist_peer_store: true,
            max_state_bytes: MAX_STATE_BYTES,
        }
    }
}

/// Compute the DHT listen address from the explicit/stored port preferences and
/// the session-level `ipv4_only` flag.
///
/// The bind IP is derived solely from `ipv4_only` (`0.0.0.0` if true, `[::]`
/// otherwise); any persisted IP is intentionally ignored. The port is chosen
/// in priority order: explicit -> stored -> 0 (random).
pub fn dht_listen_addr(port: Option<u16>, stored_port: Option<u16>, ipv4_only: bool) -> SocketAddr {
    let ip: IpAddr = if ipv4_only {
        Ipv4Addr::UNSPECIFIED.into()
    } else {
        Ipv6Addr::UNSPECIFIED.into()
    };
    SocketAddr::new(ip, port.or(stored_port).unwrap_or(0))
}

#[derive(Serialize, Deserialize)]
struct DhtSerialize<Table, PeerStore> {
    addr: SocketAddr,
    table: Table,
    // option for backwards compat
    table_v6: Option<Table>,
    peer_store: Option<PeerStore>,
}

pub struct PersistentDht {
    // config_filename: PathBuf,
}

fn dump_dht(dht: &Dht, filename: &Path, persist_peers: bool, limit: usize) -> anyhow::Result<()> {
    struct BoundedBytes {
        bytes: Vec<u8>,
        limit: usize,
    }
    impl Write for BoundedBytes {
        fn write(&mut self, input: &[u8]) -> std::io::Result<usize> {
            if input.len() > self.limit.saturating_sub(self.bytes.len()) {
                return Err(std::io::Error::other("DHT checkpoint exceeds byte limit"));
            }
            self.bytes.extend_from_slice(input);
            Ok(input.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut output = BoundedBytes {
        bytes: Vec::new(),
        limit: limit.clamp(1, MAX_STATE_BYTES),
    };
    let addr = dht.listen_addr();
    dht.with_routing_tables(|v4, v6| {
        serde_json::to_writer(
            &mut output,
            &DhtSerialize {
                addr,
                table: v4,
                table_v6: Some(v6),
                peer_store: persist_peers.then_some(&dht.peer_store),
            },
        )
    })?;
    let parent = filename
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let mut file = tempfile::Builder::new()
        .prefix(".routing-")
        .suffix(".tmp")
        .tempfile_in(parent)?;
    file.write_all(&output.bytes)?;
    file.as_file().sync_all()?;
    file.persist(filename).context("publish DHT checkpoint")?;
    #[cfg(unix)]
    std::fs::File::open(parent)?.sync_all()?;
    Ok(())
}

fn load_dht(
    filename: &Path,
    limit: usize,
) -> anyhow::Result<DhtSerialize<RoutingTable, PeerStore>> {
    let metadata = std::fs::symlink_metadata(filename)?;
    anyhow::ensure!(
        metadata.is_file() && !metadata.file_type().is_symlink(),
        "invalid DHT checkpoint file"
    );
    let limit = limit.clamp(1, MAX_STATE_BYTES);
    anyhow::ensure!(
        metadata.len() <= limit as u64,
        "DHT checkpoint exceeds byte limit"
    );
    let mut bytes = Vec::new();
    OpenOptions::new()
        .read(true)
        .open(filename)?
        .take(limit as u64 + 1)
        .read_to_end(&mut bytes)?;
    anyhow::ensure!(bytes.len() <= limit, "DHT checkpoint exceeds byte limit");
    let mut decoded: DhtSerialize<RoutingTable, PeerStore> = serde_json::from_slice(&bytes)?;
    if let Some(v6) = &decoded.table_v6 {
        anyhow::ensure!(
            v6.id() == decoded.table.id(),
            "inconsistent DHT routing identities"
        );
    }
    decoded.table = decoded.table.rebuild_persisted(false);
    decoded.table_v6 = decoded.table_v6.map(|table| table.rebuild_persisted(true));
    Ok(decoded)
}

impl PersistentDht {
    pub fn default_persistence_filename() -> anyhow::Result<PathBuf> {
        let dirs = get_configuration_directory("dht")?;
        let path = dirs.cache_dir().join("dht.json");
        Ok(path)
    }

    #[inline(never)]
    pub fn create<'a>(
        persistence_config: DhtPersistenceConfig,
        port: Option<u16>,
        ipv4_only: bool,
        bootstrap_addrs: Option<Vec<String>>,
        cancellation_token: Option<CancellationToken>,
        bind_device: Option<&'a BindDevice>,
    ) -> BoxFuture<'a, anyhow::Result<Dht>> {
        async move {
            let config_filename = match persistence_config.config_filename {
                Some(f) => f,
                None => Self::default_persistence_filename()?,
            };

            info!(
                filename=?config_filename,
                "will store DHT routing table periodically",
            );

            if let Some(parent) = config_filename.parent()
                && std::fs::create_dir_all(parent).is_err()
            {
                warn!("DHT checkpoint directory unavailable; discovery remains ephemeral until storage recovers");
            }

            let de = load_dht(&config_filename, persistence_config.max_state_bytes).ok();
            let (stored_port, routing_table, routing_table_v6, peer_store) = de
                .map(|de| (Some(de.addr.port()), Some(de.table), de.table_v6, if persistence_config.persist_peer_store { de.peer_store } else { None }))
                .unwrap_or((None, None, None, None));

            let listen_addr = dht_listen_addr(port, stored_port, ipv4_only);
            let peer_id = routing_table.as_ref().map(|r| r.id());

            let dht_config = DhtConfig {
                peer_id,
                bootstrap_addrs,
                routing_table,
                routing_table_v6,
                listen_addr: Some(listen_addr),
                peer_store,
                cancellation_token,
                bind_device,
            };
            let dht = DhtState::with_config(dht_config).await?;
            spawn_with_cancel::<anyhow::Error>(
                debug_span!("dht_persistence"),
                "dht_persistence",
                dht.cancellation_token().clone(),
                {
                    let dht = dht.clone();
                    let dump_interval = persistence_config
                        .dump_interval
                        .unwrap_or_else(|| Duration::from_secs(60))
                        .max(Duration::from_secs(1));
                    async move {
                        loop {
                            trace!("sleeping for {:?}", &dump_interval);
                            tokio::time::sleep(dump_interval).await;

                            match dump_dht(&dht, &config_filename, persistence_config.persist_peer_store, persistence_config.max_state_bytes) {
                                Ok(_) => trace!(filename=?config_filename, "dumped DHT"),
                                Err(e) => {
                                    error!(filename=?config_filename, "error dumping DHT: {:#}", e)
                                }
                            }
                        }
                    }
                },
            );

            Ok(dht)
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Id20;

    #[test]
    fn corrupt_structural_routing_data_is_rebuilt_without_following_links() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("routing.json");
        let own = Id20::new([1; 20]);
        let mut v4 = RoutingTable::new(own, None);
        v4.add_node(Id20::new([2; 20]), "127.0.0.1:9".parse().unwrap());
        v4.add_node(Id20::new([3; 20]), "[::1]:9".parse().unwrap());
        v4.add_node(own, "127.0.0.1:10".parse().unwrap());
        let v6 = RoutingTable::new(own, None);
        let input = DhtSerialize {
            addr: "127.0.0.1:1".parse().unwrap(),
            table: &v4,
            table_v6: Some(&v6),
            peer_store: None::<&PeerStore>,
        };
        let pristine = serde_json::to_value(input).unwrap();
        for links in [[0, 0], [999999, 999999]] {
            let mut data = pristine.clone();
            let leaf = data["table"]["buckets"]["data"][0].clone();
            let mut bad = leaf.clone();
            bad["data"] = serde_json::json!({"LeftRight": links});
            data["table"]["buckets"]["data"] = serde_json::json!([bad, leaf]);
            data["table"]["size"] = serde_json::json!(usize::MAX);
            data["table"]["buckets"]["size"] = serde_json::json!(usize::MAX);
            data["table"]["buckets"]["max_size"] = serde_json::json!(usize::MAX);
            std::fs::write(&file, serde_json::to_vec(&data).unwrap()).unwrap();
            let restored = load_dht(&file, MAX_STATE_BYTES).unwrap();
            assert_eq!(restored.table.len(), 1);
            assert_eq!(restored.table.iter().count(), 1);
            assert!(
                restored
                    .table
                    .iter()
                    .all(|node| node.addr().is_ipv4() && node.id() != own)
            );
        }
    }

    #[test]
    fn persisted_nodes_are_bounded_even_when_counters_claim_more() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("routing.json");
        let own = Id20::new([1; 20]);
        let v4 = RoutingTable::new(own, None);
        let input = DhtSerialize {
            addr: "127.0.0.1:1".parse().unwrap(),
            table: &v4,
            table_v6: None::<&RoutingTable>,
            peer_store: None::<&PeerStore>,
        };
        let mut data = serde_json::to_value(input).unwrap();
        let nodes: Vec<_> = (0..600u32)
            .map(|i| {
                let mut bytes = [0u8; 20];
                bytes[..4].copy_from_slice(&i.to_be_bytes());
                serde_json::json!({"id":Id20::new(bytes).as_string(),"addr":"127.0.0.1:1234"})
            })
            .collect();
        data["table"]["buckets"]["data"][0]["data"]["Leaf"]["nodes"] = serde_json::json!(nodes);
        data["table"]["size"] = serde_json::json!(usize::MAX);
        data["table"]["buckets"]["size"] = serde_json::json!(usize::MAX);
        data["table"]["buckets"]["max_size"] = serde_json::json!(usize::MAX);
        std::fs::write(&file, serde_json::to_vec(&data).unwrap()).unwrap();
        let restored = load_dht(&file, MAX_STATE_BYTES).unwrap();
        assert!(restored.table.len() <= 512);
        assert_eq!(restored.table.len(), restored.table.iter().count());
        assert!(restored.table.len() > 0);
    }

    #[test]
    fn conflicting_local_routing_ids_are_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("routing.json");
        let v4 = RoutingTable::new(Id20::new([1; 20]), None);
        let v6 = RoutingTable::new(Id20::new([2; 20]), None);
        let input = DhtSerialize {
            addr: "127.0.0.1:1".parse().unwrap(),
            table: &v4,
            table_v6: Some(&v6),
            peer_store: None::<&PeerStore>,
        };
        std::fs::write(&file, serde_json::to_vec(&input).unwrap()).unwrap();
        assert!(load_dht(&file, MAX_STATE_BYTES).is_err());
    }

    #[tokio::test]
    async fn persistence_restores_both_routing_families() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("routing.json");
        let id = Id20::new([1; 20]);
        let mut v6 = RoutingTable::new(id, None);
        v6.add_node(Id20::new([2; 20]), "[::1]:9".parse().unwrap());
        let token = CancellationToken::new();
        let first = DhtState::with_config(DhtConfig {
            peer_id: Some(id),
            routing_table_v6: Some(v6),
            listen_addr: Some("127.0.0.1:0".parse().unwrap()),
            bootstrap_addrs: Some(vec![]),
            cancellation_token: Some(token.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        dump_dht(&first, &file, false, MAX_STATE_BYTES).unwrap();
        let restored = PersistentDht::create(
            DhtPersistenceConfig {
                config_filename: Some(file),
                ..Default::default()
            },
            Some(0),
            true,
            Some(vec![]),
            Some(token.clone()),
            None,
        )
        .await
        .unwrap();
        let count = restored.with_routing_tables(|_, v6| v6.len());
        token.cancel();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn persistence_bounds_writes_and_publishes_routing_only_atomically() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("routing.json");
        let token = CancellationToken::new();
        let dht = DhtState::with_config(DhtConfig {
            bootstrap_addrs: Some(vec![]),
            listen_addr: Some("127.0.0.1:0".parse().unwrap()),
            cancellation_token: Some(token.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        std::fs::write(&file, b"previous-checkpoint").unwrap();
        assert!(dump_dht(&dht, &file, false, 16).is_err());
        assert_eq!(std::fs::read(&file).unwrap(), b"previous-checkpoint");
        dump_dht(&dht, &file, false, MAX_STATE_BYTES).unwrap();
        let saved: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
        assert!(saved["peer_store"].is_null());
        assert!(saved["table"].is_object());
        assert!(saved["table_v6"].is_object());
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
        token.cancel();
    }

    #[tokio::test]
    async fn persistence_invalid_or_oversized_state_does_not_block_discovery() {
        let directory = tempfile::tempdir().unwrap();
        let file = directory.path().join("routing.json");
        let token = CancellationToken::new();
        for input in [b"invalid-json".as_slice(), &[b'x'; 2048]] {
            std::fs::write(&file, input).unwrap();
            assert!(load_dht(&file, 1024).is_err());
            let dht = PersistentDht::create(
                DhtPersistenceConfig {
                    config_filename: Some(file.clone()),
                    max_state_bytes: 1024,
                    persist_peer_store: false,
                    ..Default::default()
                },
                Some(0),
                true,
                Some(vec![]),
                Some(token.child_token()),
                None,
            )
            .await
            .unwrap();
            assert_ne!(dht.listen_addr().port(), 0);
            assert_eq!(std::fs::read(&file).unwrap(), input);
            dht.cancellation_token().cancel();
        }
        token.cancel();
    }

    #[test]
    fn no_port_no_stored_v6() {
        let addr = dht_listen_addr(None, None, false);
        assert_eq!(addr.ip(), IpAddr::from(Ipv6Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 0);
    }

    #[test]
    fn no_port_no_stored_v4() {
        let addr = dht_listen_addr(None, None, true);
        assert_eq!(addr.ip(), IpAddr::from(Ipv4Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 0);
    }

    #[test]
    fn explicit_port_v6() {
        let addr = dht_listen_addr(Some(6881), None, false);
        assert_eq!(addr.ip(), IpAddr::from(Ipv6Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 6881);
    }

    #[test]
    fn explicit_port_v4() {
        let addr = dht_listen_addr(Some(6881), None, true);
        assert_eq!(addr.ip(), IpAddr::from(Ipv4Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 6881);
    }

    #[test]
    fn stored_port_only_v6() {
        let addr = dht_listen_addr(None, Some(12345), false);
        assert_eq!(addr.ip(), IpAddr::from(Ipv6Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 12345);
    }

    #[test]
    fn stored_port_only_v4() {
        let addr = dht_listen_addr(None, Some(12345), true);
        assert_eq!(addr.ip(), IpAddr::from(Ipv4Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 12345);
    }

    #[test]
    fn explicit_overrides_stored_v6() {
        let addr = dht_listen_addr(Some(6881), Some(12345), false);
        assert_eq!(addr.ip(), IpAddr::from(Ipv6Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 6881);
    }

    #[test]
    fn explicit_overrides_stored_v4() {
        let addr = dht_listen_addr(Some(6881), Some(12345), true);
        assert_eq!(addr.ip(), IpAddr::from(Ipv4Addr::UNSPECIFIED));
        assert_eq!(addr.port(), 6881);
    }
}
