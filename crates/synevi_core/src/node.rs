use crate::replica::ReplicaConfig;
use crate::{coordinator::CoordinatorIterator, wait_handler::WaitHandler};
use anyhow::Result;
use diesel_ulid::DieselUlid;
use std::fmt::Debug;
use std::sync::{atomic::AtomicU64, Arc};
use synevi_network::network::{Network, NodeInfo};
use synevi_persistence::event_store::Store;
use synevi_types::{Executor, Transaction};
use tokio::sync::Mutex;
use tracing::instrument;

#[derive(Debug, Default)]
pub struct Stats {
    pub total_requests: AtomicU64,
    pub total_accepts: AtomicU64,
    pub total_recovers: AtomicU64,
}

pub struct Node<N, E, S>
where
    N: Network + Send + Sync,
    E: Executor + Send + Sync,
    S: Store + Send + Sync,
{
    pub info: NodeInfo,
    pub network: N,
    pub executor: E,
    pub event_store: Mutex<S>,
    pub stats: Stats,
    semaphore: tokio::sync::Semaphore,
}

impl<N, E, S> Node<N, E, S>
where
    N: Network + Send + Sync,
    E: Executor + Send + Sync,
{
    #[instrument(level = "trace", skip(network, executor, store))]
    pub async fn new_with_parameters(
        id: DieselUlid,
        serial: u16,
        network: N,
        store: S,
        executor: E,
    ) -> Result<Arc<Self>> {
        let node_name = NodeInfo { id, serial };

        let stats = Stats {
            total_requests: AtomicU64::new(0),
            total_accepts: AtomicU64::new(0),
            total_recovers: AtomicU64::new(0),
        };

        //let reorder_buffer = ReorderBuffer::new(event_store.clone());

        // let reorder_clone = reorder_buffer.clone();
        // tokio::spawn(async move {
        //     reorder_clone.run().await.unwrap();
        // });

        let node = Arc::new(Node {
            info: node_name,
            event_store: Mutex::new(store),
            network,
            stats,
            semaphore: Arc::new(tokio::sync::Semaphore::new(10)),
            executor,
        });

        let wait_handler = WaitHandler::new(node.clone());
        let wait_handler_clone = wait_handler.clone();
        tokio::spawn(async move {
            wait_handler_clone.run().await.unwrap();
        });

        let replica = Arc::new(ReplicaConfig::new(node.clone(), wait_handler));
        // Spawn tonic server
        network.spawn_server(replica).await?;

        // If no config / persistence -> default
        Ok(node)
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn add_member(&mut self, id: DieselUlid, serial: u16, host: String) -> Result<()> {
        self.network.add_member(id, serial, host).await
    }

    #[instrument(level = "trace", skip(self, transaction))]
    pub async fn transaction<T: Transaction>(
        &self,
        id: u128,
        transaction: T,
    ) -> Result<T::ExecutionResult> {
        let _permit = self.semaphore.acquire().await?;
        let interface = self.network.get_interface().await;
        let mut coordinator_iter = CoordinatorIterator::new(
            self.info.clone(),
            self.event_store.clone(),
            interface,
            transaction,
            self.stats.clone(),
            self.wait_handler.clone(),
            id,
        )
        .await;

        while coordinator_iter.next().await?.is_some() {}
        Ok(coordinator_iter.get_result())
    }

    #[instrument(level = "trace", skip(self))]
    pub fn get_event_store(&self) -> Arc<Mutex<S>> {
        self.event_store.clone()
    }

    #[instrument(level = "trace", skip(self))]
    pub fn get_info(&self) -> Arc<NodeInfo> {
        self.info.clone()
    }

    pub fn get_stats(&self) -> (u64, u64, u64) {
        (
            self.stats
                .total_requests
                .load(std::sync::atomic::Ordering::Relaxed),
            self.stats
                .total_accepts
                .load(std::sync::atomic::Ordering::Relaxed),
            self.stats
                .total_recovers
                .load(std::sync::atomic::Ordering::Relaxed),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::str::FromStr;
    use std::sync::Arc;

    use diesel_ulid::DieselUlid;
    use rand::distributions::{Distribution, Uniform};

    use synevi_network::consensus_transport::State;
    use synevi_types::{T, T0};

    use crate::coordinator::CoordinatorIterator;
    use crate::node::Node;
    use crate::tests::NetworkMock;

    #[tokio::test(flavor = "multi_thread")]
    async fn recovery_single() {
        let node_names: Vec<_> = (0..5).map(|_| DieselUlid::generate()).collect();
        let mut nodes: Vec<Node> = vec![];

        for (i, m) in node_names.iter().enumerate() {
            let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 13100 + i)).unwrap();
            let network = Arc::new(synevi_network::network::NetworkConfig::new(socket_addr));
            let (sdx, _) = tokio::sync::mpsc::channel(100);
            let node = Node::new_with_parameters(*m, i as u16, network, None, sdx)
                .await
                .unwrap();
            nodes.push(node);
        }
        for (i, name) in node_names.iter().enumerate() {
            for (i2, node) in nodes.iter_mut().enumerate() {
                if i != i2 {
                    node.add_member(*name, i as u16, format!("http://localhost:{}", 13100 + i))
                        .await
                        .unwrap();
                }
            }
        }
        let coordinator = nodes.pop().unwrap();
        let arc_coordinator = Arc::new(coordinator);

        let coordinator = arc_coordinator.clone();
        let interface = coordinator.network.get_interface().await;
        let mut coordinator_iter = CoordinatorIterator::new(
            coordinator.info.clone(),
            coordinator.event_store.clone(),
            interface,
            Vec::from("Recovery transaction"),
            coordinator.stats.clone(),
            coordinator.wait_handler.clone(),
            0,
        )
        .await;
        coordinator_iter.next().await.unwrap();

        // let accepted = coordinator_iter.next().await.unwrap();
        // let committed = coordinator_iter.next().await.unwrap();
        // let applied = coordinator_iter.next().await.unwrap();

        arc_coordinator
            .transaction(1, Vec::from("First"))
            .await
            .unwrap();

        let coord = arc_coordinator
            .get_event_store()
            .lock()
            .await
            .events
            .clone();
        for node in nodes {
            assert_eq!(node.get_event_store().lock().await.events, coord);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn recovery_random_test() {
        let node_names: Vec<_> = (0..5).map(|_| DieselUlid::generate()).collect();
        let mut nodes: Vec<Node> = vec![];

        for (i, m) in node_names.iter().enumerate() {
            let socket_addr = SocketAddr::from_str(&format!("0.0.0.0:{}", 13000 + i)).unwrap();
            let network = Arc::new(synevi_network::network::NetworkConfig::new(socket_addr));
            let (sdx, _) = tokio::sync::mpsc::channel(100);
            let node = Node::new_with_parameters(*m, i as u16, network, None, sdx)
                .await
                .unwrap();
            nodes.push(node);
        }
        for (i, name) in node_names.iter().enumerate() {
            for (i2, node) in nodes.iter_mut().enumerate() {
                if i != i2 {
                    node.add_member(*name, i as u16, format!("http://localhost:{}", 13000 + i))
                        .await
                        .unwrap();
                }
            }
        }
        let coordinator = nodes.pop().unwrap();
        let arc_coordinator = Arc::new(coordinator);

        let coordinator = arc_coordinator.clone();

        let mut rng = rand::thread_rng();
        let die = Uniform::from(0..10);

        // Working coordinator
        for _ in 0..10 {
            // Working coordinator
            let mut coordinator_iter = CoordinatorIterator::new(
                coordinator.info.clone(),
                coordinator.event_store.clone(),
                coordinator.network.get_interface().await,
                Vec::from("Recovery transaction"),
                coordinator.stats.clone(),
                coordinator.wait_handler.clone(),
                0,
            )
            .await;
            while coordinator_iter.next().await.unwrap().is_some() {
                let throw = die.sample(&mut rng);
                if throw == 0 {
                    break;
                }
            }
        }
        coordinator
            .transaction(0, Vec::from("last transaction"))
            .await
            .unwrap();

        let coordinator_store: BTreeMap<T0, T> = arc_coordinator
            .get_event_store()
            .lock()
            .await
            .events
            .clone()
            .into_values()
            .map(|e| (e.t_zero, e.t))
            .collect();
        assert!(arc_coordinator
            .get_event_store()
            .lock()
            .await
            .events
            .clone()
            .iter()
            .all(|(_, e)| e.state == State::Applied));

        let mut got_mismatch = false;
        for node in nodes {
            let node_store: BTreeMap<T0, T> = node
                .get_event_store()
                .lock()
                .await
                .events
                .clone()
                .into_values()
                .map(|e| (e.t_zero, e.t))
                .collect();
            assert!(node
                .get_event_store()
                .lock()
                .await
                .events
                .clone()
                .iter()
                .all(|(_, e)| e.state == State::Applied));
            assert_eq!(coordinator_store.len(), node_store.len());
            if coordinator_store != node_store {
                println!("Node: {:?}", node.get_info());
                let mut node_store_iter = node_store.iter();
                for (k, v) in coordinator_store.iter() {
                    if let Some(next) = node_store_iter.next() {
                        if next != (k, v) {
                            println!("Diff: Got {:?}, Expected: {:?}", next, (k, v));
                            println!("Nanos: {:?} | {:?}", next.1 .0.get_nanos(), v.0.get_nanos());
                        }
                    }
                }
                got_mismatch = true;
            }

            assert!(!got_mismatch);
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn database_test() {
        let network = Arc::new(NetworkMock::default());
        let path = "../../tests/database/init_test_db".to_string();
        let (sdx, _) = tokio::sync::mpsc::channel(100);
        let node = Node::new_with_parameters(DieselUlid::generate(), 0, network, Some(path), sdx)
            .await
            .unwrap();
        node.transaction(
            u128::from_be_bytes(DieselUlid::generate().as_byte_array()),
            Vec::from("this is a transaction test"),
        )
        .await
        .unwrap();
        let mut event_store = node.event_store.lock().await;
        let db = event_store.database.clone();
        let all = db.unwrap().read_all().unwrap();
        let db_entry =
            synevi_persistence::event::Event::from_bytes(all.first().cloned().unwrap()).unwrap();
        let ev_entry = event_store.events.first_entry().unwrap();
        let ev_entry = ev_entry.get();

        assert_eq!(&db_entry, ev_entry);
    }
}
