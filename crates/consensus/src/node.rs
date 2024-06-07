use crate::coordinator::{Coordinator, Member, MemberInfo};
use crate::event_store::EventStore;
use crate::replica::Replica;
use anyhow::Result;
use bytes::Bytes;
use consensus_transport::consensus_transport::consensus_transport_server::ConsensusTransportServer;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::task::JoinSet;
use tonic::transport::{Channel, Server};
use tracing::instrument;

pub struct Node {
    name: Arc<String>,
    members: Vec<Arc<Member>>,
    event_store: Arc<EventStore>,
    join_set: JoinSet<Result<()>>,
}

impl Node {
    pub async fn new_with_config() -> Self {
        todo!()
    }

    #[instrument(level = "trace")]
    pub async fn new_with_parameters(name: String, socket_addr: SocketAddr) -> Self {
        let node_name = Arc::new(name);
        let node_name_clone = node_name.clone();
        let event_store = Arc::new(EventStore::init());
        let event_store_clone = event_store.clone();
        let mut join_set = JoinSet::new();
        join_set.spawn(async move {
            let builder = Server::builder().add_service(ConsensusTransportServer::new(Replica {
                node: node_name_clone,
                event_store: event_store_clone,
            }));
            builder.serve(socket_addr).await?;
            Ok(())
        });

        // If no config / persistence -> default
        Node {
            name: node_name,
            members: vec![],
            event_store,
            join_set,
        }
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn run(&mut self) -> Result<()> {
        self.join_set
            .join_next()
            .await
            .ok_or(anyhow::anyhow!("Empty joinset"))???;
        Ok(())
    }


    #[instrument(level = "trace", skip(self))]
    pub async fn do_stuff(&self) -> Result<()> {
        Ok(())
    }


    #[instrument(level = "trace", skip(self))]
    pub async fn add_member(&mut self, member: MemberInfo) -> Result<()> {
        let channel = Channel::from_shared(member.host.clone())?.connect().await?;
        self.members.push(Arc::new(Member {
            info: member,
            channel,
        }));

        Ok(())
    }

    #[instrument(level = "trace", skip(self))]
    pub async fn transaction(&self, transaction: Bytes) -> Result<()> {
        let mut coordinator = Coordinator::new(
            self.name.clone(),
            self.members.clone(),
            self.event_store.clone(),
        );
        coordinator.transaction(transaction).await
    }
}
