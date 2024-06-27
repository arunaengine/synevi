use crate::error::WaitError;
use crate::event_store::{Event, EventStore};
use crate::utils::{from_dependency, T, T0};
use anyhow::Result;
use bytes::Bytes;
use consensus_transport::consensus_transport::*;
use consensus_transport::replica::Replica;
use monotime::MonoTime;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tracing::instrument;

#[derive(Debug)]
pub struct ReplicaConfig {
    pub event_store: Arc<Mutex<EventStore>>,
}

#[async_trait::async_trait]
impl Replica for ReplicaConfig {
    #[instrument(level = "trace", skip(self))]
    async fn pre_accept(&self, request: PreAcceptRequest) -> Result<PreAcceptResponse> {
        let (deps, t) = self.event_store.lock().await.pre_accept(request).await?;

        Ok(PreAcceptResponse {
            timestamp: t.into(),
            dependencies: deps,
        })
    }

    #[instrument(level = "trace", skip(self))]
    async fn accept(&self, request: AcceptRequest) -> Result<AcceptResponse> {
        let t_zero = T0(MonoTime::try_from(request.timestamp_zero.as_slice())?);
        let t = T(MonoTime::try_from(request.timestamp.as_slice())?);

        let (tx, _) = watch::channel((State::Accepted, t));
        self.event_store
            .lock()
            .await
            .upsert(Event {
                t_zero,
                t,
                state: tx,
                event: request.event.into(),
                dependencies: from_dependency(request.dependencies.clone())?,
            })
            .await;

        // Should this be saved to event_store before it is finalized in commit?
        // TODO: Check if the deps do not need unification
        let dependencies = self
            .event_store
            .lock()
            .await
            .get_dependencies(&t, &t_zero)
            .await;
        Ok(AcceptResponse { dependencies })
    }

    #[instrument(level = "trace", skip(self))]
    async fn commit(&self, request: CommitRequest) -> Result<CommitResponse> {
        let t_zero = T0(MonoTime::try_from(request.timestamp_zero.as_slice())?);
        let t = T(MonoTime::try_from(request.timestamp.as_slice())?);
        let dependencies = from_dependency(request.dependencies.clone())?;
        let (tx, _) = watch::channel((State::Commited, t));

        self.event_store
            .lock()
            .await
            .upsert(Event {
                t_zero,
                t,
                state: tx,
                event: request.event.clone().into(),
                dependencies: dependencies.clone(),
            })
            .await;

        let mut handles = self
            .event_store
            .lock()
            .await
            .create_wait_handles(dependencies.clone(), t)
            .await?;

        while let Some(x) = handles.0.join_next().await {
            match x {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => match e {
                    WaitError::Timeout(t0) => {
                        // Wait for a node specific timeout
                        // Spawn recovery to t0
                    }
                    WaitError::SenderClosed => {
                        tracing::error!("Sender of transaction got closed")
                    }
                },
                Err(_) => {
                    tracing::error!("Join error")
                }
            }
        }

        Ok(CommitResponse {})
    }

    #[instrument(level = "trace", skip(self))]
    async fn apply(&self, request: ApplyRequest) -> Result<ApplyResponse> {
        let transaction: Bytes = request.event.into();

        let t_zero = T0(MonoTime::try_from(request.timestamp_zero.as_slice())?);
        let t = T(MonoTime::try_from(request.timestamp.as_slice())?);

        let dependencies = from_dependency(request.dependencies.clone())?;

        let mut handles = self
            .event_store
            .lock()
            .await
            .create_wait_handles(dependencies.clone(), t)
            .await?;

        while let Some(x) = handles.0.join_next().await {
            match x {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => match e {
                    WaitError::Timeout(t0) => {
                        // Wait for a node specific timeout
                        // Spawn recovery to t0
                    }
                    WaitError::SenderClosed => {
                        tracing::error!("Sender of transaction got closed")
                    }
                },
                Err(_) => {
                    tracing::error!("Join error")
                }
            }
        }
        let (tx, _) = watch::channel((State::Applied, t));
        self.event_store
            .lock()
            .await
            .upsert(Event {
                t_zero,
                t,
                state: tx,
                event: transaction,
                dependencies,
            })
            .await;

        Ok(ApplyResponse {})
    }

    #[instrument(level = "trace", skip(self))]
    async fn recover(&self, _request: RecoverRequest) -> Result<RecoverResponse> {
        todo!()
    }
}
