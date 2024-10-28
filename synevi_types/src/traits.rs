use ahash::RandomState;
use serde::Serialize;
use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, Weak},
};
use tokio::sync::mpsc::Receiver;

use crate::{
    types::{Event, Hashes, RecoverDependencies, RecoverEvent, SyneviResult, UpsertEvent},
    Ballot, State, SyneviError, T, T0,
};

pub trait Transaction: std::fmt::Debug + Clone + Send {
    type TxErr: Send + Serialize;
    type TxOk: Send + Serialize;
    fn as_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SyneviError>
    where
        Self: Sized;
}

impl Transaction for Vec<u8> {
    type TxErr = Vec<u8>;
    type TxOk = Vec<u8>;
    fn as_bytes(&self) -> Vec<u8> {
        self.clone()
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self, SyneviError> {
        Ok(bytes)
    }
}

#[async_trait::async_trait]
pub trait Executor: Send + Sync + 'static {
    type Tx: Transaction + Serialize;
    // Executor expects a type with interior mutability
    async fn execute(&self, id: u128, transaction: Self::Tx) -> SyneviResult<Self>;
}

#[async_trait::async_trait]
impl<E> Executor for Arc<E>
where
    E: Executor,
{
    type Tx = E::Tx;
    async fn execute(&self, id: u128, transaction: Self::Tx) -> SyneviResult<Self> {
        self.as_ref().execute(id, transaction).await
    }
}

#[async_trait::async_trait]
impl<E> Executor for Weak<E>
where
    E: Executor,
{
    type Tx = E::Tx;

    async fn execute(&self, id: u128, transaction: Self::Tx) -> SyneviResult<Self> {
        self.upgrade()
            .ok_or_else(|| SyneviError::ArcDropped)?
            .as_ref()
            .execute(id, transaction)
            .await
    }
}

pub type Dependencies = HashSet<T0, RandomState>;

pub trait Store: Send + Sync + Sized + 'static {
    // fn new(node_serial: u16) -> Result<Self, SyneviError>;
    // Initialize a new t0
    fn init_t_zero(&self, node_serial: u16) -> T0;
    // Pre-accept a transaction
    fn pre_accept_tx(
        &self,
        id: u128,
        t_zero: T0,
        transaction: Vec<u8>,
    ) -> Result<(T, Dependencies), SyneviError>;
    // Get the dependencies for a transaction
    fn get_tx_dependencies(&self, t: &T, t_zero: &T0) -> Dependencies;
    // Get the recover dependencies for a transaction
    fn get_recover_deps(&self, t_zero: &T0) -> Result<RecoverDependencies, SyneviError>;
    // Tries to recover an unfinished event from the store
    fn recover_event(
        &self,
        t_zero_recover: &T0,
        node_serial: u16,
    ) -> Result<Option<RecoverEvent>, SyneviError>;
    // Check and update the ballot for a transaction
    // Returns true if the ballot was accepted (current <= ballot)
    fn accept_tx_ballot(&self, t_zero: &T0, ballot: Ballot) -> Option<Ballot>;
    // Update or insert a transaction, returns the hash of the transaction if applied
    fn upsert_tx(&self, upsert_event: UpsertEvent) -> Result<(), SyneviError>;

    fn get_event_state(&self, t_zero: &T0) -> Option<State>;

    fn get_event_store(&self) -> BTreeMap<T0, Event>;
    fn last_applied(&self) -> (T, T0);
    //    fn last_applied_hash(&self) -> Result<(T, [u8; 32]), SyneviError>;

    fn get_event(&self, t_zero: T0) -> Result<Option<Event>, SyneviError>;
    fn get_event_by_id(&self, id: u128) -> Result<Option<Event>, SyneviError>;
    fn get_events_after(
        &self,
        last_applied: T,
    ) -> Result<Receiver<Result<Event, SyneviError>>, SyneviError>;

    //    fn get_and_update_hash(
    //        &self,
    //        t_zero: T0,
    //        execution_hash: [u8; 32],
    //    ) -> Result<Hashes, SyneviError>;

    fn get_or_update_transaction_hash(&self, event: UpsertEvent) -> Result<Hashes, SyneviError>;

    // Increases the max time to be above the specified guard
    // Ensures that the guards t0 will not get a fast path afterwards
    fn inc_time_with_guard(&self, guard: T0) -> Result<(), SyneviError>;
}
