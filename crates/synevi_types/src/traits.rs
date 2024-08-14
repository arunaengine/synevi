use crate::ConsensusError;
use anyhow::Result;

pub trait Transaction: std::fmt::Debug + Clone + Send {
    fn as_bytes(&self) -> Vec<u8>;
    fn from_bytes(bytes: Vec<u8>) -> Result<Self>
    where
        Self: Sized;
}

impl Transaction for Vec<u8> {
    fn as_bytes(&self) -> Vec<u8> {
        self.clone()
    }

    fn from_bytes(bytes: Vec<u8>) -> Result<Self> {
        Ok(bytes)
    }
}

#[async_trait::async_trait]
pub trait Executor: Send + Sync + 'static {
    type Tx: Transaction;
    type TxOk: Send;
    type TxErr: Send;
    // Executor expects a type with interior mutability
    async fn execute(
        &self,
        transaction: Self::Tx,
    ) -> Result<Self::TxOk, ConsensusError<Self::TxErr>>;
}
