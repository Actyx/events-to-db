use actyxos_sdk::{
    event::{Event, OffsetMap},
    Payload,
};
use anyhow::Result;
use async_trait::async_trait;

#[async_trait]
pub trait Db<C: DbConnection> {
    async fn connect(&self) -> Result<C>;
}
#[async_trait]
pub trait DbConnection {
    async fn insert(&self, items: Vec<Event<Payload>>) -> Result<()>;
    async fn get_offsets(&self) -> Result<OffsetMap>;
}
