use actyxos_sdk::{
    event::{Event, FishName, OffsetMap, Semantics, SourceId},
    Offset, Payload, TimeStamp,
};
use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize, Deserialize, Debug)]
pub struct DbEvent {
    pub source: SourceId,
    pub semantics: Semantics,
    pub name: FishName,
    pub seq: u64,
    pub psn: Offset,
    pub timestamp: TimeStamp,
    pub payload: Payload,
}

impl From<Event<Payload>> for DbEvent {
    fn from(e: Event<Payload>) -> Self {
        DbEvent {
            source: e.stream.source,
            semantics: e.stream.semantics,
            name: e.stream.name,
            seq: 0,
            psn: e.offset,
            timestamp: e.timestamp,
            payload: e.payload,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct DbEventVec {
    pub source: Vec<String>,
    pub semantics: Vec<String>,
    pub name: Vec<String>,
    pub seq: Vec<i64>,
    pub psn: Vec<i64>,
    pub timestamp: Vec<i64>,
    pub payload: Vec<Value>,
}

impl DbEventVec {
    pub fn empty(capacity: usize) -> DbEventVec {
        DbEventVec {
            source: Vec::with_capacity(capacity),
            semantics: Vec::with_capacity(capacity),
            name: Vec::with_capacity(capacity),
            seq: Vec::with_capacity(capacity),
            psn: Vec::with_capacity(capacity),
            timestamp: Vec::with_capacity(capacity),
            payload: Vec::with_capacity(capacity),
        }
    }
}

impl From<&Vec<DbEvent>> for DbEventVec {
    fn from(events: &Vec<DbEvent>) -> DbEventVec {
        let mut rows = DbEventVec::empty(events.len());
        events.iter().for_each(|e: &DbEvent| {
            rows.source.push(e.source.as_str().to_string());
            rows.semantics.push(e.semantics.as_str().to_string());
            rows.name.push(e.name.as_str().to_string());
            rows.seq.push(e.seq as i64);
            rows.psn.push((e.psn - Offset::ZERO) as i64);
            rows.timestamp.push(e.timestamp.as_i64());
            rows.payload.push(e.payload.json_value());
        });
        rows
    }
}

#[async_trait]
pub trait Db<C: DbConnection> {
    async fn connect(&self) -> Result<C>;
}
#[async_trait]
pub trait DbConnection {
    async fn insert(&self, items: Vec<DbEvent>) -> Result<()>;
    async fn get_offsets(&self) -> Result<OffsetMap>;
}

#[test]
fn convert_db_event_to_db_event_vec() {
    let events = vec![
        DbEvent {
            source: "foo".try_into().unwrap(),
            semantics: "foo.semantics".try_into().unwrap(),
            name: "foo.name".try_into().unwrap(),
            seq: 10,
            psn: 11.try_into().unwrap(),
            timestamp: 12.try_into().unwrap(),
            payload: serde_json::from_str(r#"{"foo":"foo"}"#).unwrap(),
        },
        DbEvent {
            source: "bar".try_into().unwrap(),
            semantics: "bar.semantics".try_into().unwrap(),
            name: "bar.name".try_into().unwrap(),
            seq: 20,
            psn: 21.try_into().unwrap(),
            timestamp: 22.try_into().unwrap(),
            payload: serde_json::from_str(r#"{"bar":"bar"}"#).unwrap(),
        },
    ];

    let expected = DbEventVec {
        source: vec!["foo".try_into().unwrap(), "bar".try_into().unwrap()],
        semantics: vec![
            "foo.semantics".try_into().unwrap(),
            "bar.semantics".try_into().unwrap(),
        ],
        name: vec![
            "foo.name".try_into().unwrap(),
            "bar.name".try_into().unwrap(),
        ],
        seq: vec![10, 20],
        psn: vec![11.try_into().unwrap(), 21.try_into().unwrap()],
        timestamp: vec![12.try_into().unwrap(), 22.try_into().unwrap()],
        payload: vec![
            serde_json::from_str(r#"{"foo":"foo"}"#).unwrap(),
            serde_json::from_str(r#"{"bar":"bar"}"#).unwrap(),
        ],
    };

    let actual: DbEventVec = (&events).into();

    assert_eq!(actual, expected);
}
