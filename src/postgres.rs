use crate::db::{Db, DbConnection};
use actyxos_sdk::{
    event::{Event, SourceId},
    Offset, OffsetMap, Payload,
};
use anyhow::Result;
use async_trait::async_trait;
use serde_cbor::error::Error;
use serde_json::Value;
use std::{collections::BTreeMap, convert::TryFrom, time::Instant};
use tokio_postgres::{types::Type, NoTls};
use tokio_postgres::{Client, Statement};
use tracing::{debug, error, info};

#[derive(Debug, PartialEq)]
pub struct DbEventVec<'a> {
    pub sources: Vec<&'a str>,
    pub semantics: Vec<&'a str>,
    pub names: Vec<&'a str>,
    pub lamports: Vec<i64>,
    pub offsets: Vec<i64>,
    pub timestamps: Vec<i64>,
    pub payloads: Vec<Value>,
}

impl<'a> DbEventVec<'a> {
    pub fn empty(capacity: usize) -> DbEventVec<'a> {
        DbEventVec {
            sources: Vec::with_capacity(capacity),
            semantics: Vec::with_capacity(capacity),
            names: Vec::with_capacity(capacity),
            lamports: Vec::with_capacity(capacity),
            offsets: Vec::with_capacity(capacity),
            timestamps: Vec::with_capacity(capacity),
            payloads: Vec::with_capacity(capacity),
        }
    }
}

impl<'a> From<&'a [Event<Payload>]> for DbEventVec<'a> {
    fn from(events: &'a [Event<Payload>]) -> DbEventVec<'a> {
        let mut rows = DbEventVec::empty(events.len());
        for e in events {
            let ev: Result<Event<Value>, Error> = e.extract();
            match ev {
                Ok(ev) => {
                    rows.sources.push(e.stream.source.as_str());
                    rows.semantics.push(e.stream.semantics.as_str());
                    rows.names.push(e.stream.name.as_str());
                    rows.lamports.push(e.lamport.as_i64());
                    rows.offsets.push((e.offset - Offset::ZERO) as i64);
                    rows.timestamps.push(e.timestamp.as_i64());
                    rows.payloads.push(ev.payload);
                }
                Err(err) => {
                    error!(
                        "Error parsing payload as JSON: {:?}.\n     Event: {:?}",
                        err, e
                    );
                }
            }
        }
        rows
    }
}

pub struct PostgresConnection {
    client: Client,
    insert_stmt: Statement,
    table: String,
}

#[derive(Debug)]
pub struct Postgres {
    host: String,
    port: u16,
    user: String,
    password: String,
    db_name: String,
    table: String,
}

impl Postgres {
    pub fn new(
        host: String,
        port: u16,
        user: String,
        password: String,
        db_name: String,
        table: String,
    ) -> Postgres {
        Postgres {
            host,
            port,
            user,
            password,
            db_name,
            table,
        }
    }
}

#[async_trait]
impl Db<PostgresConnection> for Postgres {
    async fn connect(&self) -> Result<PostgresConnection> {
        info!("Connecting to database");
        let conn_str = format!(
            "host={} port={} user={} password={} dbname={}",
            self.host, self.port, self.user, self.password, self.db_name
        );
        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
        tokio::spawn(Box::pin(connection));
        info!("Successfully connected to database");

        info!("Creating table {} if it does not exist", self.table);
        let create_table_sql = format!(
            r#"
                CREATE TABLE IF NOT EXISTS {} (
                    source text not null,
                    semantics text not null,
                    name text not null,
                    seq bigint not null,
                    psn bigint not null,
                    timestamp bigint not null,
                    payload jsonb,
                    PRIMARY KEY (source, psn)
                )
            "#,
            self.table
        );
        let create_table_statement = client.prepare(&*create_table_sql).await?;
        client.execute(&create_table_statement, &[]).await?;
        info!("Created table");

        let sql = format!(
            r#"INSERT INTO {} (source, semantics, name, seq, psn, timestamp, payload)
                 SELECT * FROM unnest($1,$2,$3,$4,$5,$6,$7)
                 ON CONFLICT DO NOTHING"#,
            &self.table
        );
        let types = vec![
            Type::TEXT_ARRAY,
            Type::TEXT_ARRAY,
            Type::TEXT_ARRAY,
            Type::INT8_ARRAY,
            Type::INT8_ARRAY,
            Type::INT8_ARRAY,
            Type::JSONB_ARRAY,
        ];
        let insert_stmt = client.prepare_typed(&sql, &types).await?;

        let conn = PostgresConnection {
            client,
            insert_stmt,
            table: self.table.clone(),
        };

        Ok(conn)
    }
}

#[async_trait]
impl DbConnection for PostgresConnection {
    async fn insert(&self, items: Vec<Event<Payload>>) -> Result<()> {
        let start = Instant::now();

        let num_rows = items.len();
        debug!("Preparing {} events", num_rows);
        let rows_suffix = if num_rows > 1 { "s" } else { "" };
        let rows = DbEventVec::from(&*items);

        let mut sources = rows.sources.clone();
        sources.sort_unstable();
        sources.dedup();
        let sources_suffix = if sources.len() > 1 { "s" } else { "" };
        let sources = sources.join(", ");

        debug!(
            "About to write {} record{} into DB. Source{}: {}",
            num_rows, rows_suffix, sources_suffix, sources,
        );
        self.client
            .execute(
                &self.insert_stmt,
                // Make sure that the order of the fields here matches the INSERT statement in the connect() method above
                &[
                    &rows.sources,
                    &rows.semantics,
                    &rows.names,
                    &rows.lamports,
                    &rows.offsets,
                    &rows.timestamps,
                    &rows.payloads,
                ],
            )
            .await?;

        let elapsed = start.elapsed().as_millis();
        info!(
            "Wrote {} record{} in {} ms ({} records/sec). Source{}: {}",
            num_rows,
            rows_suffix,
            elapsed,
            num_rows as u128 * 1_000 / elapsed,
            sources_suffix,
            sources,
        );

        Ok(())
    }

    async fn get_offsets(&self) -> Result<OffsetMap> {
        info!("Querying initial offset map");
        let sql = format!(
            "SELECT source, MAX(psn) FROM {} GROUP BY source",
            self.table
        );
        let query = self.client.prepare(&*sql).await?;
        let rows = self.client.query(&query, &[]).await?;

        let offsets: BTreeMap<_, _> = rows
            .into_iter()
            .map(|row| {
                let source: String = row.get(0);
                let offset: i64 = row.get(1);
                (
                    SourceId::new(source).unwrap(),
                    Offset::try_from(offset as u64).unwrap(),
                )
            })
            .collect();

        Ok(OffsetMap::from(offsets))
    }
}

#[cfg(test)]
mod tests {
    use actyxos_sdk::{event::StreamInfo, LamportTimestamp};
    use std::convert::TryInto;

    use super::*;
    #[test]
    fn convert_events_to_db_event_vec() {
        let events = vec![
            Event {
                lamport: LamportTimestamp::new(10),
                stream: StreamInfo {
                    semantics: "foo.semantics".try_into().unwrap(),
                    name: "foo.name".try_into().unwrap(),
                    source: "foo".try_into().unwrap(),
                },
                timestamp: 12.try_into().unwrap(),
                offset: 11.try_into().unwrap(),
                payload: Payload::from_json_str(r#"{"foo":"foo"}"#).unwrap(),
            },
            Event {
                lamport: LamportTimestamp::new(20),
                stream: StreamInfo {
                    semantics: "bar.semantics".try_into().unwrap(),
                    name: "bar.name".try_into().unwrap(),
                    source: "bar".try_into().unwrap(),
                },
                timestamp: 22.try_into().unwrap(),
                offset: 21.try_into().unwrap(),
                payload: Payload::from_json_str(r#"{"bar":"bar"}"#).unwrap(),
            },
        ];

        let expected = DbEventVec {
            sources: vec!["foo", "bar"],
            semantics: vec!["foo.semantics", "bar.semantics"],
            names: vec!["foo.name", "bar.name"],
            lamports: vec![10, 20],
            offsets: vec![11, 21],
            timestamps: vec![12, 22],
            payloads: vec![
                serde_json::from_str(r#"{"foo":"foo"}"#).unwrap(),
                serde_json::from_str(r#"{"bar":"bar"}"#).unwrap(),
            ],
        };

        let actual: DbEventVec = (&events).into();

        assert_eq!(actual, expected);
    }
}
