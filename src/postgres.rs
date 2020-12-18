use std::{collections::BTreeMap, convert::TryFrom, time::Instant};

use actyxos_sdk::{event::SourceId, Offset, OffsetMap};
use anyhow::Result;
use async_trait::async_trait;
use tokio_postgres::{types::Type, NoTls, Row};
use tokio_postgres::{Client, Statement};
use tracing::{debug, info};

use crate::db::{Db, DbConnection, DbEvent, DbEventVec};

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
        let create_table_statement = client
            .prepare(&*create_table_sql)
            .await
            .expect("Could not prepare CREATE TABLE statement");
        client
            .execute(&create_table_statement, &[])
            .await
            .expect("Could not CREATE TABLE");
        info!("Created table");

        let sql = format!(
            r#"INSERT INTO {} (source, semantics, name, seq, psn, timestamp, payload)
                 SELECT * FROM unnest($1,$2,$3,$4,$5,$6,$7)"#,
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
    async fn insert(&self, items: Vec<DbEvent>) -> Result<()> {
        let start = Instant::now();

        debug!("Preparing {} events", items.len());
        let rows: DbEventVec = (&items).into();

        debug!("About to write {} records into DB", items.len());
        self.client
            .execute(
                &self.insert_stmt,
                &[
                    &rows.source,
                    &rows.semantics,
                    &rows.name,
                    &rows.seq,
                    &rows.psn,
                    &rows.timestamp,
                    &rows.payload,
                ],
            )
            .await?;

        let elapsed = start.elapsed().as_millis() as usize;
        info!(
            "Wrote {} records into DB in {} ms ({} rows/sec)",
            items.len(),
            elapsed,
            (items.len() as f64 / (elapsed as f64 / 1_000.0)) as u64
        );

        Ok(())
    }

    async fn get_offsets(&self) -> Result<OffsetMap> {
        info!("Querying initial offset map map");
        let sql = format!(
            "SELECT source, MAX(psn) FROM {} GROUP BY source",
            self.table
        );
        let query = self
            .client
            .prepare(&*sql)
            .await
            .expect("Could not prepare SELECT for getting the PSN map");

        let rows: Vec<Row> = self
            .client
            .query(&query, &[])
            .await
            .expect("Could not execute SELECT to get initial PSN map");

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
