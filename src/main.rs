use crate::postgres::Postgres;
use actyxos_sdk::event_service::{EventService, Subscription};
use anyhow::Result;
use backtrace::Backtrace;
use db::{Db, DbConnection, DbEvent};
use futures::{future::FutureExt, stream::StreamExt};
use futures_batch::ChunksTimeoutStreamExt;
use serde::{Deserialize, Serialize};
use std::time::Duration;
use structopt::StructOpt;
use tokio_compat_02::FutureExt as FutureExt02;
use tracing::*;

mod db;
mod postgres;

#[derive(StructOpt, Debug, Serialize, Deserialize)]
#[structopt(
    about = "Insert events into a Postgres database.\nEnvironment variables:\n  PGPASSWORD: PostgreSQL password, if not provided with --password/-p\n  AX_EVENT_SERVICE_URI: URL to connect to the Event Service. Default: http://localhost:4454/api/"
)]
struct Opt {
    #[structopt(long, short = "f", env = "FROM_START")]
    from_start: bool,
    #[structopt(long, short = "w", env = "PGPASSWORD", hide_env_values = true)]
    password: Option<String>,
    #[structopt(long, short = "r", env = "MAX_BATCH_RECORDS", default_value = "1024")]
    max_batch_records: usize,
    #[structopt(long, short = "s", env = "MAX_BATCH_SECONDS", default_value = "1")]
    max_batch_seconds: u64,
    #[structopt(long, short = "d", env = "DB_NAME")]
    db_name: String,
    #[structopt(long, short = "h", env = "DB_HOST", default_value = "localhost")]
    host: String,
    #[structopt(long, short = "p", env = "DB_PORT", default_value = "5432")]
    port: u16,
    #[structopt(long, short = "u", env = "DB_USER")]
    username: String,
    #[structopt(long, short = "t", env = "DB_TABLE", default_value = "events")]
    table: String,
    #[structopt(
        about = "Subscription set to listen to",
        env = "SUBSCRIPTIONS",
        default_value = "[{}]"
    )]
    subscriptions: String,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
pub async fn main() -> Result<()> {
    env_logger::init();
    init_panic_hook();
    let opt: Opt = Opt::from_args();

    let subscriptions: Vec<Subscription> =
        serde_json::from_str(opt.subscriptions.as_str()).unwrap();
    info!("Subscribing to: {:?}", subscriptions);

    let pg = Postgres::new(
        opt.host,
        opt.port,
        opt.username,
        opt.password.unwrap(),
        opt.db_name,
        opt.table,
    );

    run_pipeline(
        Box::new(pg),
        subscriptions,
        opt.max_batch_records,
        opt.max_batch_seconds,
    )
    .compat()
    .await
}

async fn run_pipeline<C: DbConnection + 'static>(
    db: Box<dyn Db<C>>,
    subscriptions: Vec<Subscription>,
    max_batch_records: usize,
    max_batch_seconds: u64,
) -> Result<()> {
    let event_service = EventService::default();
    debug!("Connected to EventService");

    let store_offsets = event_service.get_offsets().await?;

    let db = db.connect().await?;
    let db_offsets = db.get_offsets().await?;
    info!("Offset map from database: {:?}", db_offsets);
    info!("Offset map from store:    {:?}", store_offsets);

    info!(
        "Database has {} events. Store has {} events.",
        &db_offsets.size(),
        &store_offsets.size()
    );

    event_service
        .subscribe_from(db_offsets, subscriptions)
        .await?
        .map(|e| -> DbEvent { e.into() })
        .chunks_timeout(max_batch_records, Duration::new(max_batch_seconds, 0))
        .for_each(|chunk| db.insert(chunk).map(|x| x.unwrap()))
        .await;

    Ok(())
}

/// sets up a panic hook that dumps all available info and exits the process with a non-zero exit code.
///
/// the panic hook is a global, but calling this method multiple times is fine
fn init_panic_hook() {
    std::panic::set_hook(Box::new(|info| {
        // the backtrace library is the same lib that produces the dumps in std lib.
        let backtrace = Backtrace::new();

        // formatting code inspired by log-panics
        let thread = std::thread::current();
        let thread = thread.name().unwrap_or("unnamed");

        let msg = match info.payload().downcast_ref::<&'static str>() {
            Some(s) => *s,
            None => match info.payload().downcast_ref::<String>() {
                Some(s) => &**s,
                None => "Box<Any>",
            },
        };

        match info.location() {
            Some(location) => {
                eprintln!(
                    "thread '{}' panicked at '{}': {}:{}{:?}",
                    thread,
                    msg,
                    location.file(),
                    location.line(),
                    backtrace
                );
            }
            None => eprintln!("thread '{}' panicked at '{}'{:?}", thread, msg, backtrace),
        }
        std::process::exit(3);
    }));
}
