use crate::postgres::Postgres;
use actyxos_sdk::event_service::{EventService, Subscription};
use anyhow::Result;
use backtrace::Backtrace;
use db::{Db, DbConnection};
use env_logger::Env;
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
    about = "Insert events into a Postgres database.\nEnvironment variables:\n  AX_EVENT_SERVICE_URI: URL to connect to the Event Service. Default: http://localhost:4454/api/"
)]
struct Opt {
    #[structopt(long, short = "f", takes_value = false, env = "FROM_START")]
    from_start: bool,
    #[structopt(long, short = "r", env, default_value = "1024")]
    max_batch_records: usize,
    #[structopt(long, short = "s", env, default_value = "1")]
    max_batch_seconds: u64,
    #[structopt(long, short = "d", env)]
    db_name: String,
    #[structopt(long, short = "h", env, default_value = "localhost")]
    db_host: String,
    #[structopt(long, short = "p", env, default_value = "5432")]
    db_port: u16,
    #[structopt(long, short = "u", env)]
    db_user: String,
    #[structopt(long, short = "w", env, hide_env_values = true)]
    db_password: String,
    #[structopt(long, short = "t", env, default_value = "events")]
    table: String,
    #[structopt(
        about = "Subscriptions to subscribe to",
        env,
        parse(try_from_str = parse_subscriptions),
        default_value = "{}"
    )]
    subscriptions: Vec<Subscription>,
}

fn parse_subscriptions(subs: &str) -> Result<Subscription, serde_json::Error> {
    serde_json::from_str(subs)
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
pub async fn main() -> Result<()> {
    env_logger::Builder::from_env(Env::default().default_filter_or("info")).init();
    init_panic_hook();
    let opt: Opt = Opt::from_args();

    info!("Subscribing to: {:?}", opt.subscriptions);

    let pg = Postgres::new(
        opt.db_host,
        opt.db_port,
        opt.db_user,
        opt.db_password,
        opt.db_name,
        opt.table,
    );

    run_pipeline(
        Box::new(pg),
        opt.subscriptions,
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
