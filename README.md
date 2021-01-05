# events-to-db

Copy event streams to a database table. This will connect to the ActyxOS Event Service, subscribe to event streams, and
push the events to a database (currenty Postgres) for further analysis using SQL.

## Usage

```
USAGE:
    events-to-db [OPTIONS] [subscriptions]

FLAGS:
        --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -h, --db-host <db-host>                         [env: DB_HOST=]  [default: localhost]
    -d, --db-name <db-name>                         [env: DB_NAME=]
    -w, --db-password <db-password>                 [env: DB_PASSWORD]
    -p, --db-port <db-port>                         [env: DB_PORT=]  [default: 5432]
    -u, --db-user <db-user>                         [env: DB_USER=]
    -f, --from-start <from-start>                   [env: FROM_START=]
    -r, --max-batch-records <max-batch-records>     [env: MAX_BATCH_RECORDS=]  [default: 1024]
    -s, --max-batch-seconds <max-batch-seconds>     [env: MAX_BATCH_SECONDS=]  [default: 1]
    -t, --table <table>                             [env: TABLE=]  [default: events]

ARGS:
    <subscriptions>     [env: SUBSCRIPTIONS=]  [default: {}]
```

## Subscriptions

The subscriptions are given as zero or more JSON objects, each of which can have any of the following keys:

- `source`: Source ID
- `semantics`: Event stream semantics
- `name`: Event stream name

Subscriptions are OR'ed together and NULL/nonexistent elements are considered wildcards. Some examples:

- `'{}'` (default) - Subscribe to all events from all sources
- `'{"source":"foo"}'` - Subscribe to all events from source `foo`
- `'{"source":"foo","semantics":"edge.ax.sf.mdc-series"}'` - Subscribe to all events from source `foo` that have the given semantics
- `'{"source":"foo"}' '{"semantics":"bar"}'` - Subscribe to all events from sources `foo` and `bar`

## Environment variables

Aside from the environment variables listed under `events-to-db --help`, there is the following:

- `AX_EVENT_SERVICE_URI`: URL to connect to the Event Service. Default: http://localhost:4454/api/
- `RUST_LOG`: Set to control the log level, as per the [env_logger crate](https://docs.rs/env_logger/0.8.2/env_logger/).
  The default is set to `info`.

## Database schema

The database table will be created if it does not exist (note that no schema migration is done, so if the table already
exists it will be used as-is). For PostgreSQL the table schema is as follows:

```
CREATE TABLE IF NOT EXISTS {} (
    source text not null,      -- Event source ID
    semantics text not null,   -- Event source semantics
    name text not null,        -- Event source name
    seq bigint not null,       -- Event sequence number within its stream
    psn bigint not null,       -- Event offset (named PSN for historical reasons)
    timestamp bigint not null, -- Event wall-clock timestamp in microseconds
    payload jsonb,             -- Event payload
    PRIMARY KEY (source, psn)
)
```

## Notes

- By default, `events-to-db` queries the database for the maximum offset (`psn`) for each source, and subscribes
  starting at the first event from each source. That means that, if you change the subscriptions in a subsequent run,
  it will not backfill any events that were not previously subscribed to. To get around this you can add the `--from-start`
  flag, which will subscribe starting at the beginning of time. Any events that are already in the database (identified
  by `source` and `psn`) will not be inserted again.

- `events-to-db` will batch inserts for better performance. To tune the batch size, use the `--max-batch-records` and
  `--max-batch-seconds` flags

## TODO

- CI integration
- Add databases other than PostgreSQL
- Schema migration
- One thread/DB connection per subscription
