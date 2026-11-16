# Replicated log

UCU Data Engineering, Distributed Systems course. A replicated log with one master
and any number of secondaries.

Each iteration lives in its own branch (`ds-rl-iter-1`, `ds-rl-iter-2`, `ds-rl-iter-3`).

## Iteration 3 (current)

- **Retry**: if a send to a secondary fails (error, or no ACK within the request timeout),
  the master sends the message again. There is no limit on attempts.
  - The pause between attempts grows with each attempt: `retry_delay * attempt`,
    but not more than `max_retry_delay`.
  - Each message to each secondary is retried on its own. A secondary that was down
    gets every message it missed after it comes back.
  - An append waits until its write concern is reached, however long that takes.
    Other appends are not blocked by it.
- **Deduplication**: a secondary keeps the first copy of a message and ignores repeats
  with the same sequence number. It still sends the ACK, so the master stops retrying.
- **Total order**: the same as in iteration 2. A secondary with `[m1, m2, m4]` shows only `[m1, m2]`
  until the retry brings `m3`.
- **Heartbeats**: the master sends a heartbeat to each secondary every `heartbeat_interval`,
  one task per secondary. The status depends on the time since the last successful heartbeat:
  - `healthy` — less than `suspected_after`; one successful heartbeat is enough to get back here;
  - `suspected` — more than `suspected_after`;
  - `unhealthy` — more than `unhealthy_after`.

  The status changes retries: Healthy — the pause described above; Suspected — twice that pause;
  Unhealthy — no sends at all, the master checks the status again every `max_retry_delay`.
  `GET /health` on the master shows the statuses.
- **Quorum append**: the master accepts appends only while a majority of nodes is available
  (`nodes / 2 + 1`, master included). A secondary counts unless it is Unhealthy.
  With 3 nodes, the master and one secondary are enough. Without a quorum the master is
  read-only: every append, with any write concern, gets `503`.

## Iteration 2

- `POST /append` on the master takes a **write concern**: how many nodes must have
  the message before the master replies.
  - `1` — master only
  - `2` — master + 1 secondary
  - `3` — master + 2 secondaries
- The master sends the message to all secondaries **in parallel**. It replies as soon as
  the write concern is reached. Replication to the other secondaries goes on in the background.
- **Total order**: the master gives each message a sequence number (1, 2, 3, ...).
  Every node lists messages in that order. A secondary shows a message only when it has all earlier messages.
- The link is assumed perfect: no lost messages, no retries.

## Iteration 1

- `POST` on the master appends a message to its in-memory log.
- The master sends the message to all secondaries **in parallel**.
- The `POST` returns only after **every** secondary has sent an ACK (blocking replication).
  In iteration 2 this is `write_concern` = 3.
- The link is assumed perfect: no lost messages, no retries.

## Run with Docker

```sh
docker compose up --build
```

To see retries in practice, stop a secondary (the iteration 3 acceptance test):

```sh
docker compose stop secondary-2
curl -X POST localhost:28080/append -H 'content-type: application/json' -d '{"value": "m1", "write_concern": 1}'  # returns at once
curl -X POST localhost:28080/append -H 'content-type: application/json' -d '{"value": "m2", "write_concern": 2}'  # returns at once
curl -X POST localhost:28080/append -H 'content-type: application/json' -d '{"value": "m3", "write_concern": 3}'  # waits
# in another terminal:
curl -X POST localhost:28080/append -H 'content-type: application/json' -d '{"value": "m4", "write_concern": 1}'  # returns at once
docker compose start secondary-2     # the m3 append returns after the next retry
curl localhost:28082/list            # secondary-2 has [m1, m2, m3, m4]
```

`docker compose pause secondary-2` / `unpause` works too: a paused secondary does not answer,
so the master retries after the request timeout.

Delays, reordering and other network faults live only in the test harness, not in the service code.

| Node        | URL                    |
|-------------|------------------------|
| master      | http://localhost:28080 |
| secondary-1 | http://localhost:28081 |
| secondary-2 | http://localhost:28082 |

Log level is set with `RUST_LOG` (default `info`).

Retry settings of the master, as environment variables (also read by `docker compose`):

| Variable             | Default | Meaning                                          |
|----------------------|---------|--------------------------------------------------|
| `REQUEST_TIMEOUT_MS` | 5000    | how long to wait for an ACK before sending again |
| `RETRY_DELAY_MS`     | 1000    | pause after the first failed attempt; grows with each attempt |
| `MAX_RETRY_DELAY_MS` | 30000   | upper limit for the pause                        |
| `HEARTBEAT_INTERVAL_MS` | 1000 | how often to send a heartbeat to each secondary  |
| `SUSPECTED_AFTER_MS` | 3000    | no successful heartbeat for this long -> suspected |
| `UNHEALTHY_AFTER_MS` | 10000   | no successful heartbeat for this long -> unhealthy |

To see heartbeats and quorum, stop both secondaries and watch the statuses:

```sh
docker compose stop secondary-1 secondary-2
curl localhost:28080/health   # {"1":"suspected","2":"suspected"} after ~3s, "unhealthy" after ~10s
curl -X POST localhost:28080/append -H 'content-type: application/json' -d '{"value": "m1", "write_concern": 1}'  # 503 once both are unhealthy
docker compose start secondary-1
curl localhost:28080/health   # {"1":"healthy","2":"unhealthy"}; appends work again
```

```sh
REQUEST_TIMEOUT_MS=2000 docker compose up
```

## HTTP API

### Master

```sh
# Append. Blocks until `write_concern` nodes have the message
# (retrying failed secondaries as long as needed). Returns 201.
curl -X POST localhost:28080/append \
  -H 'content-type: application/json' \
  -d '{"value": "m1", "write_concern": 2}'

# List
curl localhost:28080/list

# Status of each secondary, by id (position in SECONDARIES, from 1)
curl localhost:28080/health   # {"1":"healthy","2":"suspected"}
```

Errors, as `{"error": "..."}`:
- `400` — `write_concern` is below 1 or above the number of nodes;
- `503` — no quorum, the master is read-only.

A secondary that cannot be reached is not an error: the master retries it, and the append waits.

### Secondary

```sh
curl localhost:28081/list
```

`POST /replicate` and `POST /heartbeat` are internal. Only the master calls them.

## Run without Docker

```sh
cargo run -- secondary --port 8081
cargo run -- secondary --port 8082
cargo run -- master --port 8080 --secondaries http://localhost:8081,http://localhost:8082
```

Every option can also be set with an environment variable:
`PORT`, `SECONDARIES`, `REQUEST_TIMEOUT_MS`, `RETRY_DELAY_MS`, `MAX_RETRY_DELAY_MS`,
`HEARTBEAT_INTERVAL_MS`, `SUSPECTED_AFTER_MS`, `UNHEALTHY_AFTER_MS`.
See `cargo run -- master --help`.

## Tests

```sh
cargo test
```

The tests do not use the network. They build master and secondary nodes in memory,
and connect them through a test harness instead of the HTTP client.
For each secondary, the harness can:
- delay requests (master -> secondary) and responses (ACK, secondary -> master);
- hold the next N requests and deliver them in reverse order;
- drop requests (the secondary never sees them) or responses (the secondary stores the message, the ACK is lost);
- stop and start the secondary (every request fails at once);
- count sent requests (with retries) and requests the secondary handled (with duplicates).

A delayed request still reaches the secondary after the master's timeout,
so a retry and the late copy both arrive. This is how deduplication is tested.

Tokio paused time is used, so delays and timeouts do not slow down the tests.

| File                     | What it checks                                              |
|--------------------------|-------------------------------------------------------------|
| `tests/replication.rs`   | write concern = all nodes: parallel sends, wait for all ACKs |
| `tests/write_concern.rs` | write concern 1/2/3, bad values, eventual consistency, acceptance test |
| `tests/ordering.rs`      | total order: reordered messages, gaps, concurrent appends   |
| `tests/retry.rs`         | retry after lost messages and ACKs, deduplication, total order with retries, acceptance test |
| `tests/heartbeat.rs`     | status changes, retry pause by status, no sends to an unhealthy secondary |
| `tests/quorum.rs`        | read-only without quorum, suspected counts, quorum comes back |

## Code layout

```
src/
├── main.rs          # CLI (clap), logging, starts a master or a secondary
├── core/            # log and replication logic, no HTTP
│   ├── mod.rs       # Client and Request traits, Error
│   ├── message.rs   # Message, Replicate
│   ├── master.rs
│   └── secondary.rs
└── http/            # HTTP implementation of the core traits
    ├── client.rs    # core::Client over reqwest
    └── server.rs    # axum routes for master and secondary
tests/
├── harness/         # in-memory core::Client with delays, reordering, drops, stopped nodes
├── replication.rs
├── write_concern.rs
├── ordering.rs
├── retry.rs
├── heartbeat.rs
└── quorum.rs
```

The master knows only the `core::Client` trait. In production it is the HTTP client.
In tests it is the harness.
