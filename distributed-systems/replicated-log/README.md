# Replicated log

UCU Data Engineering, Distributed Systems course. A replicated log with one master
and any number of secondaries.

Each iteration lives in its own branch (`ds-rl-iter-1`, `ds-rl-iter-2`, `ds-rl-iter-3`).

## Iteration 2 (current)

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

To see write concern in practice, pause a secondary:

```sh
docker compose pause secondary-2
curl -X POST localhost:28080/append -H 'content-type: application/json' -d '{"value": "m1", "write_concern": 2}'  # returns at once
curl -X POST localhost:28080/append -H 'content-type: application/json' -d '{"value": "m2", "write_concern": 3}'  # waits
curl localhost:28081/list            # secondary-1 already has m1 and m2
docker compose unpause secondary-2   # the second append now returns
curl localhost:28082/list            # secondary-2 has caught up
```

Delays, reordering and other network faults live only in the test harness, not in the service code.

| Node        | URL                    |
|-------------|------------------------|
| master      | http://localhost:28080 |
| secondary-1 | http://localhost:28081 |
| secondary-2 | http://localhost:28082 |

Log level is set with `RUST_LOG` (default `info`).

## HTTP API

### Master

```sh
# Append. Blocks until `write_concern` nodes have the message. Returns 201.
curl -X POST localhost:28080/append \
  -H 'content-type: application/json' \
  -d '{"value": "m1", "write_concern": 2}'

# List
curl localhost:28080/list
```

Errors, as `{"error": "..."}`:
- `400` — `write_concern` is below 1 or above the number of nodes;
- `502` — a secondary needed for the write concern cannot be reached.

### Secondary

```sh
curl localhost:28081/list
```

`POST /replicate` is internal. Only the master calls it.

## Run without Docker

```sh
cargo run -- secondary --port 8081
cargo run -- secondary --port 8082
cargo run -- master --port 8080 --secondaries http://localhost:8081,http://localhost:8082
```

Every option can also be set with an environment variable:
`PORT`, `SECONDARIES`. See `cargo run -- master --help`.

## Tests

```sh
cargo test
```

The tests do not use the network. They build master and secondary nodes in memory,
and connect them through a test harness instead of the HTTP client.
For each secondary, the harness can:
- delay requests (master -> secondary) and responses (ACK, secondary -> master);
- hold the next N requests and deliver them in reverse order.

Tokio paused time is used, so delays do not slow down the tests.

| File                     | What it checks                                              |
|--------------------------|-------------------------------------------------------------|
| `tests/replication.rs`   | write concern = all nodes: parallel sends, wait for all ACKs |
| `tests/write_concern.rs` | write concern 1/2/3, bad values, eventual consistency, acceptance test |
| `tests/ordering.rs`      | total order: reordered messages, gaps, concurrent appends   |

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
├── harness/         # in-memory core::Client with delays and reordering
├── replication.rs
├── write_concern.rs
└── ordering.rs
```

The master knows only the `core::Client` trait. In production it is the HTTP client.
In tests it is the harness.
