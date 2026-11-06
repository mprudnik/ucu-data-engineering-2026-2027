# Replicated log

UCU Data Engineering, Distributed Systems course. A replicated log with one master
and any number of secondaries.

Each iteration lives in its own branch (`iter-1`, `iter-2`, `iter-3`).

## Iteration 1

- `POST` on the master appends a message to its in-memory log.
- The master sends the message to all secondaries **in parallel**.
- The `POST` returns only after **every** secondary has sent an ACK (blocking replication).
- The link is assumed perfect: no lost messages, no retries.

## Run with Docker

```sh
docker compose up --build
```

With a delay on the secondaries, to see that `POST` blocks:

```sh
S1_DELAY_MS=5000 S2_DELAY_MS=6000 docker compose up --build
```

| Node        | URL                    |
|-------------|------------------------|
| master      | http://localhost:28080 |
| secondary-1 | http://localhost:28081 |
| secondary-2 | http://localhost:28082 |

Log level is set with `RUST_LOG` (default `info`).

## HTTP API

### Master

```sh
# Append. Blocks until all secondaries ACK. Returns 201 and the stored message.
curl -X POST localhost:28080/append \
  -H 'content-type: application/json' \
  -d '{"value": "m1"}'

# List
curl localhost:28080/list
```

If a secondary cannot be reached, `/append` returns `502` with `{"error": "..."}`.

### Secondary

```sh
curl localhost:28081/list
```

`POST /replicate` is internal. Only the master calls it.

## Run without Docker

```sh
cargo run -- secondary --port 8081 --delay-ms 2000
cargo run -- secondary --port 8082
cargo run -- master --port 8080 --secondaries http://localhost:8081,http://localhost:8082
```

Every option can also be set with an environment variable:
`PORT`, `SECONDARIES`, `DELAY_MS`. See `cargo run -- master --help`.

## Tests

```sh
cargo test
```

The tests do not use the network. They build master and secondary nodes in memory,
and connect them through a test harness instead of the HTTP client.
The harness can delay messages between nodes.
Tokio paused time is used, so delays do not slow down the tests.

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
├── harness/         # in-memory core::Client with delays
└── iter-1.rs
```

The master knows only the `core::Client` trait. In production it is the HTTP client.
In tests it is the harness.
