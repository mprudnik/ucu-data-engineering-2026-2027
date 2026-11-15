//! In-memory test harness.
//!
//! [`Client`] replaces the HTTP client. It calls `secondary::Node` directly,
//! so no sockets are used. The test controls the link to each secondary
//! through [`Network`]:
//! - request delay: master -> secondary, before the secondary handles the request;
//! - response delay: secondary -> master, before the master gets the ACK;
//! - reordering: hold the next N requests to a secondary, then deliver them in reverse order.
//!
//! Use `#[tokio::test(start_paused = true)]`. Delays then finish at once,
//! and `tokio::time::Instant::elapsed()` returns the exact delay.

#![allow(dead_code)] // each test file uses only part of the harness

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use serde_json::Value;
use tokio::{sync::oneshot, time::Instant};

/// Re-exported so tests can write `harness::NodeId`.
pub use replicated_log::core::NodeId;
use replicated_log::core::{self, Request, master, message::Replicate, secondary};

/// Default cluster size in tests: master + 2 secondaries.
pub const SECONDARIES: usize = 2;
pub const S1: NodeId = NodeId(1);
pub const S2: NodeId = NodeId(2);

/// Link settings shared by all harness clients.
/// Settings are read when a send task first runs, so the test can change them between appends.
/// With write concern below all nodes, `append` returns before its background sends have run:
/// call [`settle`] before changing settings, or the change also applies to those sends.
#[derive(Default)]
pub struct Network {
    request_delays: Mutex<HashMap<NodeId, Duration>>,
    response_delays: Mutex<HashMap<NodeId, Duration>>,
    reorders: Mutex<HashMap<NodeId, Reorder>>,
}

/// Requests held for one secondary until `count` of them have arrived.
struct Reorder {
    count: usize,
    held: Vec<HeldRequest>,
}

/// A request waiting in a reorder batch, with the channel for its reply.
struct HeldRequest {
    method: &'static str,
    body: Value,
    reply: oneshot::Sender<Result<Value, core::Error>>,
}

/// What a request does after the reorder check.
enum ReorderStep {
    /// No reorder is set for this secondary: deliver now.
    Deliver(Value),
    /// Held in the batch: wait for the reply.
    Wait(oneshot::Receiver<Result<Value, core::Error>>),
    /// This request filled the batch: deliver the whole batch in reverse order.
    DeliverBatch(
        Vec<HeldRequest>,
        oneshot::Receiver<Result<Value, core::Error>>,
    ),
}

impl Network {
    /// Delay every request from the master to secondary `to`.
    pub fn set_delay(&self, to: NodeId, delay: Duration) {
        self.request_delays.lock().unwrap().insert(to, delay);
    }

    pub fn remove_delay(&self, to: NodeId) {
        self.request_delays.lock().unwrap().remove(&to);
    }

    /// Delay every response (ACK) from secondary `to` back to the master.
    pub fn set_response_delay(&self, to: NodeId, delay: Duration) {
        self.response_delays.lock().unwrap().insert(to, delay);
    }

    pub fn remove_response_delay(&self, to: NodeId) {
        self.response_delays.lock().unwrap().remove(&to);
    }

    /// Hold the next `count` requests to secondary `to`, then deliver them
    /// in reverse order (the last one first). Works once; after the batch
    /// is delivered, requests go through normally again.
    ///
    /// Requests in the batch get no reply until the batch is full.
    pub fn reorder(&self, to: NodeId, count: usize) {
        assert!(count > 0, "reorder count must be at least 1");
        let reorder = Reorder {
            count,
            held: Vec::new(),
        };
        self.reorders.lock().unwrap().insert(to, reorder);
    }

    fn request_delay(&self, to: NodeId) -> Duration {
        self.request_delays
            .lock()
            .unwrap()
            .get(&to)
            .copied()
            .unwrap_or_default()
    }

    fn response_delay(&self, to: NodeId) -> Duration {
        self.response_delays
            .lock()
            .unwrap()
            .get(&to)
            .copied()
            .unwrap_or_default()
    }

    fn reorder_step(&self, to: NodeId, method: &'static str, body: Value) -> ReorderStep {
        let mut reorders = self.reorders.lock().unwrap();
        let Some(reorder) = reorders.get_mut(&to) else {
            return ReorderStep::Deliver(body);
        };
        let (reply, receiver) = oneshot::channel();
        reorder.held.push(HeldRequest {
            method,
            body,
            reply,
        });
        if reorder.held.len() < reorder.count {
            return ReorderStep::Wait(receiver);
        }
        let batch = reorders.remove(&to).expect("reorder exists").held;
        ReorderStep::DeliverBatch(batch, receiver)
    }
}

/// The master's connection to one secondary, in memory.
pub struct Client {
    to: NodeId,
    target: Arc<secondary::Node>,
    network: Arc<Network>,
}

impl core::Client for Client {
    async fn send<R: Request>(&self, req: R) -> Result<R::Response, core::Error> {
        // Go through JSON like the HTTP client does. This also checks the serde derives.
        let body = serde_json::to_value(req).map_err(serde_error)?;

        tokio::time::sleep(self.network.request_delay(self.to)).await;
        let reply = self.deliver(R::METHOD, body).await?;
        tokio::time::sleep(self.network.response_delay(self.to)).await;

        serde_json::from_value(reply).map_err(serde_error)
    }
}

impl Client {
    /// Hands the request to the secondary, unless a reorder batch holds it.
    async fn deliver(&self, method: &'static str, body: Value) -> Result<Value, core::Error> {
        match self.network.reorder_step(self.to, method, body) {
            ReorderStep::Deliver(body) => dispatch(&self.target, method, body).await,
            ReorderStep::Wait(receiver) => receiver.await.expect("reorder batch was dropped"),
            ReorderStep::DeliverBatch(batch, receiver) => {
                for held in batch.into_iter().rev() {
                    let reply = dispatch(&self.target, held.method, held.body).await;
                    let _ = held.reply.send(reply);
                }
                receiver.await.expect("reply to own request was dropped")
            }
        }
    }
}

/// Same routing as the secondary's axum router, but in memory.
async fn dispatch(node: &secondary::Node, method: &str, body: Value) -> Result<Value, core::Error> {
    match method {
        m if m == Replicate::METHOD => {
            let req: Replicate = serde_json::from_value(body).map_err(serde_error)?;
            node.append(req.message).await;
            Ok(Value::Null) // `Replicate::Response` is `()`
        }
        m => Err(core::Error::Transport {
            code: 404,
            message: format!("unknown method: {m}"),
        }),
    }
}

fn serde_error(e: serde_json::Error) -> core::Error {
    core::Error::Transport {
        code: 0,
        message: e.to_string(),
    }
}

/// Sends `tracing` logs to the test output. Level comes from `RUST_LOG`.
/// See them with: `RUST_LOG=info cargo test -- --nocapture`
/// (without `--nocapture`, logs are shown only for failed tests).
fn init_logging() {
    // Fails when an earlier test in the same binary already set it up; that is fine.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

/// One master and `n` secondaries, connected through one [`Network`].
/// Secondaries have ids `NodeId(1)..=NodeId(n)`, the same as S1, S2, ... in the task.
pub struct Cluster {
    pub master: master::Node<Client>,
    pub secondaries: HashMap<NodeId, Arc<secondary::Node>>,
    pub network: Arc<Network>,
}

impl Cluster {
    pub fn new(secondaries: usize) -> Self {
        init_logging();
        let network = Arc::new(Network::default());
        let secondaries: HashMap<_, _> = (1..=secondaries)
            .map(|i| (NodeId(i), Arc::new(secondary::Node::new())))
            .collect();
        let clients = secondaries
            .iter()
            .map(|(&to, target)| {
                let client = Arc::new(Client {
                    to,
                    target: Arc::clone(target),
                    network: Arc::clone(&network),
                });
                (to, client)
            })
            .collect();
        Self {
            master: master::Node::new(clients),
            secondaries,
            network,
        }
    }

    /// Append a message with the given text and write concern through the master.
    pub async fn append(&self, value: &str, write_concern: usize) -> Result<(), core::Error> {
        self.master.append(value.to_string(), write_concern).await
    }

    /// Append and return how long the append was blocked.
    pub async fn timed_append(
        &self,
        value: &str,
        write_concern: usize,
    ) -> Result<Duration, core::Error> {
        let start = Instant::now();
        self.append(value, write_concern).await?;
        Ok(start.elapsed())
    }

    /// Messages on the master.
    pub async fn master_values(&self) -> Vec<String> {
        self.master.list().await
    }

    /// Messages on secondary `id`.
    pub async fn secondary_values(&self, id: NodeId) -> Vec<String> {
        let node = self
            .secondaries
            .get(&id)
            .unwrap_or_else(|| panic!("no secondary with id {id:?}"));
        node.list().await
    }
}

/// Lets background replication run until it is blocked again
/// (by a delay, a reorder batch, or because it is done).
///
/// With paused time, Tokio moves the clock only when no task can run.
/// So all ready tasks run first. Moves the clock by 1ms.
pub async fn settle() {
    tokio::time::sleep(Duration::from_millis(1)).await;
}
