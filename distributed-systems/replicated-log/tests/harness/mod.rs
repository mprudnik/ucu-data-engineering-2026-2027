//! In-memory test harness.
//!
//! [`Client`] replaces the HTTP client. It calls `secondary::Node` directly,
//! so no sockets are used. The test controls delays through [`Network`],
//! in both directions:
//! - request delay: master -> secondary, before the secondary handles the request;
//! - response delay: secondary -> master, before the master gets the ACK.
//!
//! Use `#[tokio::test(start_paused = true)]`. Delays then finish at once,
//! and `tokio::time::Instant::elapsed()` returns the exact delay.

#![allow(dead_code)] // each test file uses only part of the harness

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use chrono::Utc;
use serde_json::Value;

/// Re-exported so tests can write `harness::NodeId`.
pub use replicated_log::core::NodeId;
use replicated_log::core::{
    self, Request, master,
    message::{Message, Replicate},
    secondary,
};

/// Delay settings shared by all harness clients.
/// A delay is read when a request is sent, so the test can change it between appends.
#[derive(Default)]
pub struct Network {
    request_delays: Mutex<HashMap<NodeId, Duration>>,
    response_delays: Mutex<HashMap<NodeId, Duration>>,
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
        let reply = dispatch(&self.target, R::METHOD, body).await?;
        tokio::time::sleep(self.network.response_delay(self.to)).await;

        serde_json::from_value(reply).map_err(serde_error)
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

/// One master and `n` secondaries, connected through one [`Network`].
/// Secondaries have ids `NodeId(1)..=NodeId(n)`, the same as S1, S2, ... in the task.
pub struct Cluster {
    pub master: master::Node<Client>,
    pub secondaries: HashMap<NodeId, Arc<secondary::Node>>,
    pub network: Arc<Network>,
}

impl Cluster {
    pub fn new(secondaries: usize) -> Self {
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

    /// Append a message with the given text through the master.
    pub async fn append(&self, value: &str) -> Result<(), core::Error> {
        self.master.append(message(value)).await
    }

    /// Message texts on the master.
    pub async fn master_values(&self) -> Vec<String> {
        values(&self.master.list().await)
    }

    /// Message texts on secondary `id`.
    pub async fn secondary_values(&self, id: NodeId) -> Vec<String> {
        let node = self
            .secondaries
            .get(&id)
            .unwrap_or_else(|| panic!("no secondary with id {id:?}"));
        values(&node.list().await)
    }
}

/// A message with the given text and the current time.
pub fn message(value: &str) -> Message {
    Message {
        value: value.to_string(),
        timestamp: Utc::now(),
    }
}

/// Only the texts, for short asserts.
pub fn values(messages: &[Message]) -> Vec<String> {
    messages.iter().map(|m| m.value.clone()).collect()
}
