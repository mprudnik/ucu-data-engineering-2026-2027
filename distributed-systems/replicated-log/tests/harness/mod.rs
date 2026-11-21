//! In-memory test harness.
//!
//! [`Client`] replaces the HTTP client. It calls `secondary::Node` directly,
//! so no sockets are used. The test controls the link to each secondary
//! through [`Network`]:
//! - request delay: master -> secondary, before the secondary handles the request;
//! - response delay: secondary -> master, before the master gets the ACK;
//! - reordering: hold the next N requests to a secondary, then deliver them in reverse order;
//! - request drop: the request is lost, the secondary never sees it, no reply comes;
//! - response drop: the secondary handles the request, but the ACK is lost;
//! - stopped secondary: every request fails at once with a transport error.
//!
//! A dropped request or response never gets a reply. The master stops waiting
//! only by its own request timeout ([`master::Config::request_timeout`]).
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
/// Re-exported so tests can write `harness::NodeHealth`.
pub use replicated_log::core::NodeHealth;
use replicated_log::core::{
    self, Request, master,
    message::{Heartbeat, Replicate},
    secondary,
};

/// Default cluster size in tests: master + 2 secondaries.
pub const SECONDARIES: usize = 2;
/// Secondary S1 from the task.
pub const S1: NodeId = NodeId(1);
/// Secondary S2 from the task.
pub const S2: NodeId = NodeId(2);

/// Returns the master settings used by [`Cluster::new`].
///
/// The timeout is longer than any delay in the tests, so a slow secondary
/// is never retried. Tests for retries use [`Cluster::with_config`].
fn long_timeout_config() -> master::Config {
    master::Config {
        request_timeout: Duration::from_secs(60 * 60),
        ..Default::default()
    }
}

/// Link settings shared by all harness clients.
///
/// Settings are read once, when a send starts. A change does not affect
/// requests that are already on the way.
///
/// With write concern below all nodes, `append` returns before its background
/// sends have started. Call [`settle`] before changing settings, or the change
/// also applies to those sends.
#[derive(Default)]
pub struct Network {
    links: Mutex<HashMap<NodeId, Link>>,
    reorders: Mutex<HashMap<NodeId, Reorder>>,
    /// Every send the master started (each retry counts), in order.
    sent: Mutex<Vec<RecordedRequest>>,
    /// Every request a secondary handled, in order.
    delivered: Mutex<Vec<RecordedRequest>>,
}

/// One request seen by the harness: to which secondary, which method, and when.
#[derive(Debug, Clone, Copy)]
struct RecordedRequest {
    to: NodeId,
    method: &'static str,
    at: Instant,
}

/// Settings of the link from the master to one secondary.
#[derive(Debug, Default, Clone, Copy)]
struct Link {
    request_delay: Duration,
    response_delay: Duration,
    drop_requests: bool,
    drop_responses: bool,
    stopped: bool,
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
    /// Delays every request from the master to secondary `to`.
    pub fn set_delay(&self, to: NodeId, delay: Duration) {
        self.update_link(to, |link| link.request_delay = delay);
    }

    /// Removes the request delay set by [`Self::set_delay`].
    pub fn remove_delay(&self, to: NodeId) {
        self.update_link(to, |link| link.request_delay = Duration::ZERO);
    }

    /// Delays every response (ACK) from secondary `to` back to the master.
    pub fn set_response_delay(&self, to: NodeId, delay: Duration) {
        self.update_link(to, |link| link.response_delay = delay);
    }

    /// Removes the response delay set by [`Self::set_response_delay`].
    pub fn remove_response_delay(&self, to: NodeId) {
        self.update_link(to, |link| link.response_delay = Duration::ZERO);
    }

    /// Drops every request to secondary `to`.
    ///
    /// The secondary never sees the request, and the master gets no reply.
    pub fn drop_requests(&self, to: NodeId) {
        self.update_link(to, |link| link.drop_requests = true);
    }

    /// Stops dropping requests, as set by [`Self::drop_requests`].
    pub fn remove_drop(&self, to: NodeId) {
        self.update_link(to, |link| link.drop_requests = false);
    }

    /// Drops every response (ACK) from secondary `to`.
    ///
    /// The secondary handles the request and stores the message,
    /// but the master gets no reply.
    pub fn drop_responses(&self, to: NodeId) {
        self.update_link(to, |link| link.drop_responses = true);
    }

    /// Stops dropping responses, as set by [`Self::drop_responses`].
    pub fn remove_response_drop(&self, to: NodeId) {
        self.update_link(to, |link| link.drop_responses = false);
    }

    /// Stops secondary `to`.
    ///
    /// Every request then fails at once with a transport error,
    /// as when the connection is refused.
    pub fn stop_secondary(&self, to: NodeId) {
        self.update_link(to, |link| link.stopped = true);
    }

    /// Starts secondary `to` again after [`Self::stop_secondary`].
    pub fn start_secondary(&self, to: NodeId) {
        self.update_link(to, |link| link.stopped = false);
    }

    /// Holds the next `count` requests to secondary `to`, then delivers them
    /// in reverse order (the last one first).
    ///
    /// Works once. After the batch is delivered, requests go through normally
    /// again. Requests in the batch get no reply until the batch is full.
    ///
    /// # Panics
    ///
    /// Panics if `count` is 0.
    pub fn reorder(&self, to: NodeId, count: usize) {
        assert!(count > 0, "reorder count must be at least 1");
        let reorder = Reorder {
            count,
            held: Vec::new(),
        };
        self.reorders.lock().unwrap().insert(to, reorder);
    }

    /// Returns how many `method` sends to secondary `to` the master started,
    /// retries included.
    ///
    /// `method` is the `METHOD` of a request, for example [`Replicate::METHOD`].
    pub fn sent_requests(&self, to: NodeId, method: &str) -> usize {
        self.sent_times(to, method).len()
    }

    /// Returns when the master started each `method` send to secondary `to`,
    /// oldest first.
    pub fn sent_times(&self, to: NodeId, method: &str) -> Vec<Instant> {
        Self::times(&self.sent, to, method)
    }

    /// Returns how many `method` requests secondary `to` handled.
    ///
    /// With retries this can be more than the number of messages:
    /// the secondary got duplicates.
    pub fn delivered_requests(&self, to: NodeId, method: &str) -> usize {
        Self::times(&self.delivered, to, method).len()
    }

    fn update_link(&self, to: NodeId, change: impl FnOnce(&mut Link)) {
        change(self.links.lock().unwrap().entry(to).or_default());
    }

    fn link(&self, to: NodeId) -> Link {
        self.links
            .lock()
            .unwrap()
            .get(&to)
            .copied()
            .unwrap_or_default()
    }

    fn times(log: &Mutex<Vec<RecordedRequest>>, to: NodeId, method: &str) -> Vec<Instant> {
        log.lock()
            .unwrap()
            .iter()
            .filter(|request| request.to == to && request.method == method)
            .map(|request| request.at)
            .collect()
    }

    fn record(log: &Mutex<Vec<RecordedRequest>>, to: NodeId, method: &'static str) {
        let at = Instant::now();
        log.lock().unwrap().push(RecordedRequest { to, method, at });
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

    /// Hands one request to the secondary and counts it.
    async fn dispatch(
        &self,
        to: NodeId,
        target: &secondary::Node,
        method: &'static str,
        body: Value,
    ) -> Result<Value, core::Error> {
        Self::record(&self.delivered, to, method);
        dispatch(target, method, body).await
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

        let link = self.network.link(self.to);
        Network::record(&self.network.sent, self.to, R::METHOD);

        if link.stopped {
            return Err(core::Error::Transport {
                code: 0,
                message: format!("secondary {} is stopped", self.to.0),
            });
        }
        if link.drop_requests {
            // Lost on the way: no reply ever comes.
            return std::future::pending().await;
        }

        // The delivery runs in its own task, so it still reaches the secondary
        // when the master stops waiting (timeout) and drops this future.
        // A slow request then arrives late, next to its retry.
        let delivery = tokio::spawn(deliver(
            Arc::clone(&self.network),
            self.to,
            Arc::clone(&self.target),
            R::METHOD,
            body,
            link.request_delay,
        ));
        let reply = delivery.await.expect("delivery task panicked")?;

        if link.drop_responses {
            // The secondary handled the request, but the ACK is lost.
            return std::future::pending().await;
        }
        tokio::time::sleep(link.response_delay).await;

        serde_json::from_value(reply).map_err(serde_error)
    }
}

/// Waits for the request delay, then hands the request to the secondary,
/// unless a reorder batch holds it.
async fn deliver(
    network: Arc<Network>,
    to: NodeId,
    target: Arc<secondary::Node>,
    method: &'static str,
    body: Value,
    request_delay: Duration,
) -> Result<Value, core::Error> {
    tokio::time::sleep(request_delay).await;
    match network.reorder_step(to, method, body) {
        ReorderStep::Deliver(body) => network.dispatch(to, &target, method, body).await,
        ReorderStep::Wait(receiver) => receiver.await.expect("reorder batch was dropped"),
        ReorderStep::DeliverBatch(batch, receiver) => {
            for held in batch.into_iter().rev() {
                let reply = network.dispatch(to, &target, held.method, held.body).await;
                let _ = held.reply.send(reply);
            }
            receiver.await.expect("reply to own request was dropped")
        }
    }
}

/// Routes a request to the secondary, in memory.
///
/// Uses the same routes as the axum router of the secondary.
async fn dispatch(node: &secondary::Node, method: &str, body: Value) -> Result<Value, core::Error> {
    match method {
        m if m == Replicate::METHOD => {
            let req: Replicate = serde_json::from_value(body).map_err(serde_error)?;
            node.append(req.message).await;
            Ok(Value::Null) // `Replicate::Response` is `()`
        }
        m if m == Heartbeat::METHOD => {
            let _: Heartbeat = serde_json::from_value(body).map_err(serde_error)?;
            Ok(Value::Null) // `Heartbeat::Response` is `()`
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

/// Sends `tracing` logs to the test output.
///
/// The level comes from `RUST_LOG`. See the logs with
/// `RUST_LOG=info cargo test -- --nocapture`. Without `--nocapture`,
/// logs are shown only for failed tests.
fn init_logging() {
    // Fails when an earlier test in the same binary already set it up; that is fine.
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
}

/// One master and `n` secondaries, connected through one [`Network`].
///
/// Secondaries have ids `NodeId(1)..=NodeId(n)`, the same as S1, S2, ... in the task.
pub struct Cluster {
    /// The master node.
    pub master: Arc<master::Node<Client>>,
    /// The secondary nodes, by id.
    pub secondaries: HashMap<NodeId, Arc<secondary::Node>>,
    /// The links between the master and the secondaries.
    pub network: Arc<Network>,
}

impl Cluster {
    /// Creates a cluster where the master never times out on a slow secondary.
    ///
    /// For tests without failures (iterations 1 and 2).
    pub fn new(secondaries: usize) -> Self {
        Self::with_config(long_timeout_config(), secondaries)
    }

    /// Creates a cluster with the given master settings (timeout, retry delay, heartbeats).
    ///
    /// Heartbeats are off until [`Cluster::start_heartbeats`], so every secondary
    /// stays Healthy and the other tests do not depend on them.
    pub fn with_config(config: master::Config, secondaries: usize) -> Self {
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
            master: Arc::new(master::Node::new(config, clients)),
            secondaries,
            network,
        }
    }

    /// Starts the heartbeats from the master to all secondaries.
    pub fn start_heartbeats(&self) {
        self.master.start_heartbeats();
    }

    /// Returns the health of secondary `id`, as the master sees it.
    ///
    /// # Panics
    ///
    /// Panics if there is no secondary `id`.
    pub async fn health(&self, id: NodeId) -> NodeHealth {
        let health = self.master.health().await;
        *health
            .get(&id)
            .unwrap_or_else(|| panic!("no health for secondary {id:?}"))
    }

    /// Appends a message with the given text and write concern through the master.
    pub async fn append(&self, value: &str, write_concern: usize) -> Result<(), core::Error> {
        self.master.append(value.to_string(), write_concern).await
    }

    /// Appends a message and returns how long the append was blocked.
    pub async fn timed_append(
        &self,
        value: &str,
        write_concern: usize,
    ) -> Result<Duration, core::Error> {
        let start = Instant::now();
        self.append(value, write_concern).await?;
        Ok(start.elapsed())
    }

    /// Returns the messages on the master.
    pub async fn master_values(&self) -> Vec<String> {
        self.master.list().await
    }

    /// Returns the messages on secondary `id`.
    ///
    /// # Panics
    ///
    /// Panics if there is no secondary `id`.
    pub async fn secondary_values(&self, id: NodeId) -> Vec<String> {
        let node = self
            .secondaries
            .get(&id)
            .unwrap_or_else(|| panic!("no secondary with id {id:?}"));
        node.list().await
    }
}

/// Lets background replication run until it is blocked again.
///
/// Replication is blocked by a delay, a reorder batch, a dropped message,
/// or because it is done.
///
/// With paused time, Tokio moves the clock only when no task can run.
/// So all ready tasks run first. Moves the clock by 1ms.
pub async fn settle() {
    tokio::time::sleep(Duration::from_millis(1)).await;
}
