//! Master node: accepts appends and replicates them to the secondaries.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};

use smart_default::SmartDefault;
use tokio::{
    sync::{RwLock, watch},
    time::Instant,
};

use super::{
    NodeHealth, NodeId,
    message::{Heartbeat, Message, Replicate},
};

/// Timeouts, retry delays and heartbeat settings of the master.
#[derive(Debug, Clone, SmartDefault)]
pub struct Config {
    /// How long to wait for a reply (ACK or heartbeat) from a secondary.
    #[default(Duration::from_secs(5))]
    pub request_timeout: Duration,
    /// Pause after the first failed attempt.
    ///
    /// The pause grows with each attempt.
    #[default(Duration::from_secs(1))]
    pub retry_delay: Duration,
    /// Upper limit for the pause between attempts.
    #[default(Duration::from_secs(30))]
    pub max_retry_delay: Duration,
    /// How often the master sends a heartbeat to each secondary.
    #[default(Duration::from_secs(1))]
    pub heartbeat_interval: Duration,
    /// Time without a successful heartbeat after which a secondary is
    /// [`NodeHealth::Suspected`].
    #[default(Duration::from_secs(3))]
    pub suspected_after: Duration,
    /// Time without a successful heartbeat after which a secondary is
    /// [`NodeHealth::Unhealthy`].
    #[default(Duration::from_secs(10))]
    pub unhealthy_after: Duration,
}

/// What the master knows about one secondary.
pub struct Secondary<C: super::Client> {
    client: Arc<C>,
    health: NodeHealth,
    last_heartbeat_at: Instant,
}

impl<C: super::Client> Secondary<C> {
    /// Creates a Healthy secondary that is reached through `client`.
    pub fn new(client: Arc<C>) -> Self {
        Self {
            client,
            health: NodeHealth::Healthy,
            last_heartbeat_at: Instant::now(),
        }
    }
}

/// Sequence numbers one secondary has ACKed.
#[derive(Debug, Default, Clone)]
struct Acked {
    /// Highest sequence number such that every number from 1 up to it is ACKed.
    up_to: usize,
    /// ACKed sequence numbers after a gap, all above `up_to + 1`.
    ahead: BTreeSet<usize>,
}

impl Acked {
    fn contains(&self, sequence_number: usize) -> bool {
        sequence_number <= self.up_to || self.ahead.contains(&sequence_number)
    }

    fn insert(&mut self, sequence_number: usize) {
        if sequence_number <= self.up_to {
            return;
        }
        self.ahead.insert(sequence_number);
        while self.ahead.remove(&(self.up_to + 1)) {
            self.up_to += 1;
        }
    }
}

/// ACKed sequence numbers of every secondary.
///
/// [`Node::append`] waits for changes to it.
type AckedBySecondary = watch::Sender<HashMap<NodeId, Acked>>;

/// The master node.
///
/// Keeps the log, replicates it to the secondaries, and tracks their health.
pub struct Node<C: super::Client> {
    config: Arc<Config>,
    messages: Arc<RwLock<Vec<Message>>>,
    secondaries: Arc<HashMap<NodeId, Arc<RwLock<Secondary<C>>>>>,
    acked: Arc<AckedBySecondary>,
}

impl<C: super::Client> Node<C> {
    /// Creates a master with an empty log and the given secondaries.
    ///
    /// All secondaries start Healthy. Heartbeats start only after
    /// [`Self::start_heartbeats`].
    pub fn new(config: Config, secondaries: HashMap<NodeId, Arc<C>>) -> Self {
        let acked = secondaries.keys().map(|&id| (id, Acked::default())).collect();
        let (acked, _) = watch::channel(acked);
        Self {
            config: Arc::new(config),
            messages: Arc::new(RwLock::new(Vec::new())),
            secondaries: Arc::new(
                secondaries
                    .into_iter()
                    .map(|(id, client)| (id, Arc::new(RwLock::new(Secondary::new(client)))))
                    .collect(),
            ),
            acked: Arc::new(acked),
        }
    }

    /// Appends a message to the log and replicates it to all secondaries.
    ///
    /// Returns when `write_concern` nodes have the message, the master included.
    /// There is no timeout: the append waits as long as needed. Sends to the
    /// other secondaries go on in the background.
    ///
    /// # Errors
    ///
    /// - [`Error::InvalidWriteConcern`](super::Error::InvalidWriteConcern)
    ///   if `write_concern` is below 1 or above the number of nodes;
    /// - [`Error::NoQuorum`](super::Error::NoQuorum) if a majority of nodes
    ///   is not available.
    ///
    /// In both cases the message is not added to the log.
    pub async fn append(&self, value: String, write_concern: usize) -> Result<(), super::Error> {
        self.validate_write_concern(write_concern)?;
        // Before the sequence number is taken: a rejected message never enters the log.
        self.check_quorum().await?;

        let mut messages = self.messages.write().await;

        let sequence_number = messages.len() + 1;

        let message = Message {
            value,
            sequence_number,
        };

        messages.push(message.clone());

        // One send task per secondary. Started under the lock, so sends start
        // in sequence-number order. Nobody waits for the tasks themselves:
        // they record ACKs in `acked`.
        for (&id, secondary) in self.secondaries.iter() {
            tokio::spawn(Self::replicate(
                self.config.clone(),
                id,
                secondary.clone(),
                message.clone(),
                self.acked.clone(),
            ));
        }

        std::mem::drop(messages);

        self.wait_for_acks(sequence_number, write_concern - 1).await;

        Ok(())
    }

    /// Returns all messages on the master, in sequence-number order.
    pub async fn list(&self) -> Vec<String> {
        self.messages
            .read()
            .await
            .iter()
            .map(|m| m.value.clone())
            .collect()
    }

    /// Starts one heartbeat task per secondary.
    ///
    /// A slow secondary does not delay heartbeats to the others.
    /// The tasks run until the program stops.
    pub fn start_heartbeats(&self) {
        for (&id, secondary) in self.secondaries.iter() {
            tokio::spawn(Self::heartbeat(
                self.config.clone(),
                id,
                secondary.clone(),
                self.messages.clone(),
                self.acked.clone(),
            ));
        }
    }

    /// Sends a heartbeat every `heartbeat_interval` and updates the health.
    ///
    /// The new health is:
    /// - [`NodeHealth::Healthy`] after a reply within `request_timeout`;
    /// - [`NodeHealth::Suspected`] after no successful heartbeat for `suspected_after`;
    /// - [`NodeHealth::Unhealthy`] after no successful heartbeat for `unhealthy_after`.
    ///
    /// A change from Unhealthy to Healthy starts [`Self::catch_up`].
    async fn heartbeat(
        config: Arc<Config>,
        id: NodeId,
        secondary: Arc<RwLock<Secondary<C>>>,
        messages: Arc<RwLock<Vec<Message>>>,
        acked: Arc<AckedBySecondary>,
    ) {
        let client = secondary.read().await.client.clone();
        loop {
            tokio::time::sleep(config.heartbeat_interval).await;

            // No lock during the send: appends and `health()` must not wait for it.
            let reply = tokio::time::timeout(config.request_timeout, client.send(Heartbeat)).await;

            if let Ok(Ok(())) = reply {
                let mut write_secondary = secondary.write().await;
                let recovered = write_secondary.health == NodeHealth::Unhealthy;
                if write_secondary.health != NodeHealth::Healthy {
                    tracing::info!(secondary = id.0, from = %write_secondary.health, to = %NodeHealth::Healthy, "health changed");
                }
                write_secondary.health = NodeHealth::Healthy;
                write_secondary.last_heartbeat_at = Instant::now();
                std::mem::drop(write_secondary);

                if recovered {
                    tokio::spawn(Self::catch_up(
                        config.clone(),
                        id,
                        secondary.clone(),
                        messages.clone(),
                        acked.clone(),
                    ));
                }
                continue;
            }

            let mut secondary = secondary.write().await;
            let since_last_heartbeat = secondary.last_heartbeat_at.elapsed();
            let health = if since_last_heartbeat > config.unhealthy_after {
                NodeHealth::Unhealthy
            } else if since_last_heartbeat > config.suspected_after {
                NodeHealth::Suspected
            } else {
                continue;
            };

            if health != secondary.health {
                tracing::info!(secondary = id.0, from = %secondary.health, to = %health, "health changed");
            }
            secondary.health = health;
        }
    }

    /// Sends the secondary every message it has not ACKed.
    ///
    /// Runs after a recovery. Sends the messages oldest first, one at a time.
    /// Stops if the secondary becomes Unhealthy again. The next recovery
    /// starts a new catch-up.
    async fn catch_up(
        config: Arc<Config>,
        id: NodeId,
        secondary: Arc<RwLock<Secondary<C>>>,
        messages: Arc<RwLock<Vec<Message>>>,
        acked: Arc<AckedBySecondary>,
    ) {
        let missing = {
            let messages = messages.read().await;
            not_acked(&acked, id, &messages)
        };
        tracing::info!(secondary = id.0, count = missing.len(), "catch-up started");
        for message in missing {
            let delivered =
                Self::replicate(config.clone(), id, secondary.clone(), message, acked.clone())
                    .await;
            if !delivered {
                return;
            }
        }
    }

    /// Returns the health of each secondary.
    pub async fn health(&self) -> HashMap<NodeId, NodeHealth> {
        let mut health = HashMap::with_capacity(self.secondaries.len());
        for (id, secondary) in self.secondaries.iter() {
            health.insert(*id, secondary.read().await.health);
        }
        health
    }

    /// Checks that a majority of nodes (master + secondaries) is available.
    ///
    /// The master always counts. A secondary counts unless it is Unhealthy:
    /// a Suspected secondary only missed a few heartbeats.
    ///
    /// # Errors
    ///
    /// Returns [`Error::NoQuorum`](super::Error::NoQuorum) if fewer than
    /// a majority of nodes are available. The master is then read-only.
    async fn check_quorum(&self) -> Result<(), super::Error> {
        let total = self.secondaries.len() + 1;
        let required = total / 2 + 1;

        let mut available = 1; // the master
        for secondary in self.secondaries.values() {
            if secondary.read().await.health != NodeHealth::Unhealthy {
                available += 1;
            }
        }

        if available < required {
            return Err(super::Error::NoQuorum {
                available,
                required,
                total,
            });
        }
        Ok(())
    }

    fn validate_write_concern(&self, requested: usize) -> Result<(), super::Error> {
        let min = 1;
        let max = self.secondaries.len() + 1;
        if requested < min || requested > max {
            return Err(super::Error::InvalidWriteConcern { requested, max });
        }

        Ok(())
    }

    /// Waits until `required` secondaries have ACKed `sequence_number`.
    ///
    /// Checks at once, then again after every ACK. Uses no timers.
    async fn wait_for_acks(&self, sequence_number: usize, required: usize) {
        if required == 0 {
            return;
        }
        let mut acked = self.acked.subscribe();
        let _ = acked
            .wait_for(|acked| {
                let count = acked
                    .values()
                    .filter(|acked| acked.contains(sequence_number))
                    .count();
                count >= required
            })
            .await;
    }

    /// Sends one message to the secondary until it is ACKed, then records the ACK.
    ///
    /// Returns `true` after the ACK. Returns `false` if it stopped because
    /// the secondary is Unhealthy. The message then stays in the log
    /// for [`Self::catch_up`] after the recovery.
    async fn replicate(
        config: Arc<Config>,
        id: NodeId,
        node: Arc<RwLock<Secondary<C>>>,
        message: Message,
        acked: Arc<AckedBySecondary>,
    ) -> bool {
        let sequence_number = message.sequence_number;
        let mut attempt = 0u32;
        loop {
            // Copy what we need, so the lock is not held during the send.
            let (client, health) = {
                let node = node.read().await;
                (node.client.clone(), node.health)
            };

            // Unhealthy: stop. No task or timer waits; the catch-up sends it later.
            if health == NodeHealth::Unhealthy {
                tracing::debug!(secondary = id.0, sequence_number, "secondary unhealthy, send stopped");
                return false;
            }

            attempt += 1;
            let request = Replicate {
                message: message.clone(),
            };
            match tokio::time::timeout(config.request_timeout, client.send(request)).await {
                Ok(Ok(())) => {
                    acked.send_modify(|acked| {
                        acked.entry(id).or_default().insert(sequence_number);
                    });
                    return true;
                }
                Ok(Err(err)) => {
                    tracing::warn!(%err, sequence_number, attempt, "send failed, retrying")
                }
                Err(_) => tracing::warn!(sequence_number, attempt, "timeout, retrying"),
            };

            // Healthy: `retry_delay * attempt`. Suspected: twice that.
            // Both are limited by `max_retry_delay`.
            let mut retry_delay = config.retry_delay * attempt;
            if node.read().await.health == NodeHealth::Suspected {
                retry_delay *= 2;
            }
            tokio::time::sleep(retry_delay.min(config.max_retry_delay)).await;
        }
    }
}

/// Returns the messages from `messages` that secondary `id` has not ACKed.
fn not_acked(acked: &AckedBySecondary, id: NodeId, messages: &[Message]) -> Vec<Message> {
    let acked_by_secondary = acked.borrow();
    let acked_for_this_secondary = &acked_by_secondary[&id];
    messages
        .iter()
        .filter(|message| !acked_for_this_secondary.contains(message.sequence_number))
        .cloned()
        .collect()
}
