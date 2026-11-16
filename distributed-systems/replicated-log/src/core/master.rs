use std::{collections::HashMap, sync::Arc, time::Duration};

use smart_default::SmartDefault;
use tokio::{sync::RwLock, task::JoinSet, time::Instant};

use super::{
    NodeHealth, NodeId,
    message::{Heartbeat, Message, Replicate},
};

#[derive(Debug, Clone, SmartDefault)]
pub struct Config {
    /// How long to wait for a secondary's reply (ACK or heartbeat) before giving up on it.
    #[default(Duration::from_secs(5))]
    pub request_timeout: Duration,
    /// Pause after the first failed attempt; grows with each attempt.
    #[default(Duration::from_secs(1))]
    pub retry_delay: Duration,
    /// Upper limit for the pause between attempts.
    #[default(Duration::from_secs(30))]
    pub max_retry_delay: Duration,
    /// How often the master sends a heartbeat to each secondary.
    #[default(Duration::from_secs(1))]
    pub heartbeat_interval: Duration,
    /// No successful heartbeat for this long -> Suspected.
    #[default(Duration::from_secs(3))]
    pub suspected_after: Duration,
    /// No successful heartbeat for this long -> Unhealthy.
    #[default(Duration::from_secs(10))]
    pub unhealthy_after: Duration,
}

pub struct Secondary<C: super::Client> {
    client: Arc<C>,
    health: NodeHealth,
    last_heartbeat_at: Instant,
}

pub struct Node<C: super::Client> {
    config: Arc<Config>,
    messages: RwLock<Vec<Message>>,
    secondaries: Arc<HashMap<NodeId, Arc<RwLock<Secondary<C>>>>>,
}

impl<C: super::Client> Node<C> {
    pub fn new(config: Config, secondaries: HashMap<NodeId, Arc<C>>) -> Self {
        Self {
            config: Arc::new(config),
            messages: RwLock::new(Vec::new()),
            secondaries: Arc::new(
                secondaries
                    .into_iter()
                    .map(|(id, client)| {
                        (
                            id,
                            Arc::new(RwLock::new(Secondary {
                                client,
                                health: NodeHealth::Healthy,
                                last_heartbeat_at: Instant::now(),
                            })),
                        )
                    })
                    .collect(),
            ),
        }
    }

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

        let mut tasks = JoinSet::new();
        for secondary in self.secondaries.values() {
            tasks.spawn(Self::replicate(
                self.config.clone(),
                secondary.clone(),
                message.clone(),
            ));
        }

        std::mem::drop(messages);

        for _ in 0..write_concern - 1 {
            match tasks.join_next().await {
                Some(result) => result.expect("replication failed")?,
                None => break,
            };
        }
        if !tasks.is_empty() {
            tasks.detach_all();
        }

        Ok(())
    }

    pub async fn list(&self) -> Vec<String> {
        self.messages
            .read()
            .await
            .iter()
            .map(|m| m.value.clone())
            .collect()
    }

    /// Starts one heartbeat task per secondary. A slow secondary does not
    /// delay heartbeats to the others. The tasks run until the program stops.
    pub fn start_heartbeats(&self) {
        for (&id, secondary) in self.secondaries.iter() {
            tokio::spawn(Self::heartbeat(self.config.clone(), id, secondary.clone()));
        }
    }

    /// Every `heartbeat_interval`, sends a heartbeat and updates the health:
    /// - a reply within `request_timeout` -> Healthy;
    /// - no successful heartbeat for `suspected_after` -> Suspected;
    /// - no successful heartbeat for `unhealthy_after` -> Unhealthy.
    async fn heartbeat(config: Arc<Config>, id: NodeId, secondary: Arc<RwLock<Secondary<C>>>) {
        let client = secondary.read().await.client.clone();
        loop {
            tokio::time::sleep(config.heartbeat_interval).await;

            // No lock during the send: appends and `health()` must not wait for it.
            let reply = tokio::time::timeout(config.request_timeout, client.send(Heartbeat)).await;

            let mut secondary = secondary.write().await;
            if let Ok(Ok(())) = reply {
                secondary.last_heartbeat_at = Instant::now();
            }

            let since_last_heartbeat = secondary.last_heartbeat_at.elapsed();
            let health = if since_last_heartbeat > config.unhealthy_after {
                NodeHealth::Unhealthy
            } else if since_last_heartbeat > config.suspected_after {
                NodeHealth::Suspected
            } else {
                NodeHealth::Healthy
            };

            if health != secondary.health {
                tracing::info!(secondary = id.0, from = %secondary.health, to = %health, "health changed");
            }
            secondary.health = health;
        }
    }

    pub async fn health(&self) -> HashMap<NodeId, NodeHealth> {
        let mut health = HashMap::with_capacity(self.secondaries.len());
        for (id, secondary) in self.secondaries.iter() {
            health.insert(id.clone(), secondary.read().await.health);
        }
        health
    }

    /// Quorum append: a majority of nodes (master + secondaries) must be available,
    /// otherwise the master is read-only and rejects appends.
    /// The master always counts. A secondary counts unless it is Unhealthy:
    /// Suspected only missed a few heartbeats.
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

    async fn replicate(
        config: Arc<Config>,
        node: Arc<RwLock<Secondary<C>>>,
        message: Message,
    ) -> Result<(), super::Error> {
        let sequence_number = message.sequence_number;
        let mut attempt = 0u32;
        loop {
            // Copy what we need, so the lock is not held during the send.
            let (client, health) = {
                let node = node.read().await;
                (node.client.clone(), node.health)
            };

            // Unhealthy: do not send, check the health again later.
            if health == NodeHealth::Unhealthy {
                tracing::debug!(sequence_number, "secondary unhealthy, waiting");
                tokio::time::sleep(config.max_retry_delay).await;
                continue;
            }

            attempt += 1;
            let request = Replicate {
                message: message.clone(),
            };
            match tokio::time::timeout(config.request_timeout, client.send(request)).await {
                Ok(Ok(())) => return Ok(()),
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
