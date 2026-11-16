use std::{collections::HashMap, sync::Arc, time::Duration};

use tokio::{sync::RwLock, task::JoinSet};

use super::{
    NodeId,
    message::{Message, Replicate},
};

pub struct Config {
    pub request_timeout: Duration,
    pub retry_delay: Duration,
    pub max_retry_delay: Duration,
}

pub struct Node<C: super::Client> {
    config: Arc<Config>,
    messages: RwLock<Vec<Message>>,
    secondaries: HashMap<NodeId, Arc<C>>,
}

impl<C: super::Client> Node<C> {
    pub fn new(config: Config, secondaries: HashMap<NodeId, Arc<C>>) -> Self {
        Self {
            config: Arc::new(config),
            messages: RwLock::new(Vec::new()),
            secondaries,
        }
    }

    pub async fn append(&self, value: String, write_concern: usize) -> Result<(), super::Error> {
        self.validate_write_concern(write_concern)?;

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
        node: Arc<C>,
        message: Message,
    ) -> Result<(), super::Error> {
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let request = Replicate {
                message: message.clone(),
            };
            match tokio::time::timeout(config.request_timeout, node.send(request)).await {
                Ok(Ok(())) => break,
                Ok(Err(err)) => tracing::warn!(%err, "send failed, retrying"),
                Err(_) => tracing::warn!("timeout, retrying"),
            };
            let retry_delay = (config.retry_delay * attempt).min(config.max_retry_delay);
            tokio::time::sleep(retry_delay).await;
        }
        Ok(())
    }
}
