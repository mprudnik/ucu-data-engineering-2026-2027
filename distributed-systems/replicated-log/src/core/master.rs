use std::{collections::HashMap, sync::Arc};

use tokio::{sync::RwLock, task::JoinSet};

use super::{
    NodeId,
    message::{Message, Replicate},
};

pub struct Node<C: super::Client> {
    messages: RwLock<Vec<Message>>,
    secondaries: HashMap<NodeId, Arc<C>>,
}

impl<C: super::Client> Node<C> {
    pub fn new(secondaries: HashMap<NodeId, Arc<C>>) -> Self {
        let messages = RwLock::new(Vec::new());
        Self {
            messages,
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
            let secondary = secondary.clone();
            let message = message.clone();
            tasks.spawn(async move {
                let request = Replicate { message };
                secondary.send(request).await
            });
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
}
