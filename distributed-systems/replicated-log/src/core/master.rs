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

    pub async fn append(&self, message: Message) -> Result<(), super::Error> {
        self.messages.write().await.push(message.clone());

        let mut tasks = JoinSet::new();
        for secondary in self.secondaries.values() {
            let secondary = secondary.clone();
            let message = message.clone();
            tasks.spawn(async move {
                let request = Replicate { message };
                secondary.send(request).await
            });
        }

        let results = tasks.join_all().await;
        for result in results {
            result?;
        }

        Ok(())
    }

    pub async fn list(&self) -> Vec<Message> {
        self.messages.read().await.clone()
    }
}
