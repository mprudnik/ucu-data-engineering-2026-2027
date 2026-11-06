use tokio::sync::RwLock;

use super::message::Message;

#[derive(Default)]
pub struct Node {
    messages: RwLock<Vec<Message>>,
}

impl Node {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn append(&self, message: Message) {
        self.messages.write().await.push(message);
    }

    pub async fn list(&self) -> Vec<Message> {
        self.messages.read().await.clone()
    }
}
