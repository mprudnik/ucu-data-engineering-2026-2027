use std::collections::BTreeMap;

use tokio::sync::RwLock;

use super::message::Message;

#[derive(Default)]
pub struct Node {
    messages: RwLock<BTreeMap<usize, String>>,
}

impl Node {
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn append(&self, message: Message) {
        let mut messages = self.messages.write().await;

        if messages.contains_key(&message.sequence_number) {
            return;
        }

        messages.insert(message.sequence_number, message.value);
    }

    pub async fn list(&self) -> Vec<String> {
        let all_messages = self.messages.read().await;

        let mut ordered_messages = Vec::with_capacity(all_messages.len());

        for (sequence_number, value) in all_messages.iter() {
            let next_sequence_number = ordered_messages.len() + 1;
            if *sequence_number != next_sequence_number {
                break;
            }
            ordered_messages.push(value.clone());
        }

        ordered_messages
    }
}
