//! Secondary node: stores messages replicated by the master.

use std::collections::BTreeMap;

use tokio::sync::RwLock;

use super::message::Message;

/// A secondary node.
///
/// Stores messages by sequence number. They can arrive in any order
/// and more than once.
#[derive(Default)]
pub struct Node {
    messages: RwLock<BTreeMap<usize, String>>,
}

impl Node {
    /// Creates a secondary with an empty log.
    pub fn new() -> Self {
        Self::default()
    }

    /// Stores a message.
    ///
    /// A message with a sequence number that is already stored is ignored.
    /// The first copy is kept.
    pub async fn append(&self, message: Message) {
        let mut messages = self.messages.write().await;

        if messages.contains_key(&message.sequence_number) {
            return;
        }

        messages.insert(message.sequence_number, message.value);
    }

    /// Returns the stored messages in sequence-number order.
    ///
    /// Stops at the first gap: a message is listed only when all earlier
    /// messages are stored.
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
