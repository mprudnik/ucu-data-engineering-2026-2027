use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Message {
    pub value: String,
    pub sequence_number: usize,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Replicate {
    pub message: Message,
}

impl super::Request for Replicate {
    const METHOD: &'static str = "replicate";
    type Response = ();
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Heartbeat;

impl super::Request for Heartbeat {
    const METHOD: &'static str = "Heartbeat";
    type Response = ();
}
