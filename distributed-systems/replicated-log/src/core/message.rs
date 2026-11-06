use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Message {
    pub value: String,
    pub timestamp: DateTime<Utc>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Replicate {
    pub message: Message,
}

impl super::Request for Replicate {
    const METHOD: &'static str = "replicate";
    type Response = ();
}
