//! Log messages and the requests the master sends to secondaries.

use serde::{Deserialize, Serialize};

/// One message of the log.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Message {
    /// Text of the message, as the client sent it.
    pub value: String,
    /// Position in the log, from 1. The master sets it.
    pub sequence_number: usize,
}

/// Request to store one message on a secondary.
///
/// The response `()` is the ACK.
#[derive(Debug, Deserialize, Serialize)]
pub struct Replicate {
    /// The message to store.
    pub message: Message,
}

impl super::Request for Replicate {
    const METHOD: &'static str = "replicate";
    type Response = ();
}

/// Request to check that a secondary is up.
///
/// Any reply means the secondary is up.
#[derive(Debug, Deserialize, Serialize)]
pub struct Heartbeat;

impl super::Request for Heartbeat {
    const METHOD: &'static str = "Heartbeat";
    type Response = ();
}
