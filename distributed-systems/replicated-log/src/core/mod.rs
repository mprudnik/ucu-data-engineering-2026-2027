//! Log and replication logic, without HTTP.
//!
//! Nodes talk to each other only through the [`Client`] trait.

use serde::{Serialize, de::DeserializeOwned};

pub mod master;
pub mod message;
pub mod secondary;

/// Id of a secondary, from 1.
#[derive(Debug, Clone, Copy, std::cmp::Eq, std::cmp::PartialEq, std::hash::Hash)]
pub struct NodeId(pub usize);

/// Health of a secondary, as the master sees it from heartbeats.
///
/// Serialized as `"healthy"`, `"suspected"` or `"unhealthy"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeHealth {
    /// The last heartbeat succeeded less than `suspected_after` ago.
    Healthy,
    /// No successful heartbeat for more than `suspected_after`.
    Suspected,
    /// No successful heartbeat for more than `unhealthy_after`.
    Unhealthy,
}

/// A request the master sends to a secondary.
pub trait Request: Serialize + DeserializeOwned + Send + 'static {
    /// Name of the request. The HTTP layer uses it as the path.
    const METHOD: &'static str;
    /// What the secondary sends back.
    type Response: Serialize + DeserializeOwned + Send + 'static;
}

/// Connection from the master to one secondary.
pub trait Client: Send + Sync + 'static {
    /// Sends `req` to the secondary and waits for the response.
    ///
    /// Has no timeout of its own. The master adds one.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Transport`] if the secondary cannot be reached
    /// or replies with an error.
    fn send<R: Request>(&self, req: R) -> impl Future<Output = Result<R::Response, Error>> + Send;
}

/// Errors of the core logic.
#[derive(Debug, derive_more::Display, derive_more::Error)]
pub enum Error {
    /// A request to a secondary failed.
    #[display("Transport error: {code} ({message})")]
    Transport {
        /// HTTP status code, or 0 if there is none.
        code: i32,
        /// Description of the failure.
        message: String,
    },
    /// Write concern is below 1 or above the number of nodes (master + secondaries).
    #[display("Invalid write concern {requested}: must be between 1 and {max}")]
    InvalidWriteConcern {
        /// The write concern from the client.
        requested: usize,
        /// The number of nodes.
        max: usize,
    },
    /// Fewer than a majority of nodes are available, so the master is read-only.
    #[display("No quorum: {available} of {total} nodes available, {required} needed. Master is read-only")]
    NoQuorum {
        /// Nodes that count as available, the master included.
        available: usize,
        /// Nodes needed for a majority.
        required: usize,
        /// All nodes, the master included.
        total: usize,
    },
}
