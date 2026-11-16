use serde::{Serialize, de::DeserializeOwned};

pub mod master;
pub mod message;
pub mod secondary;

#[derive(Debug, Clone, Copy, std::cmp::Eq, std::cmp::PartialEq, std::hash::Hash)]
pub struct NodeId(pub usize);

/// The master's view of a secondary, from heartbeats. As JSON: "healthy", "suspected", "unhealthy".
#[derive(Debug, Clone, Copy, PartialEq, Eq, derive_more::Display, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeHealth {
    Healthy,
    Suspected,
    Unhealthy,
}

pub trait Request: Serialize + DeserializeOwned + Send + 'static {
    const METHOD: &'static str;
    type Response: Serialize + DeserializeOwned + Send + 'static;
}

pub trait Client: Send + Sync + 'static {
    fn send<R: Request>(&self, req: R) -> impl Future<Output = Result<R::Response, Error>> + Send;
}

#[derive(Debug, derive_more::Display, derive_more::Error)]
pub enum Error {
    #[display("Transport error: {code} ({message})")]
    Transport { code: i32, message: String },
    /// Write concern is below 1 or above the number of nodes (master + secondaries).
    #[display("Invalid write concern {requested}: must be between 1 and {max}")]
    InvalidWriteConcern { requested: usize, max: usize },
    /// Fewer than a majority of nodes are available: the master is read-only.
    #[display("No quorum: {available} of {total} nodes available, {required} needed. Master is read-only")]
    NoQuorum {
        available: usize,
        required: usize,
        total: usize,
    },
}
