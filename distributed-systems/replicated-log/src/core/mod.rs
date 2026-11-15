use serde::{Serialize, de::DeserializeOwned};

pub mod master;
pub mod message;
pub mod secondary;

#[derive(Debug, Clone, Copy, std::cmp::Eq, std::cmp::PartialEq, std::hash::Hash)]
pub struct NodeId(pub usize);

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
}
