//! HTTP server side: axum routers for the master and secondary nodes.
//!
//! Handlers only parse the request, call the core node and turn the result
//! into an HTTP response. No replication logic lives here.

use std::{collections::BTreeMap, sync::Arc};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::Deserialize;

use crate::core::{
    self, Request, master,
    message::{Heartbeat, Replicate},
    secondary,
};

/// Body of the client `POST /append` request.
///
/// ```json
/// {"value": "m1", "write_concern": 2}
/// ```
#[derive(Debug, Deserialize)]
pub struct AppendBody {
    /// Text of the message.
    pub value: String,
    /// How many nodes must have the message before the reply.
    ///
    /// `1` is the master only, `2` is the master + 1 secondary,
    /// `3` is the master + 2 secondaries.
    pub write_concern: usize,
}

/// Builds the router of the master.
///
/// Routes:
/// - `POST /append` appends a message and returns after `write_concern` nodes have it;
/// - `GET /list` lists all messages on the master;
/// - `GET /health` returns the health of each secondary, by id:
///   `{"1": "healthy", "2": "suspected"}`.
pub fn master_router<C: core::Client>(node: Arc<master::Node<C>>) -> Router {
    Router::new()
        .route("/append", post(master_append::<C>))
        .route("/list", get(master_list::<C>))
        .route("/health", get(master_health::<C>))
        .with_state(node)
}

async fn master_append<C: core::Client>(
    State(node): State<Arc<master::Node<C>>>,
    Json(body): Json<AppendBody>,
) -> Result<StatusCode, core::Error> {
    let write_concern = body.write_concern;
    tracing::info!(value = %body.value, write_concern, "append request received");
    // The master builds the message (with its sequence number); clients send only the text.
    node.append(body.value, write_concern).await?;
    tracing::info!(write_concern, "append done, write concern reached");
    Ok(StatusCode::CREATED)
}

async fn master_list<C: core::Client>(
    State(node): State<Arc<master::Node<C>>>,
) -> Json<Vec<String>> {
    Json(node.list().await)
}

/// Returns the health of each secondary, ordered by id.
///
/// The id is the JSON key.
async fn master_health<C: core::Client>(
    State(node): State<Arc<master::Node<C>>>,
) -> Json<BTreeMap<usize, core::NodeHealth>> {
    let health = node.health().await;
    Json(
        health
            .into_iter()
            .map(|(id, status)| (id.0, status))
            .collect(),
    )
}

/// Builds the router of a secondary.
///
/// Routes:
/// - `POST /replicate` stores a message and returns the ACK (internal, called by the master);
/// - `POST /heartbeat` replies to show that the secondary is up (internal, called by the master);
/// - `GET /list` lists all replicated messages.
pub fn secondary_router(node: Arc<secondary::Node>) -> Router {
    let replicate_path = format!("/{}", <Replicate as Request>::METHOD);
    let heartbeat_path = format!("/{}", <Heartbeat as Request>::METHOD);
    Router::new()
        .route(&replicate_path, post(secondary_replicate))
        .route(&heartbeat_path, post(secondary_heartbeat))
        .route("/list", get(secondary_list))
        .with_state(node)
}

async fn secondary_heartbeat(Json(_): Json<Heartbeat>) -> Json<<Heartbeat as Request>::Response> {
    tracing::debug!("heartbeat received");
    Json(())
}

async fn secondary_replicate(
    State(node): State<Arc<secondary::Node>>,
    Json(req): Json<Replicate>,
) -> Json<<Replicate as Request>::Response> {
    let sequence_number = req.message.sequence_number;
    tracing::info!(sequence_number, value = %req.message.value, "replicate request received");
    node.append(req.message).await;
    tracing::info!(sequence_number, "replicate done, sending ACK");
    // `Replicate::Response` is `()`, which is sent as JSON `null`.
    Json(())
}

async fn secondary_list(State(node): State<Arc<secondary::Node>>) -> Json<Vec<String>> {
    Json(node.list().await)
}

/// Converts a core error into an HTTP reply.
///
/// Status codes:
/// - `400 Bad Request` if the write concern is out of range;
/// - `502 Bad Gateway` if a secondary could not be reached or failed;
/// - `503 Service Unavailable` if there is no quorum.
impl IntoResponse for core::Error {
    fn into_response(self) -> Response {
        let status = match self {
            core::Error::InvalidWriteConcern { .. } => StatusCode::BAD_REQUEST,
            core::Error::Transport { .. } => StatusCode::BAD_GATEWAY,
            core::Error::NoQuorum { .. } => StatusCode::SERVICE_UNAVAILABLE,
        };
        tracing::error!(error = %self, %status, "request failed");
        let body = Json(serde_json::json!({ "error": self.to_string() }));
        (status, body).into_response()
    }
}

/// Binds `0.0.0.0:<port>` and serves the router until the process stops.
///
/// # Errors
///
/// Returns an error if the port cannot be bound or the server fails.
pub async fn serve(router: Router, port: u16) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, "listening");
    axum::serve(listener, router).await
}
