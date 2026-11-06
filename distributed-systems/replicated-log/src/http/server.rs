//! HTTP server side: axum routers for the master and secondary nodes.
//!
//! Handlers only parse the request, call the core node and turn the result
//! into an HTTP response. No replication logic lives here.

use std::{sync::Arc, time::Duration};

use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use chrono::Utc;
use serde::Deserialize;

use crate::core::{
    self, Request, master,
    message::{Message, Replicate},
    secondary,
};

/// Body of the client `POST /append` request: `{"value": "..."}`.
#[derive(Debug, Deserialize)]
pub struct AppendBody {
    pub value: String,
}

/// Master routes:
/// - `POST /append` — append a message; returns after all secondaries ACK.
/// - `GET  /list`   — list all messages on the master.
pub fn master_router<C: core::Client>(node: Arc<master::Node<C>>) -> Router {
    Router::new()
        .route("/append", post(master_append::<C>))
        .route("/list", get(master_list::<C>))
        .with_state(node)
}

async fn master_append<C: core::Client>(
    State(node): State<Arc<master::Node<C>>>,
    Json(body): Json<AppendBody>,
) -> Result<(StatusCode, Json<Message>), core::Error> {
    // The master sets the timestamp; clients send only the text.
    let message = Message {
        value: body.value,
        timestamp: Utc::now(),
    };
    tracing::info!(value = %message.value, "append request received");
    node.append(message.clone()).await?;
    tracing::info!(value = %message.value, "append done, all secondaries ACKed");
    Ok((StatusCode::CREATED, Json(message)))
}

async fn master_list<C: core::Client>(
    State(node): State<Arc<master::Node<C>>>,
) -> Json<Vec<Message>> {
    Json(node.list().await)
}

/// Shared state of the secondary routes.
#[derive(Clone)]
struct SecondaryState {
    node: Arc<secondary::Node>,
    /// Sleep before handling each replicate request.
    /// Only used to show blocking replication in Docker.
    delay: Duration,
}

/// Secondary routes:
/// - `POST /replicate` — internal, called by the master; returns the ACK.
/// - `GET  /list`      — list all replicated messages.
pub fn secondary_router(node: Arc<secondary::Node>, delay: Duration) -> Router {
    let replicate_path = format!("/{}", <Replicate as Request>::METHOD);
    Router::new()
        .route(&replicate_path, post(secondary_replicate))
        .route("/list", get(secondary_list))
        .with_state(SecondaryState { node, delay })
}

async fn secondary_replicate(
    State(state): State<SecondaryState>,
    Json(req): Json<Replicate>,
) -> Json<<Replicate as Request>::Response> {
    tracing::info!(value = %req.message.value, "replicate request received");
    if !state.delay.is_zero() {
        tracing::info!(delay_ms = state.delay.as_millis() as u64, "delaying replicate");
        tokio::time::sleep(state.delay).await;
    }
    state.node.append(req.message).await;
    tracing::info!("replicate done, sending ACK");
    // `Replicate::Response` is `()`, which is sent as JSON `null`.
    Json(())
}

async fn secondary_list(State(state): State<SecondaryState>) -> Json<Vec<Message>> {
    Json(state.node.list().await)
}

/// A core error in a handler means a secondary could not be reached
/// or answered with an error, so the master replies `502 Bad Gateway`.
impl IntoResponse for core::Error {
    fn into_response(self) -> Response {
        tracing::error!(error = %self, "request failed");
        let body = Json(serde_json::json!({ "error": self.to_string() }));
        (StatusCode::BAD_GATEWAY, body).into_response()
    }
}

/// Binds `0.0.0.0:<port>` and serves the router until the process stops.
pub async fn serve(router: Router, port: u16) -> std::io::Result<()> {
    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(port, "listening");
    axum::serve(listener, router).await
}
