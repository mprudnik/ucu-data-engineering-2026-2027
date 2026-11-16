//! Binary entry point.
//!
//! Parses the command line (or environment variables), sets up logging and
//! starts one node: a master or a secondary.
//!
//! ```text
//! replicated-log master    --port 8080 --secondaries http://s1:8080,http://s2:8080
//! replicated-log secondary --port 8080
//! ```

use std::{sync::Arc, time::Duration};

use clap::{Parser, Subcommand};
use replicated_log::{
    core::{NodeId, master, secondary},
    http::{client, server},
};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(version, about = "Replicated log node")]
struct Cli {
    #[command(subcommand)]
    role: Role,
}

#[derive(Debug, Subcommand)]
enum Role {
    /// Accepts appends from clients and replicates them to all secondaries.
    Master {
        /// Port for the HTTP server.
        #[arg(long, env = "PORT", default_value_t = 8080)]
        port: u16,
        /// Base URLs of the secondaries, comma separated.
        #[arg(long, env = "SECONDARIES", value_delimiter = ',', required = true)]
        secondaries: Vec<String>,
        /// How long to wait for a secondary's ACK before sending again, in milliseconds.
        #[arg(long, env = "REQUEST_TIMEOUT_MS", default_value_t = 5000)]
        request_timeout_ms: u64,
        /// Pause before sending a failed message again, in milliseconds.
        #[arg(long, env = "RETRY_DELAY_MS", default_value_t = 1000)]
        retry_delay_ms: u64,
        /// Upper limit for the pause between attempts, which grows with each attempt, in milliseconds.
        #[arg(long, env = "MAX_RETRY_DELAY_MS", default_value_t = 30_000)]
        max_retry_delay_ms: u64,
    },
    /// Stores messages replicated by the master.
    Secondary {
        /// Port for the HTTP server.
        #[arg(long, env = "PORT", default_value_t = 8080)]
        port: u16,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Log level comes from RUST_LOG, default is "info".
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    match Cli::parse().role {
        Role::Master {
            port,
            secondaries,
            request_timeout_ms,
            retry_delay_ms,
            max_retry_delay_ms,
        } => {
            tracing::info!(
                ?secondaries,
                request_timeout_ms,
                retry_delay_ms,
                max_retry_delay_ms,
                "starting master"
            );
            let config = master::Config {
                request_timeout: Duration::from_millis(request_timeout_ms),
                retry_delay: Duration::from_millis(retry_delay_ms),
                max_retry_delay: Duration::from_millis(max_retry_delay_ms),
            };
            // Secondaries get ids 1, 2, ... in the order of the --secondaries list.
            let clients = secondaries
                .into_iter()
                .enumerate()
                .map(|(i, url)| {
                    let url = url.trim_end_matches('/').to_string();
                    let client = Arc::new(client::Client::new(&client::Config { url }));
                    (NodeId(i + 1), client)
                })
                .collect();
            let node = Arc::new(master::Node::new(config, clients));
            server::serve(server::master_router(node), port).await?;
        }
        Role::Secondary { port } => {
            tracing::info!("starting secondary");
            let node = Arc::new(secondary::Node::new());
            server::serve(server::secondary_router(node), port).await?;
        }
    }
    Ok(())
}
