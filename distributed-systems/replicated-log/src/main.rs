//! Binary entry point.
//!
//! Parses the command line (or environment variables), sets up logging and
//! starts one node: a master or a secondary.
//!
//! ```text
//! replicated-log master    --port 8080 --secondaries http://s1:8080,http://s2:8080
//! replicated-log secondary --port 8080 --delay-ms 5000
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
    },
    /// Stores messages replicated by the master.
    Secondary {
        /// Port for the HTTP server.
        #[arg(long, env = "PORT", default_value_t = 8080)]
        port: u16,
        /// Sleep before each ACK, in milliseconds. Used to show blocking replication.
        #[arg(long, env = "DELAY_MS", default_value_t = 0)]
        delay_ms: u64,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Log level comes from RUST_LOG, default is "info".
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    match Cli::parse().role {
        Role::Master { port, secondaries } => {
            tracing::info!(?secondaries, "starting master");
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
            let node = Arc::new(master::Node::new(clients));
            server::serve(server::master_router(node), port).await?;
        }
        Role::Secondary { port, delay_ms } => {
            tracing::info!(delay_ms, "starting secondary");
            let node = Arc::new(secondary::Node::new());
            let router = server::secondary_router(node, Duration::from_millis(delay_ms));
            server::serve(router, port).await?;
        }
    }
    Ok(())
}
