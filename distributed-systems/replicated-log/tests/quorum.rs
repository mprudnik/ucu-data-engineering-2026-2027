//! Quorum append: the master accepts appends only while a majority of nodes
//! (master + secondaries) is available. Otherwise it is read-only.
//! The master always counts; a secondary counts unless it is Unhealthy.
//! With 3 nodes the quorum is 2: the master and at least one secondary.
//!
//! All tests run with paused Tokio time.

use std::time::Duration;

use harness::{Cluster, NodeHealth, S1, S2, SECONDARIES};
use replicated_log::core::{self, master};

mod harness;

const NO_MESSAGES: [&str; 0] = [];

/// Default settings (heartbeat every 1s, Suspected after 3s, Unhealthy after 10s),
/// with a short request timeout.
fn heartbeat_config() -> master::Config {
    master::Config {
        request_timeout: Duration::from_secs(1),
        ..Default::default()
    }
}

/// Time for a status to change after its threshold: the master sees it
/// only on the next heartbeat, so add two heartbeat intervals.
fn after(threshold: Duration, config: &master::Config) -> Duration {
    threshold + config.heartbeat_interval * 2
}

/// Both secondaries Unhealthy: 1 of 3 nodes, no quorum.
/// Every append is rejected, even with write concern 1, and nothing is stored.
#[tokio::test(start_paused = true)]
async fn append_is_rejected_without_quorum() -> anyhow::Result<()> {
    let config = heartbeat_config();
    let cluster = Cluster::with_config(config.clone(), SECONDARIES);
    cluster.network.stop_secondary(S1);
    cluster.network.stop_secondary(S2);
    cluster.start_heartbeats();

    tokio::time::sleep(after(config.unhealthy_after, &config)).await;
    assert_eq!(cluster.health(S1).await, NodeHealth::Unhealthy);
    assert_eq!(cluster.health(S2).await, NodeHealth::Unhealthy);

    let result = cluster.append("m1", 1).await;
    assert!(
        matches!(
            result,
            Err(core::Error::NoQuorum {
                available: 1,
                required: 2,
                total: 3
            })
        ),
        "append should be rejected, got {result:?}",
    );
    assert_eq!(cluster.master_values().await, NO_MESSAGES);

    Ok(())
}

/// One secondary Unhealthy: 2 of 3 nodes, still a quorum.
#[tokio::test(start_paused = true)]
async fn append_is_accepted_with_one_unhealthy_secondary() -> anyhow::Result<()> {
    let config = heartbeat_config();
    let cluster = Cluster::with_config(config.clone(), SECONDARIES);
    cluster.network.stop_secondary(S2);
    cluster.start_heartbeats();

    tokio::time::sleep(after(config.unhealthy_after, &config)).await;
    assert_eq!(cluster.health(S2).await, NodeHealth::Unhealthy);

    assert_eq!(cluster.timed_append("m1", 1).await?, Duration::ZERO);
    assert_eq!(cluster.timed_append("m2", 2).await?, Duration::ZERO);
    assert_eq!(cluster.master_values().await, ["m1", "m2"]);

    Ok(())
}

/// Suspected secondaries still count: both Suspected is 3 of 3 nodes.
#[tokio::test(start_paused = true)]
async fn suspected_secondaries_count_for_quorum() -> anyhow::Result<()> {
    let config = heartbeat_config();
    let cluster = Cluster::with_config(config.clone(), SECONDARIES);
    cluster.network.stop_secondary(S1);
    cluster.network.stop_secondary(S2);
    cluster.start_heartbeats();

    tokio::time::sleep(after(config.suspected_after, &config)).await;
    assert_eq!(cluster.health(S1).await, NodeHealth::Suspected);
    assert_eq!(cluster.health(S2).await, NodeHealth::Suspected);

    assert_eq!(cluster.timed_append("m1", 1).await?, Duration::ZERO);
    assert_eq!(cluster.master_values().await, ["m1"]);

    Ok(())
}

/// The master leaves read-only mode as soon as one secondary is Healthy again.
#[tokio::test(start_paused = true)]
async fn master_accepts_appends_again_when_quorum_returns() -> anyhow::Result<()> {
    let config = heartbeat_config();
    let cluster = Cluster::with_config(config.clone(), SECONDARIES);
    cluster.network.stop_secondary(S1);
    cluster.network.stop_secondary(S2);
    cluster.start_heartbeats();

    tokio::time::sleep(after(config.unhealthy_after, &config)).await;
    assert!(cluster.append("m1", 1).await.is_err());

    cluster.network.start_secondary(S1);
    tokio::time::sleep(config.heartbeat_interval * 2).await;
    assert_eq!(cluster.health(S1).await, NodeHealth::Healthy);

    assert_eq!(cluster.timed_append("m2", 2).await?, Duration::ZERO);
    assert_eq!(cluster.master_values().await, ["m2"]);
    assert_eq!(cluster.secondary_values(S1).await, ["m2"]);

    Ok(())
}
