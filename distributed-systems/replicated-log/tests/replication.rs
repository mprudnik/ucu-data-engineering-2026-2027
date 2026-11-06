//! Iteration 1: blocking replication.
//!
//! All tests run with paused Tokio time: delays finish at once,
//! and `elapsed()` gives the exact time an append was blocked.

use replicated_log::core::NodeId;

use std::time::Duration;

use tokio::time::Instant;

mod harness;

const CLUSTER_SIZE: usize = 2;

const S1: NodeId = NodeId(1);
const S2: NodeId = NodeId(2);

/// Appends a message and returns how long the append was blocked.
async fn timed_append(cluster: &harness::Cluster, value: &str) -> anyhow::Result<Duration> {
    let start = Instant::now();
    cluster.append(value).await?;
    Ok(start.elapsed())
}

/// Sends go to all secondaries at the same time:
/// with delays of 5s and 6s, the append takes 6s, not 11s.
#[tokio::test(start_paused = true)]
async fn replication_runs_parallel() -> anyhow::Result<()> {
    let cluster = harness::Cluster::new(CLUSTER_SIZE);

    cluster.network.set_delay(S1, Duration::from_secs(5));
    cluster.network.set_delay(S2, Duration::from_secs(6));

    let elapsed = timed_append(&cluster, "m1").await?;
    assert_eq!(elapsed, Duration::from_secs(6));

    Ok(())
}

/// The append returns only after the slowest secondary has ACKed,
/// and by then every secondary has the message.
#[tokio::test(start_paused = true)]
async fn master_waits_for_acks_from_all_secondaries() -> anyhow::Result<()> {
    let cluster = harness::Cluster::new(CLUSTER_SIZE);

    // Only S2 is slow: the fast S1 must not let the append return early.
    cluster.network.set_delay(S2, Duration::from_secs(5));

    let elapsed = timed_append(&cluster, "m1").await?;
    assert_eq!(elapsed, Duration::from_secs(5));
    assert_eq!(cluster.secondary_values(S1).await, ["m1"]);
    assert_eq!(cluster.secondary_values(S2).await, ["m1"]);

    Ok(())
}

/// A slow ACK (response) also blocks the append, not only a slow request.
#[tokio::test(start_paused = true)]
async fn master_waits_for_slow_ack() -> anyhow::Result<()> {
    let cluster = harness::Cluster::new(CLUSTER_SIZE);

    cluster
        .network
        .set_response_delay(S1, Duration::from_secs(3));

    let elapsed = timed_append(&cluster, "m1").await?;
    assert_eq!(elapsed, Duration::from_secs(3));

    Ok(())
}

/// The scenario from the task document, step by step.
#[tokio::test(start_paused = true)]
async fn task_scenario() -> anyhow::Result<()> {
    let cluster = harness::Cluster::new(CLUSTER_SIZE);

    cluster.network.set_delay(S1, Duration::from_secs(5));
    assert_eq!(timed_append(&cluster, "m1").await?, Duration::from_secs(5));
    assert_eq!(cluster.master_values().await, ["m1"]);

    cluster.network.set_delay(S2, Duration::from_secs(6));
    assert_eq!(timed_append(&cluster, "m2").await?, Duration::from_secs(6));
    assert_eq!(cluster.master_values().await, ["m1", "m2"]);

    cluster.network.remove_delay(S1);
    cluster.network.remove_delay(S2);
    assert_eq!(timed_append(&cluster, "m3").await?, Duration::ZERO);

    assert_eq!(cluster.master_values().await, ["m1", "m2", "m3"]);
    assert_eq!(cluster.secondary_values(S1).await, ["m1", "m2", "m3"]);
    assert_eq!(cluster.secondary_values(S2).await, ["m1", "m2", "m3"]);

    Ok(())
}
