//! Blocking replication: write concern = all nodes (master + 2 secondaries).
//! This is the iteration 1 behaviour.
//!
//! All tests run with paused Tokio time: delays finish at once,
//! and `elapsed()` gives the exact time an append was blocked.

use std::time::Duration;

use harness::{Cluster, S1, S2, SECONDARIES};

mod harness;

/// Master + all secondaries.
const ALL: usize = SECONDARIES + 1;

/// Sends go to all secondaries at the same time.
///
/// With delays of 5s and 6s, the append takes 6s, not 11s.
#[tokio::test(start_paused = true)]
async fn replication_runs_parallel() -> anyhow::Result<()> {
    let cluster = Cluster::new(SECONDARIES);

    cluster.network.set_delay(S1, Duration::from_secs(5));
    cluster.network.set_delay(S2, Duration::from_secs(6));

    let elapsed = cluster.timed_append("m1", ALL).await?;
    assert_eq!(elapsed, Duration::from_secs(6));

    Ok(())
}

/// The append returns only after the slowest secondary has ACKed.
///
/// By then every secondary has the message.
#[tokio::test(start_paused = true)]
async fn master_waits_for_acks_from_all_secondaries() -> anyhow::Result<()> {
    let cluster = Cluster::new(SECONDARIES);

    // Only S2 is slow: the fast S1 must not let the append return early.
    cluster.network.set_delay(S2, Duration::from_secs(5));

    let elapsed = cluster.timed_append("m1", ALL).await?;
    assert_eq!(elapsed, Duration::from_secs(5));
    assert_eq!(cluster.secondary_values(S1).await, ["m1"]);
    assert_eq!(cluster.secondary_values(S2).await, ["m1"]);

    Ok(())
}

/// A slow ACK (response) also blocks the append, not only a slow request.
#[tokio::test(start_paused = true)]
async fn master_waits_for_slow_ack() -> anyhow::Result<()> {
    let cluster = Cluster::new(SECONDARIES);

    cluster
        .network
        .set_response_delay(S1, Duration::from_secs(3));

    let elapsed = cluster.timed_append("m1", ALL).await?;
    assert_eq!(elapsed, Duration::from_secs(3));

    Ok(())
}

/// The iteration 1 scenario from the task document, step by step.
#[tokio::test(start_paused = true)]
async fn iteration_1_task_scenario() -> anyhow::Result<()> {
    let cluster = Cluster::new(SECONDARIES);

    cluster.network.set_delay(S1, Duration::from_secs(5));
    assert_eq!(cluster.timed_append("m1", ALL).await?, Duration::from_secs(5));
    assert_eq!(cluster.master_values().await, ["m1"]);

    cluster.network.set_delay(S2, Duration::from_secs(6));
    assert_eq!(cluster.timed_append("m2", ALL).await?, Duration::from_secs(6));
    assert_eq!(cluster.master_values().await, ["m1", "m2"]);

    cluster.network.remove_delay(S1);
    cluster.network.remove_delay(S2);
    assert_eq!(cluster.timed_append("m3", ALL).await?, Duration::ZERO);

    assert_eq!(cluster.master_values().await, ["m1", "m2", "m3"]);
    assert_eq!(cluster.secondary_values(S1).await, ["m1", "m2", "m3"]);
    assert_eq!(cluster.secondary_values(S2).await, ["m1", "m2", "m3"]);

    Ok(())
}
