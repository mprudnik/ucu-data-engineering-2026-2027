//! Heartbeats: the master checks each secondary regularly and keeps a status:
//! Healthy -> Suspected -> Unhealthy, by time since the last successful heartbeat.
//! One successful heartbeat makes a secondary Healthy again.
//!
//! The status changes retries:
//! - Healthy: pause of `retry_delay * attempt` (up to `max_retry_delay`);
//! - Suspected: twice that pause;
//! - Unhealthy: no sends; the master checks the status again every `max_retry_delay`.
//!
//! All tests run with paused Tokio time, so send times are exact.

use std::time::Duration;

use harness::{Cluster, NodeHealth, S1, S2, SECONDARIES, settle};
use replicated_log::core::{
    Request, master,
    message::{Heartbeat, Replicate},
};

mod harness;

const NO_MESSAGES: [&str; 0] = [];

/// Default settings (heartbeat every 1s, Suspected after 3s, Unhealthy after 10s,
/// retry delay 1s, max retry delay 30s), with a short request timeout.
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

/// Time for the master to resend after a secondary recovers:
/// longer than the timeout plus the max retry delay.
const RETRY_WAIT: Duration = Duration::from_secs(60);

/// All secondaries start Healthy, and the master keeps sending heartbeats.
#[tokio::test(start_paused = true)]
async fn secondaries_start_healthy() -> anyhow::Result<()> {
    let config = heartbeat_config();
    let cluster = Cluster::with_config(config.clone(), SECONDARIES);
    cluster.start_heartbeats();

    tokio::time::sleep(config.heartbeat_interval * 3).await;
    settle().await;

    assert_eq!(cluster.health(S1).await, NodeHealth::Healthy);
    assert_eq!(cluster.health(S2).await, NodeHealth::Healthy);
    let heartbeats = cluster.network.sent_requests(S1, Heartbeat::METHOD);
    assert!(heartbeats >= 3, "expected regular heartbeats, got {heartbeats}");

    Ok(())
}

/// A stopped secondary fails every heartbeat:
/// Suspected after `suspected_after`, Unhealthy after `unhealthy_after`.
/// The other secondary stays Healthy.
#[tokio::test(start_paused = true)]
async fn stopped_secondary_becomes_suspected_then_unhealthy() -> anyhow::Result<()> {
    let config = heartbeat_config();
    let cluster = Cluster::with_config(config.clone(), SECONDARIES);
    cluster.network.stop_secondary(S1);
    cluster.start_heartbeats();

    tokio::time::sleep(after(config.suspected_after, &config)).await;
    assert_eq!(cluster.health(S1).await, NodeHealth::Suspected);
    assert_eq!(cluster.health(S2).await, NodeHealth::Healthy);

    tokio::time::sleep(config.unhealthy_after).await;
    assert_eq!(cluster.health(S1).await, NodeHealth::Unhealthy);
    assert_eq!(cluster.health(S2).await, NodeHealth::Healthy);

    Ok(())
}

/// Lost heartbeats (no reply until the request timeout) count as missed, too.
#[tokio::test(start_paused = true)]
async fn lost_heartbeats_make_secondary_suspected() -> anyhow::Result<()> {
    let config = heartbeat_config();
    let cluster = Cluster::with_config(config.clone(), SECONDARIES);
    cluster.network.drop_requests(S1);
    cluster.start_heartbeats();

    tokio::time::sleep(after(config.suspected_after, &config)).await;
    assert_eq!(cluster.health(S1).await, NodeHealth::Suspected);

    Ok(())
}

/// One successful heartbeat makes an Unhealthy secondary Healthy again.
#[tokio::test(start_paused = true)]
async fn secondary_becomes_healthy_after_one_heartbeat() -> anyhow::Result<()> {
    let config = heartbeat_config();
    let cluster = Cluster::with_config(config.clone(), SECONDARIES);
    cluster.network.stop_secondary(S1);
    cluster.start_heartbeats();

    tokio::time::sleep(after(config.unhealthy_after, &config)).await;
    assert_eq!(cluster.health(S1).await, NodeHealth::Unhealthy);

    cluster.network.start_secondary(S1);
    tokio::time::sleep(config.heartbeat_interval * 2).await;
    assert_eq!(cluster.health(S1).await, NodeHealth::Healthy);

    Ok(())
}

/// Healthy secondary: the first retry comes after `retry_delay`.
#[tokio::test(start_paused = true)]
async fn healthy_secondary_is_retried_after_retry_delay() -> anyhow::Result<()> {
    let config = heartbeat_config();
    let cluster = Cluster::with_config(config.clone(), SECONDARIES);
    cluster.network.stop_secondary(S1);
    cluster.start_heartbeats();

    // Still Healthy: the stop is too recent.
    assert_eq!(cluster.health(S1).await, NodeHealth::Healthy);
    cluster.append("m1", 1).await?;
    tokio::time::sleep(config.retry_delay * 2).await;

    let sent = cluster.network.sent_times(S1, Replicate::METHOD);
    assert!(sent.len() >= 2, "m1 should be retried, was sent {} times", sent.len());
    assert_eq!(sent[1] - sent[0], config.retry_delay);

    Ok(())
}

/// Suspected secondary: the first retry comes after twice the `retry_delay`.
#[tokio::test(start_paused = true)]
async fn suspected_secondary_is_retried_after_double_delay() -> anyhow::Result<()> {
    let config = heartbeat_config();
    let cluster = Cluster::with_config(config.clone(), SECONDARIES);
    cluster.network.stop_secondary(S1);
    cluster.start_heartbeats();

    tokio::time::sleep(after(config.suspected_after, &config)).await;
    assert_eq!(cluster.health(S1).await, NodeHealth::Suspected);

    cluster.append("m1", 1).await?;
    tokio::time::sleep(config.retry_delay * 3).await;
    // Still before Unhealthy, so both attempts were made while Suspected.
    assert_eq!(cluster.health(S1).await, NodeHealth::Suspected);

    let sent = cluster.network.sent_times(S1, Replicate::METHOD);
    assert!(sent.len() >= 2, "m1 should be retried, was sent {} times", sent.len());
    assert_eq!(sent[1] - sent[0], config.retry_delay * 2);

    Ok(())
}

/// Unhealthy secondary: the master sends it no messages, only heartbeats.
/// When it is Healthy again, it gets every message it missed.
#[tokio::test(start_paused = true)]
async fn unhealthy_secondary_gets_no_messages_until_healthy() -> anyhow::Result<()> {
    let config = heartbeat_config();
    let cluster = Cluster::with_config(config.clone(), SECONDARIES);
    cluster.network.stop_secondary(S1);
    cluster.start_heartbeats();

    tokio::time::sleep(after(config.unhealthy_after, &config)).await;
    assert_eq!(cluster.health(S1).await, NodeHealth::Unhealthy);
    let heartbeats_before = cluster.network.sent_requests(S1, Heartbeat::METHOD);

    assert_eq!(cluster.timed_append("m1", 1).await?, Duration::ZERO);
    assert_eq!(cluster.timed_append("m2", 1).await?, Duration::ZERO);
    tokio::time::sleep(Duration::from_secs(20)).await;

    assert_eq!(cluster.network.sent_requests(S1, Replicate::METHOD), 0);
    assert!(cluster.network.sent_requests(S1, Heartbeat::METHOD) > heartbeats_before);
    assert_eq!(cluster.secondary_values(S1).await, NO_MESSAGES);
    assert_eq!(cluster.secondary_values(S2).await, ["m1", "m2"]);

    cluster.network.start_secondary(S1);
    tokio::time::sleep(config.heartbeat_interval * 2).await;
    assert_eq!(cluster.health(S1).await, NodeHealth::Healthy);

    tokio::time::sleep(RETRY_WAIT).await;
    assert_eq!(cluster.secondary_values(S1).await, ["m1", "m2"]);

    Ok(())
}
