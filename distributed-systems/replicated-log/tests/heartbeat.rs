//! Heartbeats: the master checks each secondary regularly and keeps a status:
//! Healthy -> Suspected -> Unhealthy, by time since the last successful heartbeat.
//! One successful heartbeat makes a secondary Healthy again.
//!
//! The status changes retries:
//! - Healthy: pause of `retry_delay * attempt` (up to `max_retry_delay`);
//! - Suspected: twice that pause;
//! - Unhealthy: no sends; after the recovery, the secondary gets every message it missed.
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

/// Returns the default settings with a short request timeout.
///
/// Defaults: heartbeat every 1s, Suspected after 3s, Unhealthy after 10s,
/// retry delay 1s, max retry delay 30s.
fn heartbeat_config() -> master::Config {
    master::Config {
        request_timeout: Duration::from_secs(1),
        ..Default::default()
    }
}

/// Returns the time for a status to change after `threshold`.
///
/// The master sees the change only on the next heartbeat,
/// so two heartbeat intervals are added.
fn after(threshold: Duration, config: &master::Config) -> Duration {
    threshold + config.heartbeat_interval * 2
}

/// Time for the master to resend after a secondary recovers.
///
/// Longer than the timeout plus the max retry delay.
const RETRY_WAIT: Duration = Duration::from_secs(60);

/// All secondaries start Healthy.
///
/// The master keeps sending heartbeats.
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

/// A stopped secondary becomes Suspected, then Unhealthy.
///
/// It fails every heartbeat: Suspected after `suspected_after`,
/// Unhealthy after `unhealthy_after`. The other secondary stays Healthy.
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

/// Lost heartbeats count as missed, too.
///
/// A lost heartbeat gets no reply until the request timeout.
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

/// The first retry to a Healthy secondary comes after `retry_delay`.
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

/// The first retry to a Suspected secondary comes after twice the `retry_delay`.
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

/// The master sends an Unhealthy secondary no messages, only heartbeats.
///
/// When the secondary is Healthy again, it gets every message it missed.
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

/// A message not sent to an Unhealthy secondary is not its ACK.
///
/// With write concern 3 and S2 Unhealthy, the append is blocked until S2
/// recovers and has the message (task: "w=3, the client should be blocked
/// until the node becomes available").
#[tokio::test(start_paused = true)]
async fn unhealthy_secondary_does_not_count_as_ack() -> anyhow::Result<()> {
    let config = heartbeat_config();
    let cluster = Cluster::with_config(config.clone(), SECONDARIES);
    let s2_down_time = Duration::from_secs(20);
    cluster.network.stop_secondary(S2);
    cluster.start_heartbeats();

    tokio::time::sleep(after(config.unhealthy_after, &config)).await;
    assert_eq!(cluster.health(S2).await, NodeHealth::Unhealthy);

    let (m1_elapsed, ()) = tokio::join!(cluster.timed_append("m1", 3), async {
        tokio::time::sleep(s2_down_time).await;
        assert_eq!(cluster.secondary_values(S2).await, NO_MESSAGES);
        cluster.network.start_secondary(S2);
    });

    let m1_elapsed = m1_elapsed?;
    assert!(
        m1_elapsed >= s2_down_time,
        "append with write concern 3 should be blocked while S2 is unhealthy, returned after {m1_elapsed:?}",
    );
    assert_eq!(cluster.secondary_values(S2).await, ["m1"]);

    Ok(())
}
