//! Retries: the master sends a message again until the secondary ACKs it.
//! Each message is stored once (deduplication) and listed in total order.
//!
//! All tests run with paused Tokio time: delays and timeouts finish at once,
//! and `elapsed()` gives the exact time an append was blocked.

use std::time::Duration;

use harness::{Cluster, S1, S2, SECONDARIES, settle};
use replicated_log::core::{Request, master, message::Replicate};

mod harness;

const NO_MESSAGES: [&str; 0] = [];

/// Returns the default settings with a short request timeout.
///
/// A lost or slow message is sent again within a few seconds.
/// Defaults: retry delay 1s, max retry delay 30s.
fn retry_config() -> master::Config {
    master::Config {
        request_timeout: Duration::from_secs(2),
        ..Default::default()
    }
}

/// Time for the master to notice a failure and send again.
///
/// Longer than the timeout plus the max retry delay.
const RETRY_WAIT: Duration = Duration::from_secs(60);

/// Test "retry" from the task document.
///
/// Requests to S1 are lost for 10s. The master keeps sending m1,
/// and S1 gets it after the link is back.
#[tokio::test(start_paused = true)]
async fn lost_message_is_retried() -> anyhow::Result<()> {
    let cluster = Cluster::with_config(retry_config(), SECONDARIES);
    cluster.network.drop_requests(S1);

    // Write concern 1: the append does not wait for S1.
    assert_eq!(cluster.timed_append("m1", 1).await?, Duration::ZERO);
    tokio::time::sleep(Duration::from_secs(10)).await;

    assert_eq!(cluster.secondary_values(S1).await, NO_MESSAGES);
    let sent = cluster.network.sent_requests(S1, Replicate::METHOD);
    assert!(sent >= 2, "m1 should be sent several times, was sent {sent} times");

    cluster.network.remove_drop(S1);
    tokio::time::sleep(RETRY_WAIT).await;

    assert_eq!(cluster.secondary_values(S1).await, ["m1"]);

    Ok(())
}

/// Test "retry and deduplication" from the task document.
///
/// Requests to S2 take 10s, longer than the master's timeout. The master sends m2
/// again, and the late copies still reach S2. S2 must store m2 only once.
#[tokio::test(start_paused = true)]
async fn slow_message_is_retried_and_stored_once() -> anyhow::Result<()> {
    let cluster = Cluster::with_config(retry_config(), SECONDARIES);

    cluster.append("m1", 1).await?;
    settle().await;

    cluster.network.set_delay(S2, Duration::from_secs(10));
    assert_eq!(cluster.timed_append("m2", 1).await?, Duration::ZERO);
    tokio::time::sleep(Duration::from_secs(10)).await;

    cluster.network.remove_delay(S2);
    tokio::time::sleep(RETRY_WAIT).await;

    assert_eq!(cluster.secondary_values(S2).await, ["m1", "m2"]);
    // m1 once, m2 at least twice: S2 got duplicates of m2.
    let delivered = cluster.network.delivered_requests(S2, Replicate::METHOD);
    assert!(delivered >= 3, "S2 should get m2 more than once, got {delivered} requests");

    Ok(())
}

/// A lost ACK is retried, and the message is stored once.
///
/// S1 stores m1, but its ACKs are lost. The master sends m1 again until an ACK arrives.
/// With write concern 3, the append waits for that ACK. S1 must store m1 only once.
#[tokio::test(start_paused = true)]
async fn lost_ack_is_retried_and_message_stored_once() -> anyhow::Result<()> {
    let cluster = Cluster::with_config(retry_config(), SECONDARIES);
    cluster.network.drop_responses(S1);
    let lost_ack_time = Duration::from_secs(10);

    let (m1_elapsed, ()) = tokio::join!(cluster.timed_append("m1", 3), async {
        tokio::time::sleep(lost_ack_time).await;
        // S1 already has m1, only the ACKs are lost.
        assert_eq!(cluster.secondary_values(S1).await, ["m1"]);
        cluster.network.remove_response_drop(S1);
    });
    assert!(m1_elapsed? >= lost_ack_time);

    assert_eq!(cluster.secondary_values(S1).await, ["m1"]);
    let delivered = cluster.network.delivered_requests(S1, Replicate::METHOD);
    assert!(delivered >= 2, "S1 should get m1 more than once, got {delivered} requests");

    Ok(())
}

/// Test "total order" from the task document.
///
/// m3 to S1 is lost, m4 arrives. S1 hides m4 until the retry brings m3.
#[tokio::test(start_paused = true)]
async fn secondary_hides_messages_until_retry_fills_the_gap() -> anyhow::Result<()> {
    // Retry delay big enough to check S1 before m3 is sent again.
    let config = master::Config {
        retry_delay: Duration::from_secs(10),
        ..retry_config()
    };
    let cluster = Cluster::with_config(config, SECONDARIES);

    cluster.append("m1", 1).await?;
    cluster.append("m2", 1).await?;
    settle().await;

    cluster.network.drop_requests(S1);
    cluster.append("m3", 1).await?;
    // Let m3's send start, so it reads the drop before we remove it.
    settle().await;
    cluster.network.remove_drop(S1);

    cluster.append("m4", 1).await?;
    settle().await;

    assert_eq!(cluster.secondary_values(S1).await, ["m1", "m2"]);
    assert_eq!(cluster.secondary_values(S2).await, ["m1", "m2", "m3", "m4"]);

    tokio::time::sleep(RETRY_WAIT).await;

    assert_eq!(cluster.secondary_values(S1).await, ["m1", "m2", "m3", "m4"]);

    Ok(())
}

/// The iteration 3 self-check acceptance test from the task document.
///
/// 1. M + S1 run, S2 is down
/// 2. (m1, write concern 1) - ok
/// 3. (m2, write concern 2) - ok
/// 4. (m3, write concern 3) - waits for S2
/// 5. (m4, write concern 1) - ok, not blocked by step 4
/// 6. S2 starts
/// 7. S2 has [m1, m2, m3, m4]
#[tokio::test(start_paused = true)]
async fn iteration_3_acceptance_test() -> anyhow::Result<()> {
    let cluster = Cluster::with_config(retry_config(), SECONDARIES);
    let s2_down_time = Duration::from_secs(30);
    cluster.network.stop_secondary(S2);

    assert_eq!(cluster.timed_append("m1", 1).await?, Duration::ZERO);
    assert_eq!(cluster.timed_append("m2", 2).await?, Duration::ZERO);

    // Run m3 and m4 at the same time, like two clients.
    let (m3_elapsed, ()) = tokio::join!(cluster.timed_append("m3", 3), async {
        assert_eq!(cluster.timed_append("m4", 1).await.unwrap(), Duration::ZERO);
        settle().await;

        assert_eq!(cluster.master_values().await, ["m1", "m2", "m3", "m4"]);
        assert_eq!(cluster.secondary_values(S1).await, ["m1", "m2", "m3", "m4"]);
        assert_eq!(cluster.secondary_values(S2).await, NO_MESSAGES);

        tokio::time::sleep(s2_down_time).await;
        cluster.network.start_secondary(S2);
    });
    // m3 waited until S2 started.
    assert!(m3_elapsed? >= s2_down_time);

    // m1, m2 and m4 are retried on their own; give them time to reach S2.
    tokio::time::sleep(RETRY_WAIT).await;

    assert_eq!(cluster.secondary_values(S2).await, ["m1", "m2", "m3", "m4"]);

    Ok(())
}
