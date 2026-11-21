//! Total order: every node lists messages in the order the master accepted them,
//! even when secondaries receive them in a different order.
//! A secondary shows a message only when all earlier messages are there too.

use std::time::Duration;

use harness::{Cluster, S1, S2, SECONDARIES, settle};

mod harness;

const NO_MESSAGES: [&str; 0] = [];

/// The reordering test from the task document.
///
/// S1 gets m2 before m1, but lists them as [m1, m2].
#[tokio::test(start_paused = true)]
async fn secondary_lists_reordered_messages_in_order() -> anyhow::Result<()> {
    let cluster = Cluster::new(SECONDARIES);
    cluster.network.reorder(S1, 2);

    // Write concern 1: the appends do not wait for S1, which holds m1 until m2 arrives.
    cluster.append("m1", 1).await?;
    cluster.append("m2", 1).await?;
    settle().await;

    assert_eq!(cluster.secondary_values(S1).await, ["m1", "m2"]);
    assert_eq!(cluster.secondary_values(S2).await, ["m1", "m2"]);

    Ok(())
}

/// A secondary hides messages after a gap.
///
/// S1 has m2 but not m1 yet: it must not show m2 until m1 arrives.
#[tokio::test(start_paused = true)]
async fn secondary_hides_messages_after_a_gap() -> anyhow::Result<()> {
    let cluster = Cluster::new(SECONDARIES);

    let delay = Duration::from_secs(5);

    // m1 reaches S1 after 5s.
    cluster.network.set_delay(S1, delay);
    cluster.append("m1", 1).await?;
    // Let m1's background send start, so it reads the delay before we remove it.
    settle().await;

    // m2 reaches S1 at once.
    cluster.network.remove_delay(S1);
    cluster.append("m2", 1).await?;
    settle().await;

    assert_eq!(cluster.secondary_values(S1).await, NO_MESSAGES);

    tokio::time::sleep(delay).await;
    settle().await;

    assert_eq!(cluster.secondary_values(S1).await, ["m1", "m2"]);

    Ok(())
}

/// Appends that run at the same time get one order, and all nodes agree on it.
#[tokio::test(start_paused = true)]
async fn all_nodes_have_the_same_order() -> anyhow::Result<()> {
    let cluster = Cluster::new(SECONDARIES);
    cluster.network.set_delay(S1, Duration::from_secs(2));
    cluster.network.set_delay(S2, Duration::from_secs(1));

    let (a, b, c) = tokio::join!(
        cluster.append("a", 3),
        cluster.append("b", 3),
        cluster.append("c", 3),
    );
    a?;
    b?;
    c?;

    let master = cluster.master_values().await;
    assert_eq!(master.len(), 3);
    assert_eq!(cluster.secondary_values(S1).await, master);
    assert_eq!(cluster.secondary_values(S2).await, master);

    Ok(())
}
