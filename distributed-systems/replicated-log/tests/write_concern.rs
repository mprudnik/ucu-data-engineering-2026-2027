//! Write concern: the append returns after `write_concern` nodes have the message
//! (1 = master only, 2 = master + 1 secondary, 3 = master + 2 secondaries).
//! Replication to the other secondaries goes on in the background.
//!
//! All tests run with paused Tokio time: delays finish at once,
//! and `elapsed()` gives the exact time an append was blocked.

use std::time::Duration;

use harness::{Cluster, S1, S2, SECONDARIES, settle};
use replicated_log::core;

mod harness;

const NO_MESSAGES: [&str; 0] = [];

/// The append waits only for as many secondaries as the write concern needs.
///
/// S1 answers after 5s, S2 after 6s.
#[tokio::test(start_paused = true)]
async fn append_waits_only_for_write_concern() -> anyhow::Result<()> {
    let cluster = Cluster::new(SECONDARIES);
    cluster.network.set_delay(S1, Duration::from_secs(5));
    cluster.network.set_delay(S2, Duration::from_secs(6));

    // master only
    assert_eq!(cluster.timed_append("m1", 1).await?, Duration::ZERO);
    // master + the faster secondary (S1)
    assert_eq!(cluster.timed_append("m2", 2).await?, Duration::from_secs(5));
    // master + both secondaries
    assert_eq!(cluster.timed_append("m3", 3).await?, Duration::from_secs(6));

    Ok(())
}

/// Write concern below 1 or above the number of nodes is rejected.
#[tokio::test(start_paused = true)]
async fn invalid_write_concern_is_rejected() -> anyhow::Result<()> {
    let cluster = Cluster::new(SECONDARIES);

    for write_concern in [0, SECONDARIES + 2] {
        let result = cluster.append("m1", write_concern).await;
        assert!(
            matches!(result, Err(core::Error::InvalidWriteConcern { .. })),
            "write concern {write_concern} should be rejected, got {result:?}",
        );
    }
    assert_eq!(cluster.master_values().await, NO_MESSAGES);

    Ok(())
}

/// A delayed secondary catches up after the delay.
///
/// With a delay on S2, the master and S2 return different lists for a while
/// (inconsistency). After the delay, S2 catches up (eventual consistency).
#[tokio::test(start_paused = true)]
async fn secondary_catches_up_after_delay() -> anyhow::Result<()> {
    let cluster = Cluster::new(SECONDARIES);
    cluster.network.set_delay(S2, Duration::from_secs(5));

    assert_eq!(cluster.timed_append("m1", 1).await?, Duration::ZERO);
    settle().await;

    assert_eq!(cluster.master_values().await, ["m1"]);
    assert_eq!(cluster.secondary_values(S1).await, ["m1"]);
    assert_eq!(cluster.secondary_values(S2).await, NO_MESSAGES);

    tokio::time::sleep(Duration::from_secs(5)).await;
    settle().await;

    assert_eq!(cluster.secondary_values(S2).await, ["m1"]);

    Ok(())
}

/// The iteration 2 acceptance test from the task document.
///
/// 1. M + S1 + S2, S2 has a 10s delay
/// 2. (m1, write concern 1) - ok at once
/// 3. (m2, write concern 2) - ok at once (S1 has no delay)
/// 4. (m3, write concern 3) - waits for S2
/// 5. (m4, write concern 1) - ok at once, not blocked by step 4
/// 6. S2 is behind
/// 7. M and S1 have [m1, m2, m3, m4]
#[tokio::test(start_paused = true)]
async fn iteration_2_acceptance_test() -> anyhow::Result<()> {
    let s2_delay = Duration::from_secs(10);
    let cluster = Cluster::new(SECONDARIES);
    cluster.network.set_delay(S2, s2_delay);

    assert_eq!(cluster.timed_append("m1", 1).await?, Duration::ZERO);
    assert_eq!(cluster.timed_append("m2", 2).await?, Duration::ZERO);

    // Run m3 and m4 at the same time, like two clients.
    let (m3_elapsed, ()) = tokio::join!(cluster.timed_append("m3", 3), async {
        assert_eq!(cluster.timed_append("m4", 1).await.unwrap(), Duration::ZERO);
        settle().await;

        // m3 is still waiting for S2 here.
        assert_eq!(cluster.secondary_values(S2).await, NO_MESSAGES);
        assert_eq!(cluster.master_values().await, ["m1", "m2", "m3", "m4"]);
        assert_eq!(cluster.secondary_values(S1).await, ["m1", "m2", "m3", "m4"]);
    });
    assert_eq!(m3_elapsed?, s2_delay);

    settle().await;
    assert_eq!(cluster.secondary_values(S2).await, ["m1", "m2", "m3", "m4"]);

    Ok(())
}
