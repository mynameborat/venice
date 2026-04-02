# VENG-12606: Controller Leadership Transfer Causes Push State Mismatch

**JIRA:** https://linkedin.atlassian.net/browse/VENG-12606 **Status:** Open | **Priority:** Major | **Reporter:**
Koorous Vargha

## Summary

When a child controller undergoes a leadership transfer during an active hybrid push, the new leader loads stale push
state (`STARTED`) from ZooKeeper. Servers have already received `TOPIC_SWITCH` and are consuming from the real-time
topic, but the new child controller cannot advance the push — it loops checking buffer-replay readiness indefinitely
until the parent controller kills the push.

## Root Cause Analysis (Code-Validated)

### The Race Window

The critical sequence in `AbstractPushMonitor.checkWhetherToStartEOPProcedures()` (lines 962-1013) is:

```
Line 962: if (isEOPReceivedInAllPartitions)
Line 965:   realTimeTopicSwitcher.switchToRealTimeTopic(...)  // TOPIC_SWITCH sent
Line 1010:  updatePushStatus(..., END_OF_PUSH_RECEIVED)       // ZK updated AFTER
```

**TOPIC_SWITCH is sent to Kafka BEFORE the ZK push status is updated from `STARTED` to `END_OF_PUSH_RECEIVED`.** If the
leadership transfer happens in this window (or if a different region's controller sent TOPIC_SWITCH), the new leader
reads `STARTED` from ZK.

### Code Path: Leadership Transfer → Stale State Load

1. `VeniceControllerStateModel.onBecomeLeaderFromStandby()` (line 174)
2. → `initClusterResources()` (line 265) → `clusterResources.refresh()` (line 277)
3. → `HelixVeniceClusterResources.refresh()` → `pushMonitor.loadAllPushes()` (line 338)
4. → `AbstractPushMonitor.loadAllPushes()` (line 174) →
   `offlinePushAccessor.loadOfflinePushStatusesAndPartitionStatuses()` (line 176)
5. → `updateOfflinePush()` (line 344) reads push status from ZK
   - Logs: `"Update offline push status from ZK for topic: {}, current status: {}"`
   - **Matches ticket log at 16:35:27Z**

### Why the New Leader Gets Stuck

After loading, `loadAllPushes()` (lines 197-209) attempts reconciliation:

```java
if (!offlinePushStatus.getCurrentStatus().isTerminal()) {
    ExecutionStatusWithDetails statusWithDetails =
        checkPushStatus(offlinePushStatus, routingDataRepository.getPartitionAssignments(topic), null);
    if (statusWithDetails.getStatus().isTerminal()) {
        handleTerminalOfflinePushUpdate(offlinePushStatus, statusWithDetails);
    } else {
        checkWhetherToStartEOPProcedures(offlinePushStatus);  // Falls here
    }
}
```

`checkPushStatus()` via `PushStatusDecider.checkPushStatusAndDetailsByPartitionsStatus()` can detect
`END_OF_PUSH_RECEIVED` from partition statuses (it handles `TOPIC_SWITCH_RECEIVED` correctly since it's not a
"determined status" and falls through to `END_OF_PUSH_RECEIVED` in history). This returns `END_OF_PUSH_RECEIVED` which
is non-terminal, so it calls `checkWhetherToStartEOPProcedures()`.

Inside `checkWhetherToStartEOPProcedures()`, `isEOPReceivedInEveryPartition()` at `OfflinePushStatus.java:435-468`:

```java
if (!getCurrentStatus().equals(STARTED)) {
    return false;  // Requires overall status == STARTED ✓ (it is)
}
// For each partition, check if at least one replica has END_OF_PUSH_RECEIVED
for (PartitionStatus partitionStatus: getPartitionStatuses()) {
    for (ReplicaStatus replicaStatus: partitionStatus.getReplicaStatuses()) {
        if (replicaStatus.getCurrentStatus() == requiredStatus) { ... }
        else { /* check history */ }
    }
}
```

**Problem**: The `OfflinePushStatus` object checked here is the one loaded from ZK (`topicToPushMap`), which was
refreshed at line 195 (`updateOfflinePush`). If the partition status data in ZK has replicas that have moved past
`END_OF_PUSH_RECEIVED` to `TOPIC_SWITCH_RECEIVED`, the method CAN find `END_OF_PUSH_RECEIVED` in history and return true
— which would re-send TOPIC_SWITCH (idempotent) and update ZK.

**However**, if partition statuses in ZK are incomplete (empty placeholder `PartitionStatus` objects with no replica
data — see `VeniceOfflinePushMonitorAccessor.getPartitionStatuses()` lines 371-378), or if the replica status history
has been truncated, the check fails and we enter the "not ready to start buffer replay" loop.

### The Polling Loop

`AbstractPushMonitor.java:972-978`:

```java
} else if (!offlinePushStatus.getCurrentStatus().isTerminal()) {
    LOGGER.info("{} is not ready to start buffer replay. Current state: {}",
        offlinePushStatus.getKafkaTopic(),
        offlinePushStatus.getCurrentStatus().toString());
}
```

### "server status: null"

`PushStatusCollector.java:206-208`:

```java
ExecutionStatusWithDetails serverStatus = pushStatus.getServerStatus();
if (serverStatus == null) {
    continue;  // Skips this topic entirely
}
```

After failover, `PushStatusCollector` is freshly initialized with `serverStatus = null` (in-memory only, not persisted).
It's only set via `handleServerPushStatusUpdate()` which requires the push to reach terminal state — creating a circular
dependency.

## Timeline (from ticket)

| Time              | Host           | Event                                                          |
| ----------------- | -------------- | -------------------------------------------------------------- |
| 16:23:02          | lor1-app91078  | Status → STARTED                                               |
| 16:28:01          | lva1-app134530 | "is ready to start buffer replay" → sends TOPIC_SWITCH         |
| 16:28:13          | servers        | 100 partitions received END_OF_PUSH, switching to RT           |
| 16:32:36          | lor1-app101725 | Controller deployment triggers STANDBY→OFFLINE                 |
| 16:35:23          | lor1-app91078  | LEADER→STANDBY                                                 |
| 16:35:23          | lor1-app132855 | STANDBY→LEADER                                                 |
| 16:35:27          | lor1-app132855 | "Update offline push status from ZK...current status: STARTED" |
| 16:35:27          | lor1-app132855 | "not ready to start buffer replay. Current state: STARTED"     |
| 16:35:58–17:02:58 | lor1-app132855 | Polls every 30s: server status=null (28 min)                   |
| 17:03:14          | lor1-app132855 | KILL_OFFLINE_PUSH_JOB received → all replicas ERROR            |

## Key Files

| Component             | File                                    | Lines                     |
| --------------------- | --------------------------------------- | ------------------------- |
| Leadership transition | `VeniceControllerStateModel.java`       | 174, 265, 277             |
| Cluster refresh       | `HelixVeniceClusterResources.java`      | 338                       |
| Push monitor load     | `AbstractPushMonitor.java`              | 174-248                   |
| ZK push status read   | `AbstractPushMonitor.java`              | 344-357                   |
| EOP procedures        | `AbstractPushMonitor.java`              | 944-1021                  |
| TOPIC_SWITCH send     | `RealTimeTopicSwitcher.java`            | 287-324                   |
| EOP partition check   | `OfflinePushStatus.java`                | 435-468                   |
| Push status decider   | `PushStatusDecider.java`                | 40-93, 181-239, 254-264   |
| Status collector null | `PushStatusCollector.java`              | 206-208                   |
| ZK accessor           | `VeniceOfflinePushMonitorAccessor.java` | 113-141, 253-283, 365-397 |

## Test Findings

Unit tests (see `PartitionStatusBasedPushMonitorTest`) demonstrate:

1. **When ZK partition statuses have END_OF_PUSH_RECEIVED in replica history** (even when current status is
   TOPIC_SWITCH_RECEIVED): the existing reconciliation in `loadAllPushes()` WORKS. `isEOPReceivedInEveryPartition()`
   correctly finds END_OF_PUSH_RECEIVED in the status history, re-sends TOPIC_SWITCH (idempotent), and updates push
   status to END_OF_PUSH_RECEIVED.

2. **Object aliasing between initial load and updateOfflinePush is safe**: even when `updateOfflinePush()` returns a
   different object with empty partition statuses, `checkWhetherToStartEOPProcedures` operates on the original loop
   variable (which has the correct data from the initial batch load).

3. **When ZK partition statuses are EMPTY** (no replica data at all): the push gets stuck in STARTED — this is the
   failure case. No TOPIC_SWITCH is sent, push never advances. Recovery only happens if a future
   `onPartitionStatusChange` ZK callback fires with the missing data.

**Conclusion**: The bug manifests when the new leader's ZK reads return partition status ZNodes with **no replica
data**. This can happen if:

- Servers haven't finished writing their status to ZK at failover time
- ZK partition status ZNodes exist but are empty (just created, not yet populated)
- The servers have already written and won't write again, so no ZK callback fires

## Proposed Fix

When a new child controller leader starts monitoring an in-progress push, it should reconcile with actual server state
rather than relying solely on ZK push status. If `checkPushStatus()` returns `END_OF_PUSH_RECEIVED` during
`loadAllPushes()`, the new leader should update the push status in ZK to `END_OF_PUSH_RECEIVED` and re-trigger EOP
procedures.

Additionally, if partition statuses are empty after the initial load, the controller should actively query server state
(via Customized View or direct server query) to fill in the missing data, rather than passively waiting for ZK callbacks
that may never arrive.
