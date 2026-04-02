package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.ARCHIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.CATCH_UP_BASE_TOPIC_OFFSET_LAG;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DATA_RECOVERY_COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DROPPED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_DISK_FULL;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_OTHER;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.END_OF_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.NEW;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.NOT_CREATED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.NOT_STARTED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.PROGRESS;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.START_OF_BUFFER_REPLAY_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.START_OF_INCREMENTAL_PUSH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.TOPIC_SWITCH_RECEIVED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.UNKNOWN;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.WARNING;
import static com.linkedin.venice.utils.Utils.FATAL_DATA_VALIDATION_ERROR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.utils.DataProviderUtils;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.Test;


public class OfflinePushStatusTest {
  private String kafkaTopic = "testTopic";
  private int numberOfPartition = 3;
  private int replicationFactor = 2;
  private OfflinePushStrategy strategy = OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION;

  @Test
  public void testCreateOfflinePushStatus() {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    Assert.assertEquals(offlinePushStatus.getKafkaTopic(), kafkaTopic);
    Assert.assertEquals(offlinePushStatus.getNumberOfPartition(), numberOfPartition);
    Assert.assertEquals(offlinePushStatus.getReplicationFactor(), replicationFactor);
    Assert.assertEquals(
        offlinePushStatus.getCurrentStatus(),
        STARTED,
        "Once offline push status is created, it should in STARTED status by default.");
    Assert.assertEquals(
        offlinePushStatus.getStatusHistory().get(0).getStatus(),
        STARTED,
        "Once offline push status is created, it's in STARTED status and this status should be added into status history.");
    Assert.assertEquals(
        offlinePushStatus.getPartitionStatuses().size(),
        numberOfPartition,
        "Once offline push status is created, partition statuses should also be created too.");
  }

  @Test
  public void testUpdatePartitionStatus() {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    PartitionStatus partitionStatus = new PartitionStatus(1);
    partitionStatus.updateReplicaStatus("testInstance", PROGRESS);
    offlinePushStatus.setPartitionStatus(partitionStatus);
    Assert.assertEquals(
        offlinePushStatus.getPartitionStatus(1),
        ReadOnlyPartitionStatus.fromPartitionStatus(partitionStatus));

    try {
      offlinePushStatus.setPartitionStatus(new PartitionStatus(1000));
      fail("Partition 1000 dose not exist.");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testIsReadyToStartBufferReplay() {
    // Make sure buffer replay can be started in the case where current replica status is PROGRESS but
    // END_OF_PUSH_RECEIVED was already received
    OfflinePushStatus offlinePushStatus = new OfflinePushStatus(kafkaTopic, 1, replicationFactor, strategy);
    PartitionStatus partitionStatus = new PartitionStatus(0);
    List<ReplicaStatus> replicaStatuses = new ArrayList<>(replicationFactor);
    for (int i = 0; i < replicationFactor; i++) {
      replicaStatuses.add(new ReplicaStatus(Integer.toString(i)));
    }
    partitionStatus.setReplicaStatuses(replicaStatuses);
    for (int i = 0; i < replicationFactor; i++) {
      partitionStatus.updateReplicaStatus(Integer.toString(i), END_OF_PUSH_RECEIVED);
      partitionStatus.updateReplicaStatus(Integer.toString(i), PROGRESS);
    }
    offlinePushStatus.setPartitionStatuses(Collections.singletonList(partitionStatus));
    Assert.assertTrue(
        offlinePushStatus.isEOPReceivedInEveryPartition(false),
        "Buffer replay should be allowed to start since END_OF_PUSH_RECEIVED was already received");
  }

  /**
   * Regression test for VENG-12606: Controller leadership transfer causes push state mismatch.
   *
   * After a leadership transfer, the new controller loads push status from ZK which may still be STARTED.
   * Meanwhile, servers have already received TOPIC_SWITCH and their replica status in ZK shows
   * TOPIC_SWITCH_RECEIVED (having passed through END_OF_PUSH_RECEIVED).
   *
   * isEOPReceivedInEveryPartition() must still detect END_OF_PUSH_RECEIVED in the replica status history
   * even when the current replica status has advanced to TOPIC_SWITCH_RECEIVED.
   */
  @Test
  public void testIsReadyToStartBufferReplayWhenReplicasAtTopicSwitchReceived() {
    // Simulate post-failover state: push status is STARTED (stale from ZK)
    // but replicas have progressed through END_OF_PUSH_RECEIVED to TOPIC_SWITCH_RECEIVED
    int numPartitions = 3;
    OfflinePushStatus offlinePushStatus = new OfflinePushStatus(kafkaTopic, numPartitions, replicationFactor, strategy);
    List<PartitionStatus> partitionStatuses = new ArrayList<>(numPartitions);
    for (int p = 0; p < numPartitions; p++) {
      PartitionStatus partitionStatus = new PartitionStatus(p);
      List<ReplicaStatus> replicaStatuses = new ArrayList<>(replicationFactor);
      for (int r = 0; r < replicationFactor; r++) {
        replicaStatuses.add(new ReplicaStatus("instance_" + r));
      }
      partitionStatus.setReplicaStatuses(replicaStatuses);
      // Replicas progressed: STARTED -> END_OF_PUSH_RECEIVED -> TOPIC_SWITCH_RECEIVED
      for (int r = 0; r < replicationFactor; r++) {
        partitionStatus.updateReplicaStatus("instance_" + r, END_OF_PUSH_RECEIVED);
        partitionStatus.updateReplicaStatus("instance_" + r, TOPIC_SWITCH_RECEIVED);
      }
      partitionStatuses.add(partitionStatus);
    }
    offlinePushStatus.setPartitionStatuses(partitionStatuses);

    // Push status is STARTED (as loaded from ZK after failover)
    Assert.assertEquals(offlinePushStatus.getCurrentStatus(), STARTED);
    Assert.assertTrue(
        offlinePushStatus.isEOPReceivedInEveryPartition(false),
        "Buffer replay should be detected as ready even when replicas have advanced past "
            + "END_OF_PUSH_RECEIVED to TOPIC_SWITCH_RECEIVED, since END_OF_PUSH_RECEIVED is in history");
  }

  /**
   * Regression test for VENG-12606: Verify that isEOPReceivedInEveryPartition returns false when
   * partition statuses are empty placeholders (no replica data), simulating a failover where
   * ZK partition status ZNodes haven't been populated yet.
   */
  @Test
  public void testIsNotReadyToStartBufferReplayWhenPartitionStatusesEmpty() {
    int numPartitions = 3;
    OfflinePushStatus offlinePushStatus = new OfflinePushStatus(kafkaTopic, numPartitions, replicationFactor, strategy);

    // Simulate empty partition statuses (as returned by VeniceOfflinePushMonitorAccessor.getPartitionStatuses
    // when ZK nodes haven't been populated — lines 371-378)
    List<PartitionStatus> emptyPartitionStatuses = new ArrayList<>(numPartitions);
    for (int p = 0; p < numPartitions; p++) {
      emptyPartitionStatuses.add(new PartitionStatus(p)); // No replica statuses
    }
    offlinePushStatus.setPartitionStatuses(emptyPartitionStatuses);

    Assert.assertEquals(offlinePushStatus.getCurrentStatus(), STARTED);
    Assert.assertFalse(
        offlinePushStatus.isEOPReceivedInEveryPartition(false),
        "Buffer replay should NOT be ready when partition statuses have no replica data");
  }

  /**
   * Regression test for VENG-12606: Verify behavior when only some partitions have replicas
   * at TOPIC_SWITCH_RECEIVED while others are still empty (partial ZK update during failover).
   */
  @Test
  public void testIsNotReadyToStartBufferReplayWhenPartialPartitionData() {
    int numPartitions = 3;
    OfflinePushStatus offlinePushStatus = new OfflinePushStatus(kafkaTopic, numPartitions, replicationFactor, strategy);
    List<PartitionStatus> partitionStatuses = new ArrayList<>(numPartitions);

    // Partition 0: has replica data with TOPIC_SWITCH_RECEIVED (and EOP in history)
    PartitionStatus p0 = new PartitionStatus(0);
    List<ReplicaStatus> replicas0 = new ArrayList<>();
    for (int r = 0; r < replicationFactor; r++) {
      replicas0.add(new ReplicaStatus("instance_" + r));
    }
    p0.setReplicaStatuses(replicas0);
    for (int r = 0; r < replicationFactor; r++) {
      p0.updateReplicaStatus("instance_" + r, END_OF_PUSH_RECEIVED);
      p0.updateReplicaStatus("instance_" + r, TOPIC_SWITCH_RECEIVED);
    }
    partitionStatuses.add(p0);

    // Partitions 1 and 2: empty (no replica data — simulates incomplete ZK state)
    for (int p = 1; p < numPartitions; p++) {
      partitionStatuses.add(new PartitionStatus(p));
    }
    offlinePushStatus.setPartitionStatuses(partitionStatuses);

    Assert.assertEquals(offlinePushStatus.getCurrentStatus(), STARTED);
    Assert.assertFalse(
        offlinePushStatus.isEOPReceivedInEveryPartition(false),
        "Buffer replay should NOT be ready when some partitions have no replica data");
  }

  /**
   * Regression test for VENG-12606: Reproduces the actual production failure.
   *
   * Root cause: A single partition's server is stuck on xinfra position determination
   * (PubSubPosition.PENDING in KafkaConsumerHandler — no timeout, hangs forever).
   * The server never reaches END_OF_PUSH_RECEIVED for that partition.
   *
   * Meanwhile, all other partitions (99/100 in production, 9/10 here) have replicas
   * that progressed through END_OF_PUSH_RECEIVED to TOPIC_SWITCH_RECEIVED normally.
   *
   * isEOPReceivedInEveryPartition() requires ALL partitions to have EOP. One stuck
   * partition blocks buffer replay for the entire store, and the push hangs until killed.
   */
  @Test
  public void testSingleStuckPartitionBlocksEntirePush() {
    int numPartitions = 10;
    OfflinePushStatus offlinePushStatus = new OfflinePushStatus(kafkaTopic, numPartitions, replicationFactor, strategy);
    List<PartitionStatus> partitionStatuses = new ArrayList<>(numPartitions);

    for (int p = 0; p < numPartitions; p++) {
      PartitionStatus partitionStatus = new PartitionStatus(p);
      List<ReplicaStatus> replicaStatuses = new ArrayList<>(replicationFactor);
      for (int r = 0; r < replicationFactor; r++) {
        replicaStatuses.add(new ReplicaStatus("instance_" + r));
      }
      partitionStatus.setReplicaStatuses(replicaStatuses);

      if (p == 5) {
        // Partition 5: stuck on xinfra PENDING — server never progressed past STARTED.
        // All replicas remain in STARTED (the default). No EOP ever written to ZK.
        // This simulates the xinfra KafkaConsumerHandler.pausedShardsWaitingPosition
        // hang where PubSubPosition.PENDING is never resolved.
      } else {
        // All other partitions: normal progression through EOP → TOPIC_SWITCH
        for (int r = 0; r < replicationFactor; r++) {
          partitionStatus.updateReplicaStatus("instance_" + r, END_OF_PUSH_RECEIVED);
          partitionStatus.updateReplicaStatus("instance_" + r, TOPIC_SWITCH_RECEIVED);
        }
      }
      partitionStatuses.add(partitionStatus);
    }
    offlinePushStatus.setPartitionStatuses(partitionStatuses);

    // Push is in STARTED (never advanced because the controller never sent TOPIC_SWITCH)
    Assert.assertEquals(offlinePushStatus.getCurrentStatus(), STARTED);

    // The single stuck partition blocks the entire push
    Assert.assertFalse(
        offlinePushStatus.isEOPReceivedInEveryPartition(false),
        "One partition stuck on xinfra PENDING (no EOP) should block buffer replay for all partitions");

    // Now simulate the stuck partition finally getting unstuck (e.g., xinfra timeout added)
    PartitionStatus partition5 = partitionStatuses.get(5);
    for (int r = 0; r < replicationFactor; r++) {
      partition5.updateReplicaStatus("instance_" + r, END_OF_PUSH_RECEIVED);
    }
    offlinePushStatus.setPartitionStatuses(partitionStatuses);

    // Now buffer replay should be ready
    Assert.assertTrue(
        offlinePushStatus.isEOPReceivedInEveryPartition(false),
        "After stuck partition recovers, buffer replay should proceed");
  }

  /**
   * Regression test for VENG-12606: Verify that once push status has been updated to
   * END_OF_PUSH_RECEIVED, isEOPReceivedInEveryPartition correctly returns false
   * (preventing duplicate TOPIC_SWITCH sends).
   */
  @Test
  public void testIsNotReadyToStartBufferReplayWhenStatusAlreadyEndOfPushReceived() {
    OfflinePushStatus offlinePushStatus = new OfflinePushStatus(kafkaTopic, 1, replicationFactor, strategy);
    PartitionStatus partitionStatus = new PartitionStatus(0);
    List<ReplicaStatus> replicaStatuses = new ArrayList<>(replicationFactor);
    for (int i = 0; i < replicationFactor; i++) {
      replicaStatuses.add(new ReplicaStatus(Integer.toString(i)));
    }
    partitionStatus.setReplicaStatuses(replicaStatuses);
    for (int i = 0; i < replicationFactor; i++) {
      partitionStatus.updateReplicaStatus(Integer.toString(i), END_OF_PUSH_RECEIVED);
    }
    offlinePushStatus.setPartitionStatuses(Collections.singletonList(partitionStatus));

    // Advance the push status to END_OF_PUSH_RECEIVED (as if ZK was already updated)
    offlinePushStatus.updateStatus(END_OF_PUSH_RECEIVED);
    Assert.assertEquals(offlinePushStatus.getCurrentStatus(), END_OF_PUSH_RECEIVED);

    Assert.assertFalse(
        offlinePushStatus.isEOPReceivedInEveryPartition(false),
        "Should return false when push status is already END_OF_PUSH_RECEIVED to prevent duplicate TOPIC_SWITCH");
  }

  @Test
  public void testSetPartitionStatus() {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    PartitionStatus partitionStatus = new PartitionStatus(1);
    partitionStatus.updateReplicaStatus("testInstance", PROGRESS);
    offlinePushStatus.setPartitionStatus(partitionStatus);
    Assert.assertEquals(
        offlinePushStatus.getPartitionStatus(1),
        ReadOnlyPartitionStatus.fromPartitionStatus(partitionStatus));

    try {
      offlinePushStatus.setPartitionStatus(new PartitionStatus(1000));
      fail("Partition 1000 dose not exist.");
    } catch (IllegalArgumentException e) {
      // expected
    }
  }

  @Test
  public void testOfflinePushStatusIsComparable() {
    final int partitionNum = 20;
    List<PartitionStatus> partitionStatusList = new ArrayList<>(partitionNum);
    // The initial list is not ordered by partitionId
    for (int i = partitionNum - 1; i >= 0; i--) {
      partitionStatusList.add(new PartitionStatus(i));
    }
    Collections.sort(partitionStatusList);
    for (int i = 0; i < partitionNum; i++) {
      Assert.assertEquals(partitionStatusList.get(i).getPartitionId(), i);
    }
  }

  @Test
  public void testUpdateStatusValidTransitions() {
    // Define a map to store valid transitions for each source status
    Map<ExecutionStatus, Set<ExecutionStatus>> validTransitions = new HashMap<>();
    validTransitions.put(
        NOT_CREATED,
        EnumSet.of(
            STARTED,
            ERROR,
            DVC_INGESTION_ERROR_DISK_FULL,
            DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED,
            DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES,
            DVC_INGESTION_ERROR_OTHER));

    validTransitions.put(
        STARTED,
        EnumSet.of(
            STARTED,
            ERROR,
            DVC_INGESTION_ERROR_DISK_FULL,
            DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED,
            DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES,
            DVC_INGESTION_ERROR_OTHER,
            COMPLETED,
            END_OF_PUSH_RECEIVED));

    // ERROR Cases
    validTransitions.put(ERROR, EnumSet.of(ARCHIVED));
    validTransitions.put(DVC_INGESTION_ERROR_DISK_FULL, validTransitions.get(ERROR));
    validTransitions.put(DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED, validTransitions.get(ERROR));
    validTransitions.put(DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES, validTransitions.get(ERROR));
    validTransitions.put(DVC_INGESTION_ERROR_OTHER, validTransitions.get(ERROR));

    validTransitions.put(COMPLETED, EnumSet.of(ARCHIVED));

    validTransitions.put(
        END_OF_PUSH_RECEIVED,
        EnumSet.of(
            COMPLETED,
            ERROR,
            DVC_INGESTION_ERROR_DISK_FULL,
            DVC_INGESTION_ERROR_MEMORY_LIMIT_REACHED,
            DVC_INGESTION_ERROR_TOO_MANY_DEAD_INSTANCES,
            DVC_INGESTION_ERROR_OTHER));

    // add other status to the map manually such that test will fail if a new status is added but not updated in
    // validatePushStatusTransition and here
    validTransitions.put(NEW, new HashSet<>());
    validTransitions.put(PROGRESS, new HashSet<>());
    validTransitions.put(START_OF_BUFFER_REPLAY_RECEIVED, new HashSet<>());
    validTransitions.put(TOPIC_SWITCH_RECEIVED, new HashSet<>());
    validTransitions.put(START_OF_INCREMENTAL_PUSH_RECEIVED, new HashSet<>());
    validTransitions.put(END_OF_INCREMENTAL_PUSH_RECEIVED, new HashSet<>());
    validTransitions.put(DROPPED, new HashSet<>());
    validTransitions.put(WARNING, new HashSet<>());
    validTransitions.put(CATCH_UP_BASE_TOPIC_OFFSET_LAG, new HashSet<>());
    validTransitions.put(ARCHIVED, new HashSet<>());
    validTransitions.put(UNKNOWN, new HashSet<>());
    validTransitions.put(NOT_STARTED, new HashSet<>());
    validTransitions.put(DATA_RECOVERY_COMPLETED, new HashSet<>());

    for (ExecutionStatus status: ExecutionStatus.values()) {
      if (!validTransitions.containsKey(status)) {
        fail("New ExecutionStatus " + status.toString() + " should be added to the test");
      }
    }
    for (ExecutionStatus fromStatus: ExecutionStatus.values()) {
      Set<ExecutionStatus> validTransitionsFromAStatus = validTransitions.get(fromStatus);
      for (ExecutionStatus toStatus: ExecutionStatus.values()) {
        if (validTransitionsFromAStatus.contains(toStatus)) {
          testValidTargetStatus(fromStatus, toStatus);
        } else {
          if (fromStatus == toStatus) {
            // not throwing exception for this case as its redundant, so not testing
            continue;
          }
          testInvalidTargetStatus(fromStatus, toStatus);
        }
      }
    }
  }

  @Test
  public void testCloneOfflinePushStatus() {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    OfflinePushStatus clonedPush = offlinePushStatus.clonePushStatus();
    Assert.assertEquals(clonedPush, offlinePushStatus);

    PartitionStatus partitionStatus = new PartitionStatus(1);
    partitionStatus.updateReplicaStatus("i1", COMPLETED);
    offlinePushStatus.setPartitionStatus(partitionStatus);
    Assert.assertNotEquals(clonedPush, offlinePushStatus);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = "Partition 0 not found in partition assignment")
  public void testNPECaughtWhenPollingIncPushStatus() {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    PartitionAssignment partitionAssignment = new PartitionAssignment("test-topic", 10);
    offlinePushStatus.getIncrementalPushStatus(partitionAssignment, "ignore");
  }

  @Test
  public void testGetStatusUpdateTimestamp() {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    Long statusUpdateTimestamp = offlinePushStatus.getStatusUpdateTimestamp();
    assertNotNull(statusUpdateTimestamp);

    // N.B. There are no currently production code paths setting current status to null, so this is just to test the
    // defensive code...
    offlinePushStatus.setCurrentStatus(null);
    statusUpdateTimestamp = offlinePushStatus.getStatusUpdateTimestamp();
    assertNull(statusUpdateTimestamp);

    offlinePushStatus.setCurrentStatus(PROGRESS);
    List<StatusSnapshot> statusSnapshots = new ArrayList<>();
    statusSnapshots.add(new StatusSnapshot(STARTED, LocalDateTime.of(2012, 12, 21, 0, 0, 0).toString()));
    LocalDateTime startOfProgress = LocalDateTime.of(2012, 12, 21, 1, 0, 0);
    statusSnapshots.add(new StatusSnapshot(PROGRESS, startOfProgress.toString()));
    statusSnapshots.add(new StatusSnapshot(PROGRESS, LocalDateTime.of(2012, 12, 21, 2, 0, 0).toString()));
    offlinePushStatus.setStatusHistory(statusSnapshots);
    statusUpdateTimestamp = offlinePushStatus.getStatusUpdateTimestamp();
    assertNotNull(statusUpdateTimestamp);
    assertEquals(statusUpdateTimestamp, Long.valueOf(startOfProgress.toEpochSecond(ZoneOffset.UTC)));
  }

  @Test(dataProviderClass = DataProviderUtils.class, dataProvider = "True-and-False")
  public void testHasFatalDataValidationError(boolean hasFatalDataValidationError) {
    String kafkaTopic = "testTopic";
    int numberOfPartition = 3;
    int replicationFactor = 2;
    OfflinePushStrategy strategy = OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION;

    // Create an OfflinePushStatus object
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);

    // Create a PartitionStatus
    PartitionStatus partitionStatus = new PartitionStatus(1);

    // Create a ReplicaStatus with an error ExecutionStatus and an incrementalPushVersion that contains "Fatal data
    // validation problem"
    ReplicaStatus replicaStatus = new ReplicaStatus("instance1", true);
    if (hasFatalDataValidationError) {
      replicaStatus.updateStatus(ExecutionStatus.ERROR);
      replicaStatus.setIncrementalPushVersion(
          FATAL_DATA_VALIDATION_ERROR + " with partition 1, offset 1096534. Consumption will be halted.");
    } else {
      replicaStatus.updateStatus(ExecutionStatus.COMPLETED);
    }

    // Add the ReplicaStatus to the replicaStatusMap of the PartitionStatus
    partitionStatus.setReplicaStatuses(Collections.singleton(replicaStatus));

    // Set the PartitionStatus in the OfflinePushStatus
    offlinePushStatus.setPartitionStatus(partitionStatus);

    if (hasFatalDataValidationError) {
      // Assert that hasFatalDataValidationError returns true
      Assert.assertTrue(offlinePushStatus.hasFatalDataValidationError());
    } else {
      // Assert that hasFatalDataValidationError returns false
      Assert.assertFalse(offlinePushStatus.hasFatalDataValidationError());
    }
  }

  private void testValidTargetStatus(ExecutionStatus from, ExecutionStatus to) {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    offlinePushStatus.setCurrentStatus(from);
    offlinePushStatus.updateStatus(to);
    Assert.assertEquals(offlinePushStatus.getCurrentStatus(), to, to + " should be valid from:" + from);
  }

  private void testInvalidTargetStatus(ExecutionStatus from, ExecutionStatus to) {
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(kafkaTopic, numberOfPartition, replicationFactor, strategy);
    offlinePushStatus.setCurrentStatus(from);
    try {
      offlinePushStatus.updateStatus(to);
      fail(to + " is not invalid from:" + from);
    } catch (VeniceException e) {
      // expected.
    }
  }
}
