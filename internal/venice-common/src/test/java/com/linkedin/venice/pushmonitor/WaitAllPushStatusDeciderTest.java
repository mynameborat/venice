package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;

import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.PartitionAssignment;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class WaitAllPushStatusDeciderTest extends TestPushStatusDecider {
  private final WaitAllPushStatusDecider statusDecider = new WaitAllPushStatusDecider();

  @BeforeMethod
  public void setUp() {
    topic = "WaitAllPushStatusDeciderTest";
    partitionAssignment = new PartitionAssignment(topic, numberOfPartition);
    createPartitions(numberOfPartition, replicationFactor);
  }

  @Test
  public void testGetPartitionStatus() {
    PartitionStatus partitionStatus = new PartitionStatus(0);

    Map<Instance, HelixState> instanceToStateMap = new HashMap<>();
    instanceToStateMap.put(new Instance("instance0", "host0", 1), HelixState.STANDBY);
    instanceToStateMap.put(new Instance("instance1", "host1", 1), HelixState.STANDBY);
    instanceToStateMap.put(new Instance("instance2", "host2", 1), HelixState.LEADER);

    // Not enough replicas
    partitionStatus.updateReplicaStatus("instance0", COMPLETED);
    partitionStatus.updateReplicaStatus("instance1", COMPLETED);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        STARTED);

    // have enough replicas, but one of them hasn't finished yet
    partitionStatus.updateReplicaStatus("instance2", STARTED);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        STARTED);

    // all the replicas have finished
    partitionStatus.updateReplicaStatus("instance2", COMPLETED);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        COMPLETED);

    // one of the replicas failed
    partitionStatus.updateReplicaStatus("instance1", ERROR);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        ERROR);

    // a new replica joined but yet registered in external view
    partitionStatus.updateReplicaStatus("instance3", STARTED);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        ERROR);

    instanceToStateMap.put(new Instance("instance3", "host3", 1), HelixState.STANDBY);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        STARTED);

    // new replica has finished
    partitionStatus.updateReplicaStatus("instance3", COMPLETED);
    Assert.assertEquals(
        statusDecider.getPartitionStatus(partitionStatus, replicationFactor, instanceToStateMap, null),
        COMPLETED);
  }

  @Test
  public void testCheckPushStatusReturnsBlockingPartitionDetails() {
    OfflinePushStatus pushStatus = new OfflinePushStatus(
        "testTopic_v1",
        numberOfPartition,
        replicationFactor,
        OfflinePushStrategy.WAIT_ALL_REPLICAS);

    PartitionAssignment localPartitionAssignment = new PartitionAssignment("testTopic_v1", numberOfPartition);
    createPartitions(numberOfPartition, replicationFactor);
    // Copy partitions into our local assignment using the field set up by setUp()
    for (int i = 0; i < numberOfPartition; i++) {
      localPartitionAssignment.addPartition(partitionAssignment.getPartition(i));
    }

    // Partition 0: all replicas COMPLETED
    PartitionStatus p0 = new PartitionStatus(0);
    for (int r = 0; r < replicationFactor; r++) {
      p0.updateReplicaStatus("instance" + r, ExecutionStatus.COMPLETED);
    }
    pushStatus.setPartitionStatus(p0);

    // Partition 1: all replicas still in STARTED (stuck)
    PartitionStatus p1 = new PartitionStatus(1);
    List<ReplicaStatus> replicas = new ArrayList<>();
    for (int r = 0; r < replicationFactor; r++) {
      replicas.add(new ReplicaStatus("instance" + r));
    }
    p1.setReplicaStatuses(replicas);
    pushStatus.setPartitionStatus(p1);

    ExecutionStatusWithDetails result = new WaitAllPushStatusDecider()
        .checkPushStatusAndDetailsByPartitionsStatus(pushStatus, localPartitionAssignment, null);

    Assert.assertEquals(result.getStatus(), ExecutionStatus.STARTED);
    Assert.assertNotNull(result.getDetails(), "Details should identify blocking partitions");
    Assert.assertTrue(
        result.getDetails().contains("1"),
        "Details should include partition 1 as blocking: " + result.getDetails());
  }
}
