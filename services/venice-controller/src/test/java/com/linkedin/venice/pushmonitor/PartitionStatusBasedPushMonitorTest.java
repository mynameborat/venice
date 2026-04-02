package com.linkedin.venice.pushmonitor;

import static com.linkedin.venice.LogMessages.KILLED_JOB_MESSAGE;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.COMPLETED;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.ERROR;
import static com.linkedin.venice.pushmonitor.ExecutionStatus.STARTED;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.linkedin.venice.controller.HelixAdminClient;
import com.linkedin.venice.controller.stats.DisabledPartitionStats;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.helix.CachedReadOnlyStoreRepository;
import com.linkedin.venice.helix.HelixState;
import com.linkedin.venice.ingestion.control.RealTimeTopicSwitcher;
import com.linkedin.venice.meta.BufferReplayPolicy;
import com.linkedin.venice.meta.HybridStoreConfigImpl;
import com.linkedin.venice.meta.Instance;
import com.linkedin.venice.meta.OfflinePushStrategy;
import com.linkedin.venice.meta.Partition;
import com.linkedin.venice.meta.PartitionAssignment;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.StoreCleaner;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.meta.VersionImpl;
import com.linkedin.venice.meta.VersionStatus;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;


public class PartitionStatusBasedPushMonitorTest extends AbstractPushMonitorTest {
  HelixAdminClient helixAdminClient = mock(HelixAdminClient.class);

  @Override
  protected AbstractPushMonitor getPushMonitor(StoreCleaner storeCleaner) {
    return new PartitionStatusBasedPushMonitor(
        getClusterName(),
        getMockAccessor(),
        storeCleaner,
        getMockStoreRepo(),
        getMockRoutingDataRepo(),
        getMockPushHealthStats(),
        mock(RealTimeTopicSwitcher.class),
        getClusterLockManager(),
        getAggregateRealTimeSourceKafkaUrl(),
        Collections.emptyList(),
        helixAdminClient,
        getMockControllerConfig(),
        null,
        mock(DisabledPartitionStats.class),
        getMockVeniceWriterFactory(),
        getCurrentVersionChangeNotifier());
  }

  @Override
  protected AbstractPushMonitor getPushMonitor(RealTimeTopicSwitcher mockRealTimeTopicSwitcher) {
    return new PartitionStatusBasedPushMonitor(
        getClusterName(),
        getMockAccessor(),
        getMockStoreCleaner(),
        getMockStoreRepo(),
        getMockRoutingDataRepo(),
        getMockPushHealthStats(),
        mockRealTimeTopicSwitcher,
        getClusterLockManager(),
        getAggregateRealTimeSourceKafkaUrl(),
        Collections.emptyList(),
        mock(HelixAdminClient.class),
        getMockControllerConfig(),
        null,
        mock(DisabledPartitionStats.class),
        getMockVeniceWriterFactory(),
        getCurrentVersionChangeNotifier());
  }

  @Test
  public void testLoadRunningPushWhichIsNotUpdateToDate() {
    String topic = getTopic();
    Store store = prepareMockStore(topic);
    List<OfflinePushStatus> statusList = new ArrayList<>();
    OfflinePushStatus pushStatus = new OfflinePushStatus(
        topic,
        getNumberOfPartition(),
        getReplicationFactor(),
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    statusList.add(pushStatus);
    doReturn(statusList).when(getMockAccessor()).loadOfflinePushStatusesAndPartitionStatuses();
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, getNumberOfPartition());
    doReturn(true).when(getMockRoutingDataRepo()).containsKafkaTopic(eq(topic));
    doReturn(partitionAssignment).when(getMockRoutingDataRepo()).getPartitionAssignments(topic);
    for (int i = 0; i < getNumberOfPartition(); i++) {
      Partition partition = mock(Partition.class);
      Map<Instance, HelixState> instanceToStateMap = new HashMap<>();
      instanceToStateMap.put(new Instance("instance0", "host0", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance1", "host1", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance2", "host2", 1), HelixState.LEADER);
      when(partition.getInstanceToHelixStateMap()).thenReturn(instanceToStateMap);
      when(partition.getId()).thenReturn(i);
      partitionAssignment.addPartition(partition);
      PartitionStatus partitionStatus = mock(ReadOnlyPartitionStatus.class);
      when(partitionStatus.getPartitionId()).thenReturn(i);
      when(partitionStatus.getReplicaHistoricStatusList(anyString()))
          .thenReturn(Collections.singletonList(new StatusSnapshot(COMPLETED, "")));
      pushStatus.setPartitionStatus(partitionStatus);
    }
    when(getMockAccessor().getOfflinePushStatusAndItsPartitionStatuses(Mockito.anyString())).thenAnswer(invocation -> {
      String kafkaTopic = invocation.getArgument(0);
      for (OfflinePushStatus status: statusList) {
        if (status.getKafkaTopic().equals(kafkaTopic)) {
          return status;
        }
      }
      return null;
    });
    getMonitor().loadAllPushes();
    verify(getMockStoreRepo(), atLeastOnce()).updateStore(store);
    verify(getMockStoreCleaner(), atLeastOnce()).retireOldStoreVersions(anyString(), anyString(), eq(false), anyInt());
    Assert.assertEquals(getMonitor().getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.COMPLETED);
    // After offline push completed, bump up the current version of this store.
    Assert.assertEquals(store.getCurrentVersion(), 1);
    Mockito.reset(getMockAccessor());
  }

  @Test
  public void testVersionUpdateWithTargetRegionPush() {
    String topic = getTopic();
    Store store = prepareMockStore(topic, VersionStatus.STARTED, Collections.emptyMap(), null, "testRegion");
    List<OfflinePushStatus> statusList = new ArrayList<>();
    OfflinePushStatus pushStatus = new OfflinePushStatus(
        topic,
        getNumberOfPartition(),
        getReplicationFactor(),
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    statusList.add(pushStatus);
    doReturn(statusList).when(getMockAccessor()).loadOfflinePushStatusesAndPartitionStatuses();
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, getNumberOfPartition());
    doReturn(true).when(getMockRoutingDataRepo()).containsKafkaTopic(eq(topic));
    doReturn(partitionAssignment).when(getMockRoutingDataRepo()).getPartitionAssignments(topic);
    for (int i = 0; i < getNumberOfPartition(); i++) {
      Partition partition = mock(Partition.class);
      Map<Instance, HelixState> instanceToStateMap = new HashMap<>();
      instanceToStateMap.put(new Instance("instance0", "host0", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance1", "host1", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance2", "host2", 1), HelixState.LEADER);
      when(partition.getInstanceToHelixStateMap()).thenReturn(instanceToStateMap);
      when(partition.getId()).thenReturn(i);
      partitionAssignment.addPartition(partition);
      PartitionStatus partitionStatus = mock(ReadOnlyPartitionStatus.class);
      when(partitionStatus.getPartitionId()).thenReturn(i);
      when(partitionStatus.getReplicaHistoricStatusList(anyString()))
          .thenReturn(Collections.singletonList(new StatusSnapshot(COMPLETED, "")));
      pushStatus.setPartitionStatus(partitionStatus);
    }
    when(getMockAccessor().getOfflinePushStatusAndItsPartitionStatuses(Mockito.anyString())).thenAnswer(invocation -> {
      String kafkaTopic = invocation.getArgument(0);
      for (OfflinePushStatus status: statusList) {
        if (status.getKafkaTopic().equals(kafkaTopic)) {
          return status;
        }
      }
      return null;
    });
    getMonitor().loadAllPushes();
    verify(getMockStoreRepo(), atLeastOnce()).updateStore(store);
    verify(getMockStoreCleaner(), atLeastOnce()).retireOldStoreVersions(anyString(), anyString(), eq(false), anyInt());

    // Check that version was not swapped and that its status is PUSHED
    Assert.assertEquals(getMonitor().getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.COMPLETED);
    Assert.assertEquals(store.getCurrentVersion(), 0);
    Assert.assertEquals(store.getVersion(1).getStatus(), VersionStatus.PUSHED);
    verify(currentVersionChangeNotifier, never()).onCurrentVersionChange(any(), anyString(), anyInt(), anyInt());
    Mockito.reset(getMockAccessor());
  }

  @Test
  public void testVersionUpdateWithTargetRegionPushAndSwap() {
    String topic = getTopic();
    Store store = prepareMockStore(topic, VersionStatus.STARTED, Collections.emptyMap(), null, TARGET_REGION_NAME);

    List<OfflinePushStatus> statusList = new ArrayList<>();
    OfflinePushStatus pushStatus = new OfflinePushStatus(
        topic,
        getNumberOfPartition(),
        getReplicationFactor(),
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    statusList.add(pushStatus);

    doReturn(statusList).when(getMockAccessor()).loadOfflinePushStatusesAndPartitionStatuses();
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, getNumberOfPartition());
    doReturn(true).when(getMockRoutingDataRepo()).containsKafkaTopic(eq(topic));
    doReturn(partitionAssignment).when(getMockRoutingDataRepo()).getPartitionAssignments(topic);

    for (int i = 0; i < getNumberOfPartition(); i++) {
      Partition partition = mock(Partition.class);
      Map<Instance, HelixState> instanceToStateMap = new HashMap<>();
      instanceToStateMap.put(new Instance("instance0", "host0", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance1", "host1", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance2", "host2", 1), HelixState.LEADER);
      when(partition.getInstanceToHelixStateMap()).thenReturn(instanceToStateMap);
      when(partition.getId()).thenReturn(i);
      partitionAssignment.addPartition(partition);
      PartitionStatus partitionStatus = mock(ReadOnlyPartitionStatus.class);
      when(partitionStatus.getPartitionId()).thenReturn(i);
      when(partitionStatus.getReplicaHistoricStatusList(anyString()))
          .thenReturn(Collections.singletonList(new StatusSnapshot(COMPLETED, "")));
      pushStatus.setPartitionStatus(partitionStatus);
    }

    when(getMockAccessor().getOfflinePushStatusAndItsPartitionStatuses(Mockito.anyString())).thenAnswer(invocation -> {
      String kafkaTopic = invocation.getArgument(0);
      for (OfflinePushStatus status: statusList) {
        if (status.getKafkaTopic().equals(kafkaTopic)) {
          return status;
        }
      }
      return null;
    });

    getMonitor().loadAllPushes();
    verify(getMockStoreRepo(), atLeastOnce()).updateStore(store);
    verify(getMockStoreCleaner(), atLeastOnce()).retireOldStoreVersions(anyString(), anyString(), eq(false), anyInt());

    // The version should be swapped since region matches targetSwapRegion and swap is not deferred any further
    Assert.assertEquals(getMonitor().getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.COMPLETED);
    Assert.assertEquals(store.getCurrentVersion(), 1);
    Assert.assertEquals(store.getVersion(1).getStatus(), VersionStatus.ONLINE);
    verify(currentVersionChangeNotifier, atLeastOnce()).onCurrentVersionChange(any(), anyString(), eq(1), anyInt());
    Mockito.reset(getMockAccessor());
  }

  @Test
  public void testLoadRunningPushWhichIsNotUpdateToDateAndDeletionError() {
    String topic = getTopic();
    Store store = prepareMockStore(topic);
    List<OfflinePushStatus> statusList = new ArrayList<>();
    OfflinePushStatus pushStatus = new OfflinePushStatus(
        topic,
        getNumberOfPartition(),
        getReplicationFactor(),
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    statusList.add(pushStatus);
    doReturn(statusList).when(getMockAccessor()).loadOfflinePushStatusesAndPartitionStatuses();
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, getNumberOfPartition());
    doReturn(true).when(getMockRoutingDataRepo()).containsKafkaTopic(eq(topic));
    doReturn(partitionAssignment).when(getMockRoutingDataRepo()).getPartitionAssignments(topic);
    for (int i = 0; i < getNumberOfPartition(); i++) {
      Partition partition = mock(Partition.class);
      Map<Instance, HelixState> instanceToStateMap = new HashMap<>();
      instanceToStateMap.put(new Instance("instance0", "host0", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance1", "host1", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance2", "host2", 1), HelixState.LEADER);
      when(partition.getInstanceToHelixStateMap()).thenReturn(instanceToStateMap);
      when(partition.getId()).thenReturn(i);
      partitionAssignment.addPartition(partition);
      PartitionStatus partitionStatus = mock(ReadOnlyPartitionStatus.class);
      when(partitionStatus.getPartitionId()).thenReturn(i);
      when(partitionStatus.getReplicaHistoricStatusList(anyString()))
          .thenReturn(Collections.singletonList(new StatusSnapshot(ERROR, "")));
      pushStatus.setPartitionStatus(partitionStatus);
    }
    doThrow(new VeniceException("Could not delete.")).when(getMockStoreCleaner())
        .deleteOneStoreVersion(anyString(), anyString(), anyInt());
    when(getMockAccessor().getOfflinePushStatusAndItsPartitionStatuses(Mockito.anyString())).thenAnswer(invocation -> {
      String kafkaTopic = invocation.getArgument(0);
      for (OfflinePushStatus status: statusList) {
        if (status.getKafkaTopic().equals(kafkaTopic)) {
          return status;
        }
      }
      return null;
    });
    getMonitor().loadAllPushes();
    verify(getMockStoreRepo(), atLeastOnce()).updateStore(store);
    verify(getMockStoreCleaner(), atLeastOnce()).deleteOneStoreVersion(anyString(), anyString(), anyInt());
    Assert.assertEquals(getMonitor().getOfflinePushOrThrow(topic).getCurrentStatus(), ExecutionStatus.ERROR);
    Mockito.reset(getMockAccessor());
  }

  @Test(timeOut = 30 * Time.MS_PER_SECOND)
  public void testOnExternalViewChangeDisablePartition() {
    String disabledHostName = "disabled_host";
    Instance[] instances = { new Instance("a", "a", 1), new Instance(disabledHostName, "disabledHostName", 2),
        new Instance("b", disabledHostName, 3), new Instance("d", "d", 4), new Instance("e", "e", 5) };
    // Setup a store where two of its partitions has exactly one error replica.
    Store store = getStoreWithCurrentVersion();
    String resourceName = store.getVersion(store.getCurrentVersion()).kafkaTopicName();
    EnumMap<HelixState, List<Instance>> errorStateInstanceMap = new EnumMap<>(HelixState.class);
    EnumMap<HelixState, List<Instance>> healthyStateInstanceMap = new EnumMap<>(HelixState.class);
    errorStateInstanceMap.put(HelixState.ERROR, Collections.singletonList(instances[0]));
    // if a replica is error, then the left should be 1 leader and 1 standby.
    errorStateInstanceMap.put(HelixState.LEADER, Collections.singletonList(instances[1]));
    errorStateInstanceMap.put(HelixState.OFFLINE, Collections.singletonList(instances[2]));
    healthyStateInstanceMap.put(HelixState.LEADER, Collections.singletonList(instances[0]));
    healthyStateInstanceMap.put(HelixState.STANDBY, Arrays.asList(instances[1], instances[2]));

    Partition errorPartition0 = new Partition(0, errorStateInstanceMap, new EnumMap<>(ExecutionStatus.class));
    Partition errorPartition1 = new Partition(1, errorStateInstanceMap, new EnumMap<>(ExecutionStatus.class));
    Partition healthyPartition2 = new Partition(2, healthyStateInstanceMap, new EnumMap<>(ExecutionStatus.class));
    PartitionAssignment partitionAssignment1 = new PartitionAssignment(resourceName, 3);
    partitionAssignment1.addPartition(errorPartition0);
    partitionAssignment1.addPartition(errorPartition1);
    partitionAssignment1.addPartition(healthyPartition2);
    // Mock a post reset assignment where 2 of the partition remains in error state
    OfflinePushStatus offlinePushStatus =
        new OfflinePushStatus(resourceName, 3, 3, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);
    PartitionStatus partitionStatus = new PartitionStatus(0);
    List<ReplicaStatus> replicaStatuses = new ArrayList<>(3);
    replicaStatuses.add(new ReplicaStatus("a"));
    replicaStatuses.add(new ReplicaStatus("c"));
    replicaStatuses.add(new ReplicaStatus(disabledHostName));

    replicaStatuses.get(2).updateStatus(ERROR);
    partitionStatus.setReplicaStatuses(replicaStatuses);
    offlinePushStatus.setPartitionStatus(partitionStatus);
    partitionStatus = new PartitionStatus(1);
    List<ReplicaStatus> replicaStatuses1 = new ArrayList<>(3);
    replicaStatuses1.add(new ReplicaStatus("a"));
    replicaStatuses1.add(new ReplicaStatus("c"));
    replicaStatuses1.add(new ReplicaStatus(disabledHostName));
    replicaStatuses1.get(2).updateStatus(ERROR);
    partitionStatus.setReplicaStatuses(replicaStatuses1);
    offlinePushStatus.setPartitionStatus(partitionStatus);
    CachedReadOnlyStoreRepository readOnlyStoreRepository = mock(CachedReadOnlyStoreRepository.class);
    doReturn(Collections.singletonList(store)).when(readOnlyStoreRepository).getAllStores();
    doReturn(store).when(getMockStoreRepo()).getStore(store.getName());
    AbstractPushMonitor pushMonitor = getPushMonitor(new MockStoreCleaner(clusterLockManager));
    Map<String, List<String>> map = new HashMap<>();
    String kafkaTopic = Version.composeKafkaTopic(store.getName(), 1);
    map.put(kafkaTopic, Collections.singletonList(HelixUtils.getPartitionName(kafkaTopic, 0)));
    doReturn(map).when(helixAdminClient).getDisabledPartitionsMap(anyString(), anyString());
    doReturn(true).when(mockRoutingDataRepo).containsKafkaTopic(anyString());
    doReturn(partitionAssignment1).when(mockRoutingDataRepo).getPartitionAssignments(anyString());
    pushMonitor.startMonitorOfflinePush(resourceName, 3, 3, OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

    pushMonitor.updatePushStatus(offlinePushStatus, STARTED, Optional.empty());
    pushMonitor.onExternalViewChange(partitionAssignment1);

    ExecutionStatusWithDetails executionStatusWithDetails = offlinePushStatus.getStrategy()
        .getPushStatusDecider()
        .checkPushStatusAndDetailsByPartitionsStatus(offlinePushStatus, partitionAssignment1, null);
    Assert.assertEquals(executionStatusWithDetails.getStatus(), STARTED);

    verify(helixAdminClient, times(1)).getDisabledPartitionsMap(eq(getClusterName()), eq(disabledHostName));

    StatusSnapshot snapshot = new StatusSnapshot(ERROR, "1.2");
    snapshot.setIncrementalPushVersion(KILLED_JOB_MESSAGE + store.getName());
    replicaStatuses1.get(2).setStatusHistory(Arrays.asList(snapshot));
    PartitionStatus partitionStatus1 = new PartitionStatus(0);
    partitionStatus1.updateReplicaStatus(disabledHostName, ERROR, KILLED_JOB_MESSAGE + store.getName());
    offlinePushStatus.setPartitionStatus(partitionStatus1);

    offlinePushStatus.getStrategy()
        .getPushStatusDecider()
        .checkPushStatusAndDetailsByPartitionsStatus(
            offlinePushStatus,
            partitionAssignment1,
            new DisableReplicaCallback() {
              @Override
              public void disableReplica(String instance, int partitionId) {
              }

              @Override
              public boolean isReplicaDisabled(String instance, int partitionId) {
                return false;
              }
            });
    verify(helixAdminClient, times(0)).enablePartition(anyBoolean(), anyString(), anyString(), anyString(), anyList());
  }

  /**
   * Regression test for VENG-12606: Reproduces the exact production failure.
   *
   * Root cause chain (validated via logs and code):
   * 1. xinfra KafkaConsumerHandler pauses a partition with PubSubPosition.PENDING (line 860)
   * 2. The PENDING position is NEVER resolved (no timeout in pausedShardsWaitingPosition)
   * 3. The server never consumes data for that partition → never writes END_OF_PUSH_RECEIVED to ZK
   * 4. The controller's isEOPReceivedInEveryPartition() returns false (1 partition missing)
   * 5. Buffer replay (TOPIC_SWITCH) is never sent → push stuck forever → parent kills it
   *
   * This test uses 10 partitions: 9 completed normally, 1 stuck (partition 5).
   * Verifies:
   * - Push stays stuck in STARTED when 1 partition is missing EOP
   * - TOPIC_SWITCH is never sent
   * - Subsequent onExternalViewChange events don't help (partition is genuinely stuck)
   * - Push recovers when the stuck partition finally reports EOP
   */
  @Test(timeOut = 30 * Time.MS_PER_SECOND)
  public void testXinfraPendingPositionHangBlocksEntirePush() {
    int numPartitions = 10;
    int stuckPartition = 5;
    String storeName = Utils.getUniqueString("test_store");
    String topic = storeName + "_v1";

    // Set up hybrid store — must set hybrid config on STORE (not just version)
    // because checkWhetherToStartEOPProcedures checks store.isHybrid()
    Store store = prepareMockStore(topic, VersionStatus.STARTED);
    store.setHybridStoreConfig(
        new HybridStoreConfigImpl(
            100,
            100,
            HybridStoreConfigImpl.DEFAULT_HYBRID_TIME_LAG_THRESHOLD,
            BufferReplayPolicy.REWIND_FROM_EOP));

    RealTimeTopicSwitcher realTimeTopicSwitcher = mock(RealTimeTopicSwitcher.class);
    AbstractPushMonitor pushMonitor = getPushMonitor(realTimeTopicSwitcher);

    // Create push status: STARTED in ZK, with per-partition replica data
    OfflinePushStatus pushStatus = new OfflinePushStatus(
        topic,
        numPartitions,
        getReplicationFactor(),
        OfflinePushStrategy.WAIT_N_MINUS_ONE_REPLCIA_PER_PARTITION);

    for (int p = 0; p < numPartitions; p++) {
      PartitionStatus partitionStatus = new PartitionStatus(p);
      List<ReplicaStatus> replicaStatuses = new ArrayList<>();
      for (int r = 0; r < getReplicationFactor(); r++) {
        replicaStatuses.add(new ReplicaStatus("instance" + r));
      }
      partitionStatus.setReplicaStatuses(replicaStatuses);

      if (p == stuckPartition) {
        // Partition 5: xinfra PENDING hang — server never progressed past STARTED.
        // Replicas remain in default STARTED status. No EOP written to ZK.
      } else {
        // Other partitions: normal. Servers wrote END_OF_PUSH_RECEIVED to ZK.
        for (int r = 0; r < getReplicationFactor(); r++) {
          partitionStatus.updateReplicaStatus("instance" + r, ExecutionStatus.END_OF_PUSH_RECEIVED);
          partitionStatus.updateReplicaStatus("instance" + r, ExecutionStatus.TOPIC_SWITCH_RECEIVED);
        }
      }
      pushStatus.setPartitionStatus(partitionStatus);
    }

    doReturn(Collections.singletonList(pushStatus)).when(getMockAccessor())
        .loadOfflinePushStatusesAndPartitionStatuses();
    when(getMockAccessor().getOfflinePushStatusAndItsPartitionStatuses(Mockito.anyString())).thenReturn(pushStatus);

    // Set up routing data for all 10 partitions
    PartitionAssignment partitionAssignment = new PartitionAssignment(topic, numPartitions);
    doReturn(true).when(getMockRoutingDataRepo()).containsKafkaTopic(eq(topic));
    doReturn(partitionAssignment).when(getMockRoutingDataRepo()).getPartitionAssignments(topic);
    for (int i = 0; i < numPartitions; i++) {
      Partition partition = mock(Partition.class);
      Map<Instance, HelixState> instanceToStateMap = new HashMap<>();
      instanceToStateMap.put(new Instance("instance0", "host0", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance1", "host1", 1), HelixState.STANDBY);
      instanceToStateMap.put(new Instance("instance2", "host2", 1), HelixState.LEADER);
      when(partition.getInstanceToHelixStateMap()).thenReturn(instanceToStateMap);
      when(partition.getId()).thenReturn(i);
      when(partition.getAllInstancesSet()).thenReturn(instanceToStateMap.keySet());
      partitionAssignment.addPartition(partition);
    }

    // === Phase 1: Initial failover — push is stuck ===
    pushMonitor.loadAllPushes();

    // TOPIC_SWITCH should NOT be sent — partition 5 has no EOP
    verify(realTimeTopicSwitcher, never()).switchToRealTimeTopic(any(), any(), any(), any(), anyList());

    // Push stays in STARTED — this is the VENG-12606 stuck state
    Assert.assertEquals(
        pushMonitor.getOfflinePushOrThrow(topic).getCurrentStatus(),
        ExecutionStatus.STARTED,
        "Push should be stuck in STARTED: 9/10 partitions have EOP but partition " + stuckPartition
            + " is stuck on xinfra PENDING");

    // === Phase 2: External view change — still stuck ===
    // Simulate a Helix external view change notification. The controller re-checks
    // push status, but partition 5 is still stuck. Push should remain in STARTED.
    pushMonitor.onExternalViewChange(partitionAssignment);

    verify(realTimeTopicSwitcher, never()).switchToRealTimeTopic(any(), any(), any(), any(), anyList());
    Assert.assertEquals(
        pushMonitor.getOfflinePushOrThrow(topic).getCurrentStatus(),
        ExecutionStatus.STARTED,
        "Push should remain stuck after external view change — partition " + stuckPartition + " still has no EOP");

    // === Phase 3: Stuck partition recovers — push should advance ===
    // Simulate the xinfra position finally resolving: server processes data,
    // writes END_OF_PUSH_RECEIVED to ZK, triggers onPartitionStatusChange callback.
    PartitionStatus recoveredPartition = new PartitionStatus(stuckPartition);
    List<ReplicaStatus> recoveredReplicas = new ArrayList<>();
    for (int r = 0; r < getReplicationFactor(); r++) {
      recoveredReplicas.add(new ReplicaStatus("instance" + r));
    }
    recoveredPartition.setReplicaStatuses(recoveredReplicas);
    for (int r = 0; r < getReplicationFactor(); r++) {
      recoveredPartition.updateReplicaStatus("instance" + r, ExecutionStatus.END_OF_PUSH_RECEIVED);
    }

    pushMonitor.onPartitionStatusChange(topic, ReadOnlyPartitionStatus.fromPartitionStatus(recoveredPartition));

    // NOW TOPIC_SWITCH should be sent — all partitions have EOP
    verify(realTimeTopicSwitcher, times(1)).switchToRealTimeTopic(
        eq(Utils.getRealTimeTopicName(store)),
        eq(topic),
        eq(store),
        eq(getAggregateRealTimeSourceKafkaUrl()),
        anyList());

    // Push should advance to END_OF_PUSH_RECEIVED
    Assert.assertEquals(
        pushMonitor.getOfflinePushOrThrow(topic).getCurrentStatus(),
        ExecutionStatus.END_OF_PUSH_RECEIVED,
        "Push should advance after stuck partition recovers");

    Mockito.reset(getMockAccessor());
  }

  private Store getStoreWithCurrentVersion() {
    Store store = TestUtils.getRandomStore();
    store.addVersion(new VersionImpl(store.getName(), 1, "", 3));
    store.setCurrentVersion(1);
    return store;
  }
}
