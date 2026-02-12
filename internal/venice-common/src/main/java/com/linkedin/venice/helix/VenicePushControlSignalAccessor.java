package com.linkedin.venice.helix;

import static com.linkedin.venice.zk.VeniceZkPaths.PUSH_CONTROL_SIGNALS;

import com.linkedin.venice.pushmonitor.PushControlSignal;
import com.linkedin.venice.pushmonitor.PushControlSignalAccessor;
import com.linkedin.venice.utils.HelixUtils;
import com.linkedin.venice.utils.PathResourceRegistry;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * ZooKeeper-based implementation of {@link PushControlSignalAccessor}.
 *
 * <p>Follows the same pattern as {@link VeniceOfflinePushMonitorAccessor} — a stateless,
 * thread-safe accessor that reads/writes {@link PushControlSignal} ZNodes.
 *
 * <p>ZK path layout: {@code /{clusterName}/PushControlSignals/{topic}}
 */
public class VenicePushControlSignalAccessor implements PushControlSignalAccessor {
  private static final Logger LOGGER = LogManager.getLogger(VenicePushControlSignalAccessor.class);

  private final String clusterName;
  private final String pushControlSignalParentPath;
  private final ZkBaseDataAccessor<PushControlSignal> pushControlSignalAccessor;

  public VenicePushControlSignalAccessor(
      String clusterName,
      ZkClient zkClient,
      HelixAdapterSerializer adapterSerializer) {
    this.clusterName = clusterName;
    this.pushControlSignalParentPath = getPushControlSignalParentPath();
    registerSerializers(adapterSerializer);
    zkClient.setZkSerializer(adapterSerializer);
    this.pushControlSignalAccessor = new ZkBaseDataAccessor<>(zkClient);
  }

  /**
   * For testing purpose only.
   */
  public VenicePushControlSignalAccessor(
      String clusterName,
      ZkBaseDataAccessor<PushControlSignal> pushControlSignalAccessor) {
    this.clusterName = clusterName;
    this.pushControlSignalParentPath = getPushControlSignalParentPath();
    this.pushControlSignalAccessor = pushControlSignalAccessor;
  }

  private void registerSerializers(HelixAdapterSerializer adapterSerializer) {
    String pushControlSignalPattern = pushControlSignalParentPath + "/" + PathResourceRegistry.WILDCARD_MATCH_ANY;
    adapterSerializer.registerSerializer(pushControlSignalPattern, new PushControlSignalJSONSerializer());
  }

  @Override
  public void createPushControlSignal(PushControlSignal signal) {
    LOGGER.info("Creating push control signal for topic: {} in cluster: {}.", signal.getKafkaTopic(), clusterName);
    HelixUtils.create(pushControlSignalAccessor, getPushControlSignalPath(signal.getKafkaTopic()), signal);
  }

  @Override
  public PushControlSignal getPushControlSignal(String kafkaTopic) {
    return pushControlSignalAccessor.get(getPushControlSignalPath(kafkaTopic), null, AccessOption.PERSISTENT);
  }

  @Override
  public void updatePushControlSignal(PushControlSignal signal) {
    LOGGER.info("Updating push control signal for topic: {} in cluster: {}.", signal.getKafkaTopic(), clusterName);
    HelixUtils.update(pushControlSignalAccessor, getPushControlSignalPath(signal.getKafkaTopic()), signal);
  }

  @Override
  public void deletePushControlSignal(String kafkaTopic) {
    LOGGER.info("Deleting push control signal for topic: {} in cluster: {}.", kafkaTopic, clusterName);
    HelixUtils.remove(pushControlSignalAccessor, getPushControlSignalPath(kafkaTopic));
  }

  @Override
  public void subscribePushControlSignalChange(String kafkaTopic, IZkDataListener listener) {
    pushControlSignalAccessor.subscribeDataChanges(getPushControlSignalPath(kafkaTopic), listener);
  }

  @Override
  public void unsubscribePushControlSignalChange(String kafkaTopic, IZkDataListener listener) {
    pushControlSignalAccessor.unsubscribeDataChanges(getPushControlSignalPath(kafkaTopic), listener);
  }

  private String getPushControlSignalParentPath() {
    return HelixUtils.getHelixClusterZkPath(clusterName) + "/" + PUSH_CONTROL_SIGNALS;
  }

  private String getPushControlSignalPath(String kafkaTopic) {
    return pushControlSignalParentPath + "/" + kafkaTopic;
  }
}
