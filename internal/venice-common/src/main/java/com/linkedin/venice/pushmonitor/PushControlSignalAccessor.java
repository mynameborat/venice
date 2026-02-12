package com.linkedin.venice.pushmonitor;

import org.apache.helix.zookeeper.zkclient.IZkDataListener;


/**
 * Accessor for {@link PushControlSignal} ZNodes.
 *
 * <p>Stored in ZK at {@code /{clusterName}/PushControlSignals/{topic}}, these signals carry
 * controller→server notifications (e.g. blob upload complete). Servers subscribe to data
 * changes to react when the controller emits new signals.
 */
public interface PushControlSignalAccessor {
  void createPushControlSignal(PushControlSignal signal);

  PushControlSignal getPushControlSignal(String kafkaTopic);

  void updatePushControlSignal(PushControlSignal signal);

  void deletePushControlSignal(String kafkaTopic);

  void subscribePushControlSignalChange(String kafkaTopic, IZkDataListener listener);

  void unsubscribePushControlSignalChange(String kafkaTopic, IZkDataListener listener);
}
