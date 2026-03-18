package com.linkedin.venice.featurematrix.write;

import com.linkedin.venice.integration.utils.VeniceTwoLayerMultiRegionMultiClusterWrapper;
import com.linkedin.venice.samza.VeniceSystemProducer;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Executes streaming (real-time) writes using Samza's VeniceSystemProducer
 * against a multi-region cluster.
 */
public class StreamingWriteExecutor {
  private static final Logger LOGGER = LogManager.getLogger(StreamingWriteExecutor.class);

  /**
   * Writes streaming records to the store's real-time topic in the specified region.
   *
   * @param storeName the Venice store name
   * @param multiRegionCluster the multi-region cluster wrapper
   * @param regionId the region to write to (0-based)
   * @param startKey starting key index (inclusive)
   * @param endKey ending key index (inclusive)
   */
  public static void executeStreamingWrite(
      String storeName,
      VeniceTwoLayerMultiRegionMultiClusterWrapper multiRegionCluster,
      int regionId,
      int startKey,
      int endKey) {
    LOGGER.info("Executing streaming write for store={}, region={}, keys {}-{}", storeName, regionId, startKey, endKey);

    try (VeniceSystemProducer veniceProducer =
        IntegrationTestPushUtils.getSamzaProducerForStream(multiRegionCluster, regionId, storeName)) {
      for (int i = startKey; i <= endKey; i++) {
        IntegrationTestPushUtils.sendStreamingRecord(veniceProducer, storeName, Integer.toString(i), "stream_" + i);
      }
    }

    LOGGER.info("Wrote {} streaming records to store {} in region {}", endKey - startKey + 1, storeName, regionId);
  }
}
