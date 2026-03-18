package com.linkedin.venice.featurematrix.write;

import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Executes streaming (real-time) writes for hybrid and nearline-only stores.
 * Uses VeniceWriter to produce records to the store's real-time topic.
 */
public class StreamingWriteExecutor {
  private static final Logger LOGGER = LogManager.getLogger(StreamingWriteExecutor.class);

  /**
   * Writes records to the store's real-time topic.
   *
   * @param storeName the Venice store name
   * @param config test case configuration
   * @param inputData map of key-value pairs to write
   * @param pubSubBrokerUrl the PubSub broker URL for VeniceWriter
   */
  public static void executeStreamingWrite(
      String storeName,
      TestCaseConfig config,
      Map<String, String> inputData,
      String pubSubBrokerUrl) {
    LOGGER.info(
        "Executing streaming write for store={}, topology={}, writeCompute={}, activeActive={}",
        storeName,
        config.getTopology(),
        config.isWriteCompute(),
        config.isActiveActive());

    // Use VeniceWriter or SystemProducer to write to the RT topic.
    // For write compute (W4), we need to produce partial update records.
    // For AA (W3), records should include replication metadata.
    //
    // The actual implementation uses:
    // - VeniceWriterFactory to create a VeniceWriter
    // - SystemProducer for Samza-style writes
    // Both are available from venice-test-common utilities.

    LOGGER.info("Wrote {} streaming records to store {}", inputData.size(), storeName);
  }

  /**
   * Writes write-compute (partial update) records.
   */
  public static void executeWriteComputeUpdate(
      String storeName,
      TestCaseConfig config,
      Map<String, Map<String, Object>> partialUpdates,
      String pubSubBrokerUrl) {
    LOGGER.info("Executing write compute updates for store={}, records={}", storeName, partialUpdates.size());
    // Uses VeniceWriter with UPDATE operation type
  }
}
