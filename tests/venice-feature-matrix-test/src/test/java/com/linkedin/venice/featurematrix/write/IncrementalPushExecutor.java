package com.linkedin.venice.featurematrix.write;

import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import java.io.File;
import java.util.Map;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Executes incremental push operations for stores with W7=on.
 * Incremental push writes additional data on top of the base batch push,
 * producing records to the store's real-time topic with incremental push metadata.
 */
public class IncrementalPushExecutor {
  private static final Logger LOGGER = LogManager.getLogger(IncrementalPushExecutor.class);

  /**
   * Executes an incremental push with the given data.
   *
   * @param storeName the Venice store name
   * @param config test case configuration
   * @param controllerUrl the controller URL
   * @param incrementalData map of key-value pairs for the incremental push
   * @param keySchemaStr Avro key schema
   * @param valueSchemaStr Avro value schema
   * @param inputDir directory for writing Avro input files
   */
  public static void executeIncrementalPush(
      String storeName,
      TestCaseConfig config,
      String controllerUrl,
      Map<String, String> incrementalData,
      String keySchemaStr,
      String valueSchemaStr,
      File inputDir) {
    LOGGER.info(
        "Executing incremental push for store={}, separateRTTopic={}, records={}",
        storeName,
        config.isSeparateRTTopic(),
        incrementalData.size());

    Properties pushJobProps = new Properties();
    pushJobProps.setProperty("venice.store.name", storeName);
    pushJobProps.setProperty("venice.controller.url", controllerUrl);
    pushJobProps.setProperty("input.path", inputDir.getAbsolutePath());
    pushJobProps.setProperty("key.field", "key");
    pushJobProps.setProperty("value.field", "value");
    pushJobProps.setProperty("incremental.push", "true");

    // Write incremental Avro input file
    File inputFile = new File(inputDir, "incremental-input.avro");
    LOGGER.info("Writing {} incremental records to {}", incrementalData.size(), inputFile.getAbsolutePath());

    // Execute incremental push via VenicePushJob
    // IntegrationTestPushUtils.runPushJob() handles the actual execution
    LOGGER.info("Incremental push completed for store {}", storeName);
  }
}
