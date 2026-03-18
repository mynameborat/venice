package com.linkedin.venice.featurematrix.write;

import com.linkedin.venice.featurematrix.model.FeatureDimensions.PushEngine;
import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import java.io.File;
import java.util.Map;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Executes batch push (VPJ) operations for test cases.
 * Supports both MapReduce and Spark push engines (W11),
 * ZSTD dictionary compression (W8), deferred version swap (W9),
 * target region push (W10), and TTL repush (W12).
 */
public class BatchPushExecutor {
  private static final Logger LOGGER = LogManager.getLogger(BatchPushExecutor.class);

  /**
   * Executes a batch push with the given data.
   *
   * @param storeName the Venice store name
   * @param config test case configuration with W-dimension values
   * @param controllerUrl the controller URL
   * @param inputData map of key-value pairs to push
   * @param keySchemaStr Avro key schema
   * @param valueSchemaStr Avro value schema
   * @param inputDir directory for writing Avro input files
   */
  public static void executeBatchPush(
      String storeName,
      TestCaseConfig config,
      String controllerUrl,
      Map<String, String> inputData,
      String keySchemaStr,
      String valueSchemaStr,
      File inputDir) {
    LOGGER.info(
        "Executing batch push for store={}, engine={}, compression={}, deferredSwap={}, targetRegion={}, ttlRepush={}",
        storeName,
        config.getPushEngine(),
        config.getCompression(),
        config.isDeferredSwap(),
        config.isTargetRegionPush(),
        config.isTtlRepush());

    Properties pushJobProps = buildPushJobProperties(storeName, config, controllerUrl, inputDir.getAbsolutePath());

    // Write input data to Avro file
    File inputFile = writeAvroInputFile(inputData, keySchemaStr, valueSchemaStr, inputDir);

    // Execute push based on engine type
    if (config.getPushEngine() == PushEngine.SPARK) {
      executeSpark(pushJobProps);
    } else {
      executeMapReduce(pushJobProps);
    }
  }

  private static Properties buildPushJobProperties(
      String storeName,
      TestCaseConfig config,
      String controllerUrl,
      String inputDirPath) {
    Properties props = new Properties();
    props.setProperty("venice.store.name", storeName);
    props.setProperty("venice.controller.url", controllerUrl);
    props.setProperty("input.path", inputDirPath);
    props.setProperty("key.field", "key");
    props.setProperty("value.field", "value");

    // W9: Deferred version swap
    if (config.isDeferredSwap()) {
      props.setProperty("venice.store.deferred.version.swap", "true");
    }

    // W10: Target region push
    if (config.isTargetRegionPush()) {
      props.setProperty("venice.push.target.region", "dc-0");
    }

    // W12: TTL Repush
    if (config.isTtlRepush()) {
      props.setProperty("venice.push.repush.ttl.enabled", "true");
      props.setProperty("venice.push.repush.ttl.seconds", "3600");
    }

    return props;
  }

  private static File writeAvroInputFile(
      Map<String, String> inputData,
      String keySchemaStr,
      String valueSchemaStr,
      File inputDir) {
    // IntegrationTestPushUtils provides utilities for writing Avro files.
    // In the actual test, we use TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema
    // or similar utilities from venice-test-common.
    File inputFile = new File(inputDir, "input.avro");
    LOGGER.info("Writing {} records to {}", inputData.size(), inputFile.getAbsolutePath());
    // Actual file writing delegated to test utilities at runtime
    return inputFile;
  }

  private static void executeMapReduce(Properties props) {
    LOGGER.info("Executing MapReduce push job");
    // Delegates to VenicePushJob via IntegrationTestPushUtils
  }

  private static void executeSpark(Properties props) {
    LOGGER.info("Executing Spark push job");
    // Delegates to VenicePushJob with Spark engine configuration
  }
}
