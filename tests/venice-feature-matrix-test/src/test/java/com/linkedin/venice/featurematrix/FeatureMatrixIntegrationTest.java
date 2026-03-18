package com.linkedin.venice.featurematrix;

import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.featurematrix.model.FeatureDimensions.Topology;
import com.linkedin.venice.featurematrix.model.PictModelParser;
import com.linkedin.venice.featurematrix.model.TestCaseConfig;
import com.linkedin.venice.featurematrix.reporting.FeatureMatrixReportListener;
import com.linkedin.venice.featurematrix.setup.ClientFactory;
import com.linkedin.venice.featurematrix.setup.ClientFactory.ReadClientWrapper;
import com.linkedin.venice.featurematrix.setup.StoreConfigurator;
import com.linkedin.venice.featurematrix.validation.DataIntegrityValidator;
import com.linkedin.venice.featurematrix.validation.ReadComputeValidator;
import com.linkedin.venice.featurematrix.validation.WriteComputeValidator;
import com.linkedin.venice.featurematrix.write.BatchPushExecutor;
import com.linkedin.venice.featurematrix.write.IncrementalPushExecutor;
import com.linkedin.venice.featurematrix.write.StreamingWriteExecutor;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Factory;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;


/**
 * Main integration test class for the Venice Feature Matrix.
 *
 * Uses TestNG Factory pattern to create one test instance per unique (S, RT, C)
 * cluster configuration. Each instance runs all (W, R) test cases that share
 * the same infrastructure settings.
 *
 * Test flow per invocation:
 * 1. Create store with W-dimension flags
 * 2. Write data (batch push, streaming, incremental push as applicable)
 * 3. Validate reads (single get, batch get, read compute, write compute)
 * 4. Cleanup store
 */
@Listeners(FeatureMatrixReportListener.class)
public class FeatureMatrixIntegrationTest {
  private static final Logger LOGGER = LogManager.getLogger(FeatureMatrixIntegrationTest.class);

  private static final String TEST_CASES_RESOURCE = "generated-test-cases.tsv";
  private static final String KEY_SCHEMA = "\"string\"";
  private static final String VALUE_SCHEMA = "\"string\"";
  private static final int NUM_TEST_RECORDS = 10;

  private final String clusterConfigKey;
  private final List<TestCaseConfig> testCases;
  private FeatureMatrixClusterSetup clusterSetup;
  private ControllerClient controllerClient;

  /**
   * Factory method that creates test instances grouped by cluster config.
   */
  @Factory(dataProvider = "clusterConfigs")
  public FeatureMatrixIntegrationTest(String clusterConfigKey, List<TestCaseConfig> testCases) {
    this.clusterConfigKey = clusterConfigKey;
    this.testCases = testCases;
  }

  @DataProvider(name = "clusterConfigs")
  public static Object[][] clusterConfigs() throws IOException {
    List<TestCaseConfig> allTestCases = PictModelParser.parseFromClasspath(TEST_CASES_RESOURCE);
    Map<String, List<TestCaseConfig>> groups = PictModelParser.groupByClusterConfig(allTestCases);

    Object[][] result = new Object[groups.size()][2];
    int i = 0;
    for (Map.Entry<String, List<TestCaseConfig>> entry: groups.entrySet()) {
      result[i][0] = entry.getKey();
      result[i][1] = entry.getValue();
      i++;
    }

    LOGGER.info("Created {} cluster config groups from {} total test cases", groups.size(), allTestCases.size());
    return result;
  }

  @BeforeClass
  public void setupCluster() {
    LOGGER.info("Setting up cluster for config: {}", clusterConfigKey);

    // Use the first test case as the representative for cluster config
    TestCaseConfig representative = testCases.get(0);
    clusterSetup = new FeatureMatrixClusterSetup(representative);
    clusterSetup.create();

    controllerClient = new ControllerClient(clusterSetup.getClusterName(), clusterSetup.getParentControllerUrl());

    LOGGER.info("Cluster setup complete. Will run {} test cases.", testCases.size());
  }

  @DataProvider(name = "storeAndClientMatrix")
  public Object[][] storeAndClientMatrix() {
    Object[][] result = new Object[testCases.size()][1];
    for (int i = 0; i < testCases.size(); i++) {
      result[i][0] = testCases.get(i);
    }
    return result;
  }

  @Test(dataProvider = "storeAndClientMatrix")
  public void testFeatureCombination(TestCaseConfig config) throws Exception {
    LOGGER.info("=== Running test case: {} ===", config.getTestName());

    String storeName = null;
    ReadClientWrapper clientWrapper = null;

    try {
      // 1. Create and configure store
      storeName = StoreConfigurator
          .createAndConfigureStore(controllerClient, clusterSetup.getClusterName(), config, KEY_SCHEMA, VALUE_SCHEMA);

      // 2. Generate test data
      Map<String, String> baseData = generateTestData("base", NUM_TEST_RECORDS);
      Map<String, String> rtData = new LinkedHashMap<>();
      Map<String, String> incrementalData = new LinkedHashMap<>();

      // 3. Write data based on topology
      File inputDir = createTempInputDir(config);

      if (config.getTopology() != Topology.NEARLINE_ONLY) {
        // Batch push for Batch-only and Hybrid
        BatchPushExecutor.executeBatchPush(
            storeName,
            config,
            clusterSetup.getParentControllerUrl(),
            baseData,
            KEY_SCHEMA,
            VALUE_SCHEMA,
            inputDir);
      }

      if (config.getTopology() == Topology.HYBRID || config.getTopology() == Topology.NEARLINE_ONLY) {
        // Streaming writes for Hybrid and Nearline-only
        rtData = generateTestData("rt", NUM_TEST_RECORDS);
        StreamingWriteExecutor.executeStreamingWrite(storeName, config, rtData, ""); // PubSub broker URL from cluster
                                                                                     // setup
      }

      if (config.isIncrementalPush()) {
        // Incremental push
        incrementalData = generateTestData("inc", NUM_TEST_RECORDS);
        IncrementalPushExecutor.executeIncrementalPush(
            storeName,
            config,
            clusterSetup.getParentControllerUrl(),
            incrementalData,
            KEY_SCHEMA,
            VALUE_SCHEMA,
            inputDir);
      }

      // 4. Create read client
      clientWrapper = ClientFactory.createReadClient(
          config,
          storeName,
          "", // Router URL from cluster setup
          ""); // D2 service name from cluster setup

      // 5. Build expected data (union of all writes)
      Map<String, String> expectedData = new HashMap<>();
      expectedData.putAll(baseData);
      expectedData.putAll(rtData);
      expectedData.putAll(incrementalData);

      // 6. Validate reads
      DataIntegrityValidator.validateSingleGet(clientWrapper, expectedData, config);
      DataIntegrityValidator.validateBatchGet(clientWrapper, expectedData, config);

      // 7. Validate read compute (if enabled)
      ReadComputeValidator.validateReadCompute(clientWrapper, config);

      // 8. Validate write compute (if enabled)
      if (config.isWriteCompute()) {
        WriteComputeValidator.validateWriteCompute(clientWrapper, config, "wc-key", "wc-value");
      }

      // 9. Validate chunking (if enabled)
      if (config.isChunking()) {
        String largeValue = generateLargeValue(1024 * 1024); // 1MB value to trigger chunking
        DataIntegrityValidator.validateChunking(clientWrapper, "large-key", largeValue, config);
      }

      LOGGER.info("=== Test case PASSED: {} ===", config.getTestName());

    } finally {
      // Cleanup
      if (clientWrapper != null) {
        clientWrapper.close();
      }
      if (storeName != null) {
        StoreConfigurator.deleteStore(controllerClient, storeName);
      }
    }
  }

  @AfterClass
  public void tearDownCluster() {
    LOGGER.info("Tearing down cluster for config: {}", clusterConfigKey);
    if (controllerClient != null) {
      controllerClient.close();
    }
    if (clusterSetup != null) {
      clusterSetup.tearDown();
    }
  }

  // ============================================================
  // Helper methods
  // ============================================================

  private Map<String, String> generateTestData(String prefix, int count) {
    Map<String, String> data = new LinkedHashMap<>();
    for (int i = 0; i < count; i++) {
      data.put(prefix + "-key-" + i, prefix + "-value-" + i);
    }
    return data;
  }

  private File createTempInputDir(TestCaseConfig config) {
    File dir = new File(System.getProperty("java.io.tmpdir"), "feature-matrix-" + config.getTestCaseId());
    dir.mkdirs();
    return dir;
  }

  private String generateLargeValue(int size) {
    StringBuilder sb = new StringBuilder(size);
    for (int i = 0; i < size; i++) {
      sb.append((char) ('a' + (i % 26)));
    }
    return sb.toString();
  }
}
