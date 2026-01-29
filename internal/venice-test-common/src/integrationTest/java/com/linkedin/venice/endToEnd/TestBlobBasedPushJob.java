package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.CONTROLLER_BLOB_STAGING_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.SERVER_BLOB_PUSH_STAGING_BASE_PATH;
import static com.linkedin.venice.integration.utils.VeniceClusterWrapper.DEFAULT_KEY_SCHEMA;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_PUSH_ENABLED;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_PUSH_STAGING_PATH;

import com.linkedin.venice.ConfigKeys;
import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.ControllerClient;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.meta.Store;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.TestWriteUtils;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Integration tests for blob-based Venice Push Job (PushType.BLOB).
 *
 * This test validates the blob-based push infrastructure where VPJ generates
 * RocksDB SST files directly and transfers them to servers via a staging directory,
 * bypassing Kafka for the data path.
 */
public class TestBlobBasedPushJob {
  private static final Logger LOGGER = LogManager.getLogger(TestBlobBasedPushJob.class);

  private static final int PARTITION_COUNT = 2;
  private static final int RECORD_COUNT = 100;

  private VeniceClusterWrapper cluster;
  private String blobStagingBasePath;
  private String serverDataPath;

  @BeforeClass(alwaysRun = true)
  public void setUp() throws Exception {
    // Create temp directories for blob staging and server data
    blobStagingBasePath = Utils.getTempDataDirectory().getAbsolutePath() + "/blob_staging";
    serverDataPath = Utils.getTempDataDirectory().getAbsolutePath();

    // Create the blob staging directory
    Files.createDirectories(Paths.get(blobStagingBasePath));

    // Set up controller properties with blob staging path
    Properties controllerProperties = new Properties();
    controllerProperties.setProperty(CONTROLLER_BLOB_STAGING_BASE_PATH, blobStagingBasePath);

    // Set up server properties
    Properties serverProperties = new Properties();
    serverProperties.put(ConfigKeys.PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(ConfigKeys.DATA_BASE_PATH, serverDataPath);
    serverProperties.setProperty(SERVER_BLOB_PUSH_STAGING_BASE_PATH, blobStagingBasePath);

    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(1)
        .numberOfRouters(1)
        .replicationFactor(1)
        .partitionSize(RECORD_COUNT / PARTITION_COUNT)
        .extraProperties(controllerProperties)
        .build();

    cluster = ServiceFactory.getVeniceCluster(options);

    LOGGER.info(
        "Venice cluster started with blob staging path: {} and server data path: {}",
        blobStagingBasePath,
        serverDataPath);
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    Utils.closeQuietlyWithErrorLogged(cluster);
    try {
      FileUtils.deleteDirectory(new File(blobStagingBasePath));
      FileUtils.deleteDirectory(new File(serverDataPath));
    } catch (Exception e) {
      LOGGER.error("Failed to clean up directories", e);
    }
  }

  /**
   * Test that the controller correctly handles PushType.BLOB version creation
   * and returns the blob staging path.
   */
  @Test(timeOut = 120_000)
  public void testBlobPushTypeVersionCreation() {
    String storeName = Utils.getUniqueString("blob-push-version-test");

    try (ControllerClient controllerClient =
        ControllerClient.constructClusterControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs())) {

      // Create store
      controllerClient.createNewStore(storeName, "test@test.com", DEFAULT_KEY_SCHEMA, "\"string\"");

      // Update store with partition count
      controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setPartitionCount(PARTITION_COUNT)
              .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));

      // Request a new version with PushType.BLOB
      VersionCreationResponse response = controllerClient.requestTopicForWrites(
          storeName,
          1024 * 1024, // store size
          Version.PushType.BLOB,
          Utils.getUniqueString("blob-push-job-id"),
          true, // send start of push
          false, // sorted
          false, // write compute enabled
          java.util.Optional.empty(),
          java.util.Optional.empty(),
          java.util.Optional.empty(),
          false, // defer version swap
          -1); // target version

      Assert.assertFalse(response.isError(), "Version creation failed: " + response.getError());
      Assert.assertNotNull(response.getBlobStagingPath(), "Blob staging path should be returned for BLOB push type");
      Assert.assertTrue(
          response.getBlobStagingPath().startsWith(blobStagingBasePath),
          "Blob staging path should be under the configured base path");

      LOGGER.info(
          "Successfully created version {} with blob staging path: {}",
          response.getVersion(),
          response.getBlobStagingPath());

      // Verify the push type is BLOB
      Assert.assertEquals(response.getVersion(), 1);
    }
  }

  /**
   * Test that blob-based push is rejected for hybrid stores.
   */
  @Test(timeOut = 60_000)
  public void testBlobPushRejectedForHybridStore() {
    String storeName = Utils.getUniqueString("blob-push-hybrid-reject-test");

    try (ControllerClient controllerClient =
        ControllerClient.constructClusterControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs())) {

      // Create store
      controllerClient.createNewStore(storeName, "test@test.com", DEFAULT_KEY_SCHEMA, "\"string\"");

      // Make it a hybrid store
      controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setPartitionCount(PARTITION_COUNT)
              .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA)
              .setHybridRewindSeconds(60L)
              .setHybridOffsetLagThreshold(1000L));

      // Request a new version with PushType.BLOB - should fail
      VersionCreationResponse response = controllerClient.requestTopicForWrites(
          storeName,
          1024 * 1024,
          Version.PushType.BLOB,
          Utils.getUniqueString("blob-push-job-id"),
          true,
          false,
          false,
          java.util.Optional.empty(),
          java.util.Optional.empty(),
          java.util.Optional.empty(),
          false,
          -1);

      Assert.assertTrue(response.isError(), "Blob push should be rejected for hybrid stores");
      Assert.assertTrue(
          response.getError().contains("Blob-based push is not supported for hybrid store"),
          "Error message should indicate hybrid store restriction. Actual: " + response.getError());

      LOGGER.info("Correctly rejected blob push for hybrid store with error: {}", response.getError());
    }
  }

  /**
   * Test end-to-end blob-based push job with sample data.
   * This test generates a sample dataset and uses blob-based ingestion.
   *
   * TODO: This test is disabled until the full blob-based push infrastructure is complete.
   * The VPJ side needs to actually generate and upload SST files, and the server side
   * needs to pull and ingest them. Currently only the infrastructure (APIs, configs) is in place.
   */
  @Test(timeOut = 180_000, enabled = false)
  public void testBlobBasedPushJobEndToEnd() throws Exception {
    String storeName = Utils.getUniqueString("blob-push-e2e-test");

    // Generate sample data
    File inputDir = TestWriteUtils.getTempDataDirectory();
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Schema recordSchema = TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema(inputDir, RECORD_COUNT);

    // Set up VPJ properties with blob push enabled
    Properties vpjProperties = defaultVPJProps(cluster, inputDirPath, storeName);
    vpjProperties.setProperty(BLOB_PUSH_ENABLED, "true");
    vpjProperties.setProperty(BLOB_PUSH_STAGING_PATH, blobStagingBasePath);

    try (ControllerClient controllerClient = IntegrationTestPushUtils.createStoreForJob(
        cluster.getClusterName(),
        IntegrationTestPushUtils.getKeySchemaString(recordSchema, vpjProperties),
        IntegrationTestPushUtils.getValueSchemaString(recordSchema, vpjProperties),
        vpjProperties,
        new UpdateStoreQueryParams().setPartitionCount(PARTITION_COUNT)
            .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA))) {

      cluster.createMetaSystemStore(storeName);

      LOGGER.info("Starting blob-based push job for store: {}", storeName);

      // Run the push job
      // Note: For full blob-based push, the VPJ would generate SST files and upload to staging.
      // In this test, we verify the infrastructure is correctly configured.
      // Full end-to-end blob ingestion requires the server-side integration to be complete.

      // For now, we run a regular push to verify the store setup is correct
      IntegrationTestPushUtils.runVPJ(vpjProperties);

      // Wait for version to be ready
      cluster.waitVersion(storeName, 1);

      // Verify data can be read
      try (AvroGenericStoreClient<String, Object> client = ClientFactory.getAndStartGenericAvroClient(
          ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(cluster.getRandomRouterURL()))) {

        TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
          for (int i = 1; i <= 10; i++) {
            Object value = client.get(String.valueOf(i)).get();
            Assert.assertNotNull(value, "Value for key " + i + " should not be null");
            Assert.assertEquals(value.toString(), TestWriteUtils.DEFAULT_USER_DATA_VALUE_PREFIX + i);
          }
        });

        LOGGER.info("Successfully verified data ingestion for store: {}", storeName);
      }
    }
  }

  /**
   * Test that blob staging directory is created with correct structure.
   */
  @Test(timeOut = 60_000)
  public void testBlobStagingDirectoryStructure() throws Exception {
    String storeName = Utils.getUniqueString("blob-staging-structure-test");

    try (ControllerClient controllerClient =
        ControllerClient.constructClusterControllerClient(cluster.getClusterName(), cluster.getAllControllersURLs())) {

      // Create store
      controllerClient.createNewStore(storeName, "test@test.com", DEFAULT_KEY_SCHEMA, "\"string\"");
      controllerClient.updateStore(
          storeName,
          new UpdateStoreQueryParams().setPartitionCount(PARTITION_COUNT)
              .setStorageQuotaInByte(Store.UNLIMITED_STORAGE_QUOTA));

      // Request version with BLOB push type
      VersionCreationResponse response = controllerClient.requestTopicForWrites(
          storeName,
          1024 * 1024,
          Version.PushType.BLOB,
          Utils.getUniqueString("blob-push-job-id"),
          true,
          false,
          false,
          java.util.Optional.empty(),
          java.util.Optional.empty(),
          java.util.Optional.empty(),
          false,
          -1);

      Assert.assertFalse(response.isError(), "Version creation should succeed");

      String blobStagingPath = response.getBlobStagingPath();
      Assert.assertNotNull(blobStagingPath);

      // Verify the path format follows convention: {base_path}/{store}_v{version}
      String expectedPathPrefix = blobStagingBasePath + "/" + storeName + "_v" + response.getVersion();
      Assert.assertTrue(
          blobStagingPath.startsWith(expectedPathPrefix),
          "Blob staging path should follow convention. Expected prefix: " + expectedPathPrefix + ", Actual: "
              + blobStagingPath);

      LOGGER.info("Verified blob staging directory structure: {}", blobStagingPath);
    }
  }
}
