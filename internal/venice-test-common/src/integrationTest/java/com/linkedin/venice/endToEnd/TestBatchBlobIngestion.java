package com.linkedin.venice.endToEnd;

import static com.linkedin.davinci.store.rocksdb.RocksDBServerConfig.ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED;
import static com.linkedin.venice.ConfigKeys.BLOB_INGESTION_POLL_INTERVAL_MS;
import static com.linkedin.venice.ConfigKeys.BLOB_STORAGE_BASE_URI;
import static com.linkedin.venice.ConfigKeys.BLOB_STORAGE_TYPE;
import static com.linkedin.venice.ConfigKeys.DATA_BASE_PATH;
import static com.linkedin.venice.ConfigKeys.PERSISTENCE_TYPE;
import static com.linkedin.venice.ConfigKeys.SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.createStoreForJob;
import static com.linkedin.venice.utils.IntegrationTestPushUtils.defaultVPJProps;
import static com.linkedin.venice.utils.TestWriteUtils.getTempDataDirectory;
import static com.linkedin.venice.utils.TestWriteUtils.writeSimpleAvroFileWithStringToStringSchema;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DATA_WRITER_COMPUTE_JOB_CLASS;

import com.linkedin.venice.client.store.AvroGenericStoreClient;
import com.linkedin.venice.client.store.ClientConfig;
import com.linkedin.venice.client.store.ClientFactory;
import com.linkedin.venice.controllerapi.UpdateStoreQueryParams;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterCreateOptions;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.meta.PersistenceType;
import com.linkedin.venice.spark.datawriter.jobs.DataWriterSparkJob;
import com.linkedin.venice.utils.IntegrationTestPushUtils;
import com.linkedin.venice.utils.KeyAndValueSchemas;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.Utils;
import java.io.File;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * End-to-end integration test for blob-based ingestion (BATCH_BLOB).
 *
 * Verifies the full pipeline: VPJ generates SST files, uploads to blob storage (LOCAL_FS),
 * controller signals completion via ZK, servers download SSTs and ingest into RocksDB.
 *
 * When a store has blobBasedIngestionEnabled=true, the controller auto-upgrades BATCH to BATCH_BLOB.
 * VPJ then uses BlobDataWriterSparkJob (SST generation) and the server routes to BlobIngestionTask.
 */
@Test(singleThreaded = true)
public class TestBatchBlobIngestion {
  private static final Logger LOGGER = LogManager.getLogger(TestBatchBlobIngestion.class);
  private static final int TEST_TIMEOUT = 120 * Time.MS_PER_SECOND;
  private static final String BASE_DATA_PATH = Utils.getTempDataDirectory().getAbsolutePath();

  private VeniceClusterWrapper veniceCluster;
  private File blobStorageDir;

  @BeforeClass(alwaysRun = true)
  public void setUp() {
    blobStorageDir = Utils.getTempDataDirectory();

    // Controller properties: configure blob storage with LOCAL_FS
    Properties extraProperties = new Properties();
    extraProperties.setProperty(BLOB_STORAGE_TYPE, "LOCAL_FS");
    extraProperties.setProperty(BLOB_STORAGE_BASE_URI, blobStorageDir.getAbsolutePath());

    VeniceClusterCreateOptions options = new VeniceClusterCreateOptions.Builder().numberOfControllers(1)
        .numberOfServers(0)
        .numberOfRouters(0)
        .replicationFactor(1)
        .extraProperties(extraProperties)
        .build();
    veniceCluster = ServiceFactory.getVeniceCluster(options);

    // Add server with RocksDB and blob ingestion polling config
    Properties serverProperties = new Properties();
    serverProperties.put(PERSISTENCE_TYPE, PersistenceType.ROCKS_DB);
    serverProperties.setProperty(ROCKSDB_PLAIN_TABLE_FORMAT_ENABLED, "false");
    serverProperties.setProperty(SERVER_DATABASE_CHECKSUM_VERIFICATION_ENABLED, "true");
    serverProperties.setProperty(BLOB_INGESTION_POLL_INTERVAL_MS, "1000");
    serverProperties.setProperty(DATA_BASE_PATH, BASE_DATA_PATH);
    veniceCluster.addVeniceServer(new Properties(), serverProperties);

    // Add router
    veniceCluster.addVeniceRouter(new Properties());
  }

  @AfterClass(alwaysRun = true)
  public void cleanUp() {
    Utils.closeQuietlyWithErrorLogged(veniceCluster);
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBlobBasedBatchPush() throws Exception {
    File inputDir = getTempDataDirectory();
    KeyAndValueSchemas schemas = new KeyAndValueSchemas(writeSimpleAvroFileWithStringToStringSchema(inputDir));
    String storeName = Utils.getUniqueString("blob-batch-store");
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    props.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());

    // Create store with blob-based ingestion enabled
    createStoreForJob(
        veniceCluster.getClusterName(),
        schemas.getKey().toString(),
        schemas.getValue().toString(),
        props,
        new UpdateStoreQueryParams().setBlobBasedIngestionEnabled(true)).close();

    // Run VPJ push (controller auto-upgrades BATCH -> BATCH_BLOB)
    IntegrationTestPushUtils.runVPJ(props);
    veniceCluster.refreshAllRouterMetaData();

    // Verify data is queryable
    try (AvroGenericStoreClient<String, Object> avroClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        for (int i = 1; i <= 100; i++) {
          Object value = avroClient.get(Integer.toString(i)).get();
          Assert.assertNotNull(value, "Value for key " + i + " should not be null");
          Assert.assertEquals(value.toString(), "test_name_" + i);
        }
      });
    }
  }

  @Test(timeOut = TEST_TIMEOUT)
  public void testBlobBasedBatchPushMultiplePartitions() throws Exception {
    File inputDir = getTempDataDirectory();
    KeyAndValueSchemas schemas = new KeyAndValueSchemas(writeSimpleAvroFileWithStringToStringSchema(inputDir));
    String storeName = Utils.getUniqueString("blob-batch-multi-part-store");
    String inputDirPath = "file://" + inputDir.getAbsolutePath();
    Properties props = defaultVPJProps(veniceCluster, inputDirPath, storeName);
    props.setProperty(DATA_WRITER_COMPUTE_JOB_CLASS, DataWriterSparkJob.class.getCanonicalName());

    // Create store with blob-based ingestion enabled and multiple partitions
    createStoreForJob(
        veniceCluster.getClusterName(),
        schemas.getKey().toString(),
        schemas.getValue().toString(),
        props,
        new UpdateStoreQueryParams().setBlobBasedIngestionEnabled(true).setPartitionCount(3)).close();

    // Run VPJ push
    IntegrationTestPushUtils.runVPJ(props);
    veniceCluster.refreshAllRouterMetaData();

    // Verify data is queryable across all partitions
    try (AvroGenericStoreClient<String, Object> avroClient = ClientFactory.getAndStartGenericAvroClient(
        ClientConfig.defaultGenericClientConfig(storeName).setVeniceURL(veniceCluster.getRandomRouterURL()))) {
      TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, true, () -> {
        for (int i = 1; i <= 100; i++) {
          Object value = avroClient.get(Integer.toString(i)).get();
          Assert.assertNotNull(value, "Value for key " + i + " should not be null");
          Assert.assertEquals(value.toString(), "test_name_" + i);
        }
      });
    }
  }
}
