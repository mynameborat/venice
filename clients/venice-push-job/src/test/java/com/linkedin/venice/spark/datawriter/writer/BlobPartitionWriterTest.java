package com.linkedin.venice.spark.datawriter.writer;

import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_STORAGE_BASE_URI;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TOPIC_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_ID_PROP;

import com.linkedin.venice.blobtransfer.storage.BlobStoragePaths;
import com.linkedin.venice.blobtransfer.storage.LocalFsBlobStorageClient;
import com.linkedin.venice.spark.SparkConstants;
import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import com.linkedin.venice.utils.ByteUtils;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class BlobPartitionWriterTest {
  private SparkSession spark;
  private Path tempBlobDir;

  @BeforeClass
  public void setUp() {
    spark = SparkSession.builder()
        .appName("BlobPartitionWriterTest")
        .master(SparkConstants.DEFAULT_SPARK_CLUSTER)
        .getOrCreate();
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    if (spark != null) {
      spark.stop();
    }
  }

  @BeforeMethod
  public void setUpMethod() throws Exception {
    tempBlobDir = Files.createTempDirectory("blob-test-");
  }

  @AfterMethod(alwaysRun = true)
  public void tearDownMethod() throws Exception {
    if (tempBlobDir != null) {
      // Clean up temp directory
      Files.walk(tempBlobDir).sorted(java.util.Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
  }

  @Test
  public void testPrependSchemaId() {
    byte[] value = new byte[] { 10, 20, 30 };
    int schemaId = 42;

    byte[] result = BlobPartitionWriter.prependSchemaId(value, schemaId);

    // Result should be 4 bytes for schema ID + 3 bytes for value = 7 bytes
    Assert.assertEquals(result.length, 7);

    // Verify the first 4 bytes are the big-endian schema ID
    int readSchemaId = ByteUtils.readInt(result, 0);
    Assert.assertEquals(readSchemaId, schemaId);

    // Verify the remaining bytes are the original value
    Assert.assertEquals(result[4], (byte) 10);
    Assert.assertEquals(result[5], (byte) 20);
    Assert.assertEquals(result[6], (byte) 30);
  }

  @Test
  public void testPrependSchemaId_largeSchemaId() {
    byte[] value = new byte[] { 1 };
    int schemaId = 65536;

    byte[] result = BlobPartitionWriter.prependSchemaId(value, schemaId);

    Assert.assertEquals(result.length, 5);
    Assert.assertEquals(ByteUtils.readInt(result, 0), schemaId);
    Assert.assertEquals(result[4], (byte) 1);
  }

  @Test
  public void testProcessRows_emptyPartition() throws Exception {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    String blobBaseUri = tempBlobDir.toAbsolutePath().toString();
    Properties props = createTestProperties(blobBaseUri);

    // Run inside a Spark job to have TaskContext available
    // Use LocalFsBlobStorageClient (real, serialization-safe) instead of a mock
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    jsc.parallelize(Collections.singletonList(1), 1).foreachPartition(partition -> {
      LocalFsBlobStorageClient client = new LocalFsBlobStorageClient();
      try (BlobPartitionWriter writer = new BlobPartitionWriter(props, accumulators, client)) {
        writer.processRows(Collections.emptyIterator());
      } finally {
        client.close();
      }
    });

    // With no data, no SST file should exist
    String sstPath = BlobStoragePaths.sstFile(blobBaseUri, "test-store", 1, 0, "data_0.sst");
    Assert.assertFalse(new File(sstPath).exists(), "SST file should not exist for empty partition");
  }

  @Test
  public void testProcessRows_writesAndUploadsSstFile() throws Exception {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    String blobBaseUri = tempBlobDir.toAbsolutePath().toString();
    Properties props = createTestProperties(blobBaseUri);

    // Create sorted test rows (keys must be sorted for SstFileWriter)
    List<Row> rows = new ArrayList<>();
    rows.add(createRow(new byte[] { 1, 2, 3 }, new byte[] { 10, 20 }));
    rows.add(createRow(new byte[] { 4, 5, 6 }, new byte[] { 30, 40 }));

    // Run inside a Spark job so TaskContext is available
    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    jsc.parallelize(Collections.singletonList(1), 1).foreachPartition(partition -> {
      LocalFsBlobStorageClient client = new LocalFsBlobStorageClient();
      try (BlobPartitionWriter writer = new BlobPartitionWriter(props, accumulators, client)) {
        writer.processRows(rows.iterator());
      } finally {
        client.close();
      }
    });

    // Verify SST file was uploaded to the correct path
    String expectedPath = BlobStoragePaths.sstFile(blobBaseUri, "test-store", 1, 0, "data_0.sst");
    File sstFile = new File(expectedPath);
    Assert.assertTrue(sstFile.exists(), "SST file should exist at: " + expectedPath);
    Assert.assertTrue(sstFile.length() > 0, "SST file should not be empty");
  }

  private Properties createTestProperties(String blobBaseUri) {
    Properties props = new Properties();
    props.setProperty(VALUE_SCHEMA_ID_PROP, "1");
    props.setProperty(TOPIC_PROP, "test-store_v1");
    props.setProperty(BLOB_STORAGE_BASE_URI, blobBaseUri);
    return props;
  }

  private Row createRow(byte[] key, byte[] value) {
    return new GenericRowWithSchema(new Object[] { key, value, null }, SparkConstants.DEFAULT_SCHEMA);
  }
}
