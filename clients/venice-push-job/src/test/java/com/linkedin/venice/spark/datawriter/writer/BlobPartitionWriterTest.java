package com.linkedin.venice.spark.datawriter.writer;

import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_SST_FILE_SIZE_THRESHOLD_BYTES;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_SST_TABLE_FORMAT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_STORAGE_BASE_URI;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TOPIC_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_ID_PROP;

import com.linkedin.venice.blobtransfer.storage.BlobStoragePaths;
import com.linkedin.venice.blobtransfer.storage.LocalFsBlobStorageClient;
import com.linkedin.venice.spark.SparkConstants;
import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import com.linkedin.venice.utils.ByteUtils;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.SstFileReader;
import org.rocksdb.SstFileReaderIterator;
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
  public void testPrependSchemaIdReusable_matchesNonReusePath() {
    byte[] value = new byte[] { 10, 20, 30 };
    int schemaId = 42;
    byte[] buffer = new byte[16];

    byte[] resultBuffer = BlobPartitionWriter.prependSchemaIdReusable(value, schemaId, buffer);
    byte[] expected = BlobPartitionWriter.prependSchemaId(value, schemaId);

    // The reusable buffer content should match the non-reuse path byte-for-byte
    int requiredSize = 4 + value.length;
    for (int i = 0; i < requiredSize; i++) {
      Assert.assertEquals(resultBuffer[i], expected[i], "Mismatch at byte " + i);
    }
  }

  @Test
  public void testPrependSchemaIdReusable_bufferGrowsWhenTooSmall() {
    byte[] smallBuffer = new byte[4]; // too small for 4-byte prefix + value
    byte[] value = new byte[] { 1, 2, 3, 4, 5 };
    int schemaId = 1;

    byte[] grown = BlobPartitionWriter.prependSchemaIdReusable(value, schemaId, smallBuffer);

    // Buffer should have grown (returned buffer is different from input)
    Assert.assertTrue(grown.length >= 4 + value.length, "Buffer should grow to fit the value");
    Assert.assertEquals(ByteUtils.readInt(grown, 0), schemaId);
    for (int i = 0; i < value.length; i++) {
      Assert.assertEquals(grown[4 + i], value[i]);
    }
  }

  @Test
  public void testPrependSchemaIdReusable_uniformSizeReusesSameBuffer() {
    byte[] buffer = new byte[64];
    int schemaId = 7;

    // Simulate uniform-size values (all same length) — buffer should stabilize
    byte[] value1 = new byte[] { 1, 2, 3 };
    byte[] result1 = BlobPartitionWriter.prependSchemaIdReusable(value1, schemaId, buffer);

    byte[] value2 = new byte[] { 4, 5, 6 };
    byte[] result2 = BlobPartitionWriter.prependSchemaIdReusable(value2, schemaId, result1);

    // Same buffer object should be returned (no growth needed)
    Assert.assertSame(result1, result2, "Buffer should be reused when size is sufficient");

    // Verify second value was written correctly
    Assert.assertEquals(ByteUtils.readInt(result2, 0), schemaId);
    Assert.assertEquals(result2[4], (byte) 4);
    Assert.assertEquals(result2[5], (byte) 5);
    Assert.assertEquals(result2[6], (byte) 6);
  }

  @Test
  public void testPrependSchemaIdReusable_variableSizeValues() {
    byte[] buffer = new byte[8]; // small initial buffer
    int schemaId = 99;

    // Increasing sizes to exercise growth
    byte[][] values = { new byte[2], new byte[10], new byte[100], new byte[50] };

    for (byte[] value: values) {
      // Fill value with a recognizable pattern
      java.util.Arrays.fill(value, (byte) value.length);
      buffer = BlobPartitionWriter.prependSchemaIdReusable(value, schemaId, buffer);

      byte[] expected = BlobPartitionWriter.prependSchemaId(value, schemaId);
      int requiredSize = 4 + value.length;
      Assert.assertTrue(buffer.length >= requiredSize);
      for (int i = 0; i < requiredSize; i++) {
        Assert.assertEquals(buffer[i], expected[i], "Mismatch at byte " + i + " for value of length " + value.length);
      }
    }

    // After processing value of length 100, buffer should be large enough for length 50
    // without growing again
    byte[] beforeLastCall = buffer;
    byte[] smallValue = new byte[50];
    buffer = BlobPartitionWriter.prependSchemaIdReusable(smallValue, schemaId, buffer);
    Assert.assertSame(beforeLastCall, buffer, "Buffer should not grow when value fits");
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

  @Test
  public void testProcessRows_withBlockBasedTableFormat() throws Exception {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    String blobBaseUri = tempBlobDir.toAbsolutePath().toString();
    Properties props = createTestProperties(blobBaseUri);
    props.setProperty(BLOB_SST_TABLE_FORMAT, "BLOCK_BASED_TABLE");

    List<Row> rows = new ArrayList<>();
    rows.add(createRow(new byte[] { 1, 2, 3 }, new byte[] { 10, 20 }));

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    jsc.parallelize(Collections.singletonList(1), 1).foreachPartition(partition -> {
      LocalFsBlobStorageClient client = new LocalFsBlobStorageClient();
      try (BlobPartitionWriter writer = new BlobPartitionWriter(props, accumulators, client)) {
        writer.processRows(rows.iterator());
      } finally {
        client.close();
      }
    });

    String expectedPath = BlobStoragePaths.sstFile(blobBaseUri, "test-store", 1, 0, "data_0.sst");
    File sstFile = new File(expectedPath);
    Assert.assertTrue(sstFile.exists(), "SST file should exist with BLOCK_BASED_TABLE format");
    Assert.assertTrue(sstFile.length() > 0, "SST file should not be empty");
  }

  @Test
  public void testProcessRows_withPlainTableFormat() throws Exception {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    String blobBaseUri = tempBlobDir.toAbsolutePath().toString();
    Properties props = createTestProperties(blobBaseUri);
    props.setProperty(BLOB_SST_TABLE_FORMAT, "PLAIN_TABLE");

    List<Row> rows = new ArrayList<>();
    rows.add(createRow(new byte[] { 1, 2, 3 }, new byte[] { 10, 20 }));

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    jsc.parallelize(Collections.singletonList(1), 1).foreachPartition(partition -> {
      LocalFsBlobStorageClient client = new LocalFsBlobStorageClient();
      try (BlobPartitionWriter writer = new BlobPartitionWriter(props, accumulators, client)) {
        writer.processRows(rows.iterator());
      } finally {
        client.close();
      }
    });

    String expectedPath = BlobStoragePaths.sstFile(blobBaseUri, "test-store", 1, 0, "data_0.sst");
    File sstFile = new File(expectedPath);
    Assert.assertTrue(sstFile.exists(), "SST file should exist with PLAIN_TABLE format");
    Assert.assertTrue(sstFile.length() > 0, "SST file should not be empty");
  }

  @Test
  public void testProcessRows_defaultsToBlockBasedWhenFormatNotSet() throws Exception {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    String blobBaseUri = tempBlobDir.toAbsolutePath().toString();
    Properties props = createTestProperties(blobBaseUri);
    // Do NOT set BLOB_SST_TABLE_FORMAT — should default to BLOCK_BASED_TABLE

    List<Row> rows = new ArrayList<>();
    rows.add(createRow(new byte[] { 1, 2, 3 }, new byte[] { 10, 20 }));

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    jsc.parallelize(Collections.singletonList(1), 1).foreachPartition(partition -> {
      LocalFsBlobStorageClient client = new LocalFsBlobStorageClient();
      try (BlobPartitionWriter writer = new BlobPartitionWriter(props, accumulators, client)) {
        writer.processRows(rows.iterator());
      } finally {
        client.close();
      }
    });

    String expectedPath = BlobStoragePaths.sstFile(blobBaseUri, "test-store", 1, 0, "data_0.sst");
    File sstFile = new File(expectedPath);
    Assert.assertTrue(sstFile.exists(), "SST file should exist with default table format");
  }

  @Test
  public void testComposeSstFileName() {
    Assert.assertEquals(BlobPartitionWriter.composeSstFileName(0), "data_0.sst");
    Assert.assertEquals(BlobPartitionWriter.composeSstFileName(1), "data_1.sst");
    Assert.assertEquals(BlobPartitionWriter.composeSstFileName(99), "data_99.sst");
  }

  @Test
  public void testProcessRows_sstFileSplitting() throws Exception {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    String blobBaseUri = tempBlobDir.toAbsolutePath().toString();
    Properties props = createTestProperties(blobBaseUri);
    // Set a very small threshold (1 byte) so that every FILE_SIZE_CHECK_INTERVAL records triggers a split
    props.setProperty(BLOB_SST_FILE_SIZE_THRESHOLD_BYTES, "1");

    // Generate enough sorted rows to exceed FILE_SIZE_CHECK_INTERVAL and trigger at least one split.
    // We need > FILE_SIZE_CHECK_INTERVAL records to trigger the first size check.
    int numRows = BlobPartitionWriter.FILE_SIZE_CHECK_INTERVAL * 2 + 1;
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < numRows; i++) {
      // Keys must be sorted — use 4-byte big-endian int for natural sort order
      byte[] key = new byte[4];
      key[0] = (byte) ((i >> 24) & 0xFF);
      key[1] = (byte) ((i >> 16) & 0xFF);
      key[2] = (byte) ((i >> 8) & 0xFF);
      key[3] = (byte) (i & 0xFF);
      byte[] value = new byte[] { (byte) (i & 0xFF) };
      rows.add(createRow(key, value));
    }

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    jsc.parallelize(Collections.singletonList(1), 1).foreachPartition(partition -> {
      LocalFsBlobStorageClient client = new LocalFsBlobStorageClient();
      try (BlobPartitionWriter writer = new BlobPartitionWriter(props, accumulators, client)) {
        writer.processRows(rows.iterator());
        // With threshold=1 and 2001 records, we should get at least 2 SST files
        Assert.assertTrue(
            writer.getSstFileCount() >= 2,
            "Expected at least 2 SST files with tiny threshold, got " + writer.getSstFileCount());
      } finally {
        client.close();
      }
    });

    // Verify data_0.sst and data_1.sst exist
    String sst0Path = BlobStoragePaths.sstFile(blobBaseUri, "test-store", 1, 0, "data_0.sst");
    String sst1Path = BlobStoragePaths.sstFile(blobBaseUri, "test-store", 1, 0, "data_1.sst");
    Assert.assertTrue(new File(sst0Path).exists(), "data_0.sst should exist at: " + sst0Path);
    Assert.assertTrue(new File(sst1Path).exists(), "data_1.sst should exist at: " + sst1Path);
    Assert.assertTrue(new File(sst0Path).length() > 0, "data_0.sst should not be empty");
    Assert.assertTrue(new File(sst1Path).length() > 0, "data_1.sst should not be empty");
  }

  @Test
  public void testProcessRows_singleFileWithLargeThreshold() throws Exception {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    String blobBaseUri = tempBlobDir.toAbsolutePath().toString();
    Properties props = createTestProperties(blobBaseUri);
    // Set a very large threshold so no splitting occurs
    props.setProperty(BLOB_SST_FILE_SIZE_THRESHOLD_BYTES, String.valueOf(1024 * 1024 * 1024));

    List<Row> rows = new ArrayList<>();
    rows.add(createRow(new byte[] { 1, 2, 3 }, new byte[] { 10, 20 }));
    rows.add(createRow(new byte[] { 4, 5, 6 }, new byte[] { 30, 40 }));

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    jsc.parallelize(Collections.singletonList(1), 1).foreachPartition(partition -> {
      LocalFsBlobStorageClient client = new LocalFsBlobStorageClient();
      try (BlobPartitionWriter writer = new BlobPartitionWriter(props, accumulators, client)) {
        writer.processRows(rows.iterator());
        Assert.assertEquals(writer.getSstFileCount(), 1, "Should produce exactly 1 SST file with large threshold");
      } finally {
        client.close();
      }
    });

    // Only data_0.sst should exist, no data_1.sst
    String sst0Path = BlobStoragePaths.sstFile(blobBaseUri, "test-store", 1, 0, "data_0.sst");
    String sst1Path = BlobStoragePaths.sstFile(blobBaseUri, "test-store", 1, 0, "data_1.sst");
    Assert.assertTrue(new File(sst0Path).exists(), "data_0.sst should exist");
    Assert.assertFalse(new File(sst1Path).exists(), "data_1.sst should NOT exist with large threshold");
  }

  /**
   * Verifies the sorted key invariant across split SST files: file N's last key < file N+1's first key.
   * This is the core correctness property of SST splitting — if violated, RocksDB ingest would fail
   * or produce incorrect results.
   */
  @Test
  public void testProcessRows_splitFileKeysAreNonOverlapping() throws Exception {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    String blobBaseUri = tempBlobDir.toAbsolutePath().toString();
    Properties props = createTestProperties(blobBaseUri);
    props.setProperty(BLOB_SST_FILE_SIZE_THRESHOLD_BYTES, "1");

    int numRows = BlobPartitionWriter.FILE_SIZE_CHECK_INTERVAL * 3 + 1;
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < numRows; i++) {
      byte[] key = ByteBuffer.allocate(4).putInt(i).array();
      byte[] value = new byte[] { (byte) (i & 0xFF) };
      rows.add(createRow(key, value));
    }

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    jsc.parallelize(Collections.singletonList(1), 1).foreachPartition(partition -> {
      LocalFsBlobStorageClient client = new LocalFsBlobStorageClient();
      try (BlobPartitionWriter writer = new BlobPartitionWriter(props, accumulators, client)) {
        writer.processRows(rows.iterator());
        Assert.assertTrue(
            writer.getSstFileCount() >= 3,
            "Expected at least 3 SST files for key invariant test, got " + writer.getSstFileCount());
      } finally {
        client.close();
      }
    });

    // Discover SST files from the blob directory (Spark serialization boundary prevents
    // passing file count back to driver, so we enumerate what was uploaded)
    String partitionDir = BlobStoragePaths.partitionDir(blobBaseUri, "test-store", 1, 0);
    List<String> sstFiles = new ArrayList<>();
    for (int i = 0;; i++) {
      File f = new File(partitionDir + "/" + BlobPartitionWriter.composeSstFileName(i));
      if (!f.exists()) {
        break;
      }
      sstFiles.add(f.getAbsolutePath());
    }
    Assert.assertTrue(sstFiles.size() >= 3, "Expected at least 3 SST files on disk, got " + sstFiles.size());

    // Read first and last keys from each SST file and verify ordering
    byte[] previousLastKey = null;
    for (int fileIdx = 0; fileIdx < sstFiles.size(); fileIdx++) {
      String sstPath = sstFiles.get(fileIdx);

      try (Options options = new Options(); SstFileReader reader = new SstFileReader(options)) {
        reader.open(sstPath);
        try (ReadOptions readOptions = new ReadOptions();
            SstFileReaderIterator iter = reader.newIterator(readOptions)) {
          // Get first key
          iter.seekToFirst();
          Assert.assertTrue(iter.isValid(), "SST file " + fileIdx + " should have at least one entry");
          byte[] firstKey = iter.key();

          // Verify this file's first key > previous file's last key
          if (previousLastKey != null) {
            Assert.assertTrue(
                compareBytes(previousLastKey, firstKey) < 0,
                "Key ordering violated: file " + (fileIdx - 1) + " last key " + Arrays.toString(previousLastKey)
                    + " >= file " + fileIdx + " first key " + Arrays.toString(firstKey));
          }

          // Seek to last key
          iter.seekToLast();
          Assert.assertTrue(iter.isValid(), "SST file " + fileIdx + " should have a last entry");
          previousLastKey = iter.key();
        }
      }
    }
  }

  /**
   * Verifies that temp SST files are deleted after each split upload, bounding executor disk usage.
   * After processRows completes, only the uploaded files should exist (in blob dir), not any temp files.
   */
  @Test
  public void testProcessRows_tempFilesDeletedAfterSplit() throws Exception {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    String blobBaseUri = tempBlobDir.toAbsolutePath().toString();
    Properties props = createTestProperties(blobBaseUri);
    props.setProperty(BLOB_SST_FILE_SIZE_THRESHOLD_BYTES, "1");

    int numRows = BlobPartitionWriter.FILE_SIZE_CHECK_INTERVAL * 2 + 1;
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < numRows; i++) {
      byte[] key = ByteBuffer.allocate(4).putInt(i).array();
      byte[] value = new byte[] { (byte) (i & 0xFF) };
      rows.add(createRow(key, value));
    }

    // Use a String (serializable) instead of Path for the Spark closure
    String systemTempDirStr = System.getProperty("java.io.tmpdir");

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    jsc.parallelize(Collections.singletonList(1), 1).foreachPartition(partition -> {
      // Snapshot temp SST files before
      List<String> sstFilesBefore = listTempSstFiles(systemTempDirStr);

      LocalFsBlobStorageClient client = new LocalFsBlobStorageClient();
      try (BlobPartitionWriter writer = new BlobPartitionWriter(props, accumulators, client)) {
        writer.processRows(rows.iterator());
        Assert.assertTrue(writer.getSstFileCount() >= 2, "Should split into multiple files");
      } finally {
        client.close();
      }

      // After processRows + close, no new temp SST files should remain
      List<String> sstFilesAfter = listTempSstFiles(systemTempDirStr);
      List<String> newTempFiles = new ArrayList<>(sstFilesAfter);
      newTempFiles.removeAll(sstFilesBefore);
      Assert.assertTrue(
          newTempFiles.isEmpty(),
          "Temp SST files should be cleaned up after processing, but found: " + newTempFiles);
    });
  }

  /**
   * Verifies that all records are present across split SST files — no data loss during splitting.
   */
  @Test
  public void testProcessRows_allRecordsPresentAcrossSplitFiles() throws Exception {
    DataWriterAccumulators accumulators = new DataWriterAccumulators(spark);
    String blobBaseUri = tempBlobDir.toAbsolutePath().toString();
    Properties props = createTestProperties(blobBaseUri);
    props.setProperty(BLOB_SST_FILE_SIZE_THRESHOLD_BYTES, "1");

    int numRows = BlobPartitionWriter.FILE_SIZE_CHECK_INTERVAL * 2 + 500;
    List<Row> rows = new ArrayList<>();
    for (int i = 0; i < numRows; i++) {
      byte[] key = ByteBuffer.allocate(4).putInt(i).array();
      byte[] value = new byte[] { (byte) (i & 0xFF) };
      rows.add(createRow(key, value));
    }

    JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
    jsc.parallelize(Collections.singletonList(1), 1).foreachPartition(partition -> {
      LocalFsBlobStorageClient client = new LocalFsBlobStorageClient();
      try (BlobPartitionWriter writer = new BlobPartitionWriter(props, accumulators, client)) {
        writer.processRows(rows.iterator());
        Assert.assertTrue(writer.getSstFileCount() >= 2, "Expected multiple SST files");
      } finally {
        client.close();
      }
    });

    // Discover SST files from blob directory
    String partitionDir = BlobStoragePaths.partitionDir(blobBaseUri, "test-store", 1, 0);
    List<String> sstFiles = new ArrayList<>();
    for (int i = 0;; i++) {
      File f = new File(partitionDir + "/" + BlobPartitionWriter.composeSstFileName(i));
      if (!f.exists()) {
        break;
      }
      sstFiles.add(f.getAbsolutePath());
    }
    Assert.assertTrue(sstFiles.size() >= 2, "Expected multiple SST files on disk");

    // Count total records across all SST files
    int totalRecords = 0;
    for (String sstPath: sstFiles) {
      try (Options options = new Options(); SstFileReader reader = new SstFileReader(options)) {
        reader.open(sstPath);
        try (ReadOptions readOptions = new ReadOptions();
            SstFileReaderIterator iter = reader.newIterator(readOptions)) {
          iter.seekToFirst();
          while (iter.isValid()) {
            totalRecords++;
            iter.next();
          }
        }
      }
    }

    Assert.assertEquals(totalRecords, numRows, "Total records across all SST files should equal input row count");
  }

  /** Compare two byte arrays lexicographically (unsigned). */
  private static int compareBytes(byte[] a, byte[] b) {
    int minLen = Math.min(a.length, b.length);
    for (int i = 0; i < minLen; i++) {
      int cmp = Integer.compare(Byte.toUnsignedInt(a[i]), Byte.toUnsignedInt(b[i]));
      if (cmp != 0) {
        return cmp;
      }
    }
    return Integer.compare(a.length, b.length);
  }

  private static List<String> listTempSstFiles(String dirPath) {
    List<String> result = new ArrayList<>();
    File[] files = new File(dirPath).listFiles((d, name) -> name.startsWith("venice-blob-p") && name.endsWith(".sst"));
    if (files != null) {
      for (File f: files) {
        result.add(f.getAbsolutePath());
      }
    }
    return result;
  }

  private Properties createTestProperties(String blobBaseUri) {
    Properties props = new Properties();
    props.setProperty(VALUE_SCHEMA_ID_PROP, "1");
    props.setProperty(TOPIC_PROP, "test-store_v1");
    // BLOB_STORAGE_BASE_URI must be the version dir (baseUri/storeName/vN), matching production behavior.
    // BlobStoragePaths.versionDir() produces this same path, so assertion paths will match.
    props.setProperty(BLOB_STORAGE_BASE_URI, blobBaseUri + "/test-store/v1");
    return props;
  }

  private Row createRow(byte[] key, byte[] value) {
    return new GenericRowWithSchema(new Object[] { key, value, null }, SparkConstants.DEFAULT_SCHEMA);
  }
}
