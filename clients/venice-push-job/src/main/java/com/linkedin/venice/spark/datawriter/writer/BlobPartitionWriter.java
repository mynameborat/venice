package com.linkedin.venice.spark.datawriter.writer;

import static com.linkedin.venice.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.VALUE_COLUMN_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_SST_FILE_SIZE_THRESHOLD_BYTES;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_SST_TABLE_FORMAT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_STORAGE_BASE_URI;
import static com.linkedin.venice.vpj.VenicePushJobConstants.DEFAULT_BLOB_SST_FILE_SIZE_THRESHOLD_BYTES;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_ID_PROP;

import com.linkedin.venice.blobtransfer.storage.BlobStorageClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import com.linkedin.venice.spark.datawriter.task.SparkDataWriterTaskTracker;
import com.linkedin.venice.utils.ByteUtils;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Objects;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.Row;
import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;


/**
 * Standalone partition writer for blob-based push that generates RocksDB SST files
 * and uploads them to blob storage. This class does NOT extend AbstractPartitionWriter
 * because that class is deeply coupled to VeniceWriter/Kafka.
 *
 * <p>The flow is:
 * <ol>
 *   <li>Create a local temp SST file</li>
 *   <li>Write sorted key/value pairs using {@link SstFileWriter}</li>
 *   <li>Upload the finished SST file to blob storage via {@link BlobStorageClient}</li>
 *   <li>Track metrics via {@link SparkDataWriterTaskTracker}</li>
 * </ol>
 *
 * <p>Keys are guaranteed to arrive sorted because {@code AbstractDataWriterSparkJob.runComputeJob()}
 * calls {@code SparkPartitionUtils.repartitionAndSortWithinPartitions()} before the partition writer stage.
 */
public class BlobPartitionWriter implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(BlobPartitionWriter.class);
  private static final int SCHEMA_ID_PREFIX_SIZE = 4;
  private static final String PLAIN_TABLE = "PLAIN_TABLE";

  /** Check SST file size every N records to amortize the JNI overhead of fileSize(). */
  static final int FILE_SIZE_CHECK_INTERVAL = 1000;

  private final BlobStorageClient blobStorageClient;
  private final SparkDataWriterTaskTracker taskTracker;
  private final int valueSchemaId;
  private final String blobStorageBaseUri;
  private final String sstTableFormat;
  private final int partitionId;
  private final long sstFileSizeThresholdBytes;

  private SstFileWriter sstFileWriter;
  private File tempSstFile;
  private boolean sstFileHasData;
  private int currentSstFileIndex;
  private int sstFileCount;

  /** Reusable buffer for prepending schema ID to values, avoiding per-record allocation. */
  private byte[] reusableValueBuffer = new byte[4096];

  public BlobPartitionWriter(
      Properties jobProperties,
      DataWriterAccumulators accumulators,
      BlobStorageClient blobStorageClient) {
    this.blobStorageClient = blobStorageClient;
    this.taskTracker = new SparkDataWriterTaskTracker(accumulators);

    this.valueSchemaId = Integer.parseInt(jobProperties.getProperty(VALUE_SCHEMA_ID_PROP));
    this.blobStorageBaseUri = jobProperties.getProperty(BLOB_STORAGE_BASE_URI);
    this.sstTableFormat = jobProperties.getProperty(BLOB_SST_TABLE_FORMAT, "BLOCK_BASED_TABLE");
    this.sstFileSizeThresholdBytes = Long.parseLong(
        jobProperties.getProperty(
            BLOB_SST_FILE_SIZE_THRESHOLD_BYTES,
            String.valueOf(DEFAULT_BLOB_SST_FILE_SIZE_THRESHOLD_BYTES)));
    this.partitionId = TaskContext.get().partitionId();
  }

  /**
   * Process all rows for this partition: write them to SST file(s) and upload to blob storage.
   * Values are prefixed with a 4-byte schema ID in Venice's RocksDB format: [schema_id][payload].
   *
   * <p>SST splitting preserves key ordering because rows arrive pre-sorted by
   * {@code repartitionAndSortWithinPartitions()} and files are split sequentially.
   * File N's last key &lt; file N+1's first key.
   *
   * <p>When the current SST file exceeds {@code sstFileSizeThresholdBytes} (checked every
   * {@value #FILE_SIZE_CHECK_INTERVAL} records via {@code SstFileWriter.fileSize()}), the file
   * is finished, uploaded, and deleted before a new SST file is started. This bounds executor
   * disk usage and eliminates all-or-nothing upload failure for large partitions.
   */
  public void processRows(Iterator<Row> rows) throws IOException {
    try {
      currentSstFileIndex = 0;
      sstFileCount = 0;
      createSstFileWriter();

      int recordsSinceLastSizeCheck = 0;

      while (rows.hasNext()) {
        Row row = rows.next();
        byte[] key = Objects.requireNonNull(row.getAs(KEY_COLUMN_NAME), "Key cannot be null");
        byte[] value = row.getAs(VALUE_COLUMN_NAME);

        if (value == null) {
          taskTracker.trackEmptyRecord();
          continue;
        }

        // Prepend 4-byte schema ID to value using reusable buffer to avoid per-record allocation
        int prefixedValueLength = SCHEMA_ID_PREFIX_SIZE + value.length;
        reusableValueBuffer = prependSchemaIdReusable(value, valueSchemaId, reusableValueBuffer);

        // SstFileWriter.put(byte[], byte[]) uses the full array length, so we must
        // pass an exact-sized array. When value sizes are uniform (common case), the
        // buffer stabilizes and this branch is a no-op after the first few records.
        byte[] putValue = (reusableValueBuffer.length == prefixedValueLength)
            ? reusableValueBuffer
            : Arrays.copyOf(reusableValueBuffer, prefixedValueLength);
        sstFileWriter.put(key, putValue);
        sstFileHasData = true;

        taskTracker.trackKeySize(key.length);
        taskTracker.trackUncompressedValueSize(prefixedValueLength);
        taskTracker.trackRecordSentToPubSub();

        // Periodically check actual on-disk SST size to decide whether to split
        recordsSinceLastSizeCheck++;
        if (recordsSinceLastSizeCheck >= FILE_SIZE_CHECK_INTERVAL) {
          recordsSinceLastSizeCheck = 0;
          if (sstFileWriter.fileSize() >= sstFileSizeThresholdBytes) {
            finishAndUploadCurrentSstFile();
            currentSstFileIndex++;
            createSstFileWriter();
          }
        }
      }

      // Finish and upload the last file if it has data
      if (sstFileHasData) {
        finishAndUploadCurrentSstFile();
      } else if (sstFileCount == 0) {
        LOGGER.info("Partition {} has no data, skipping SST file upload", partitionId);
      }

      LOGGER.info("Partition {} produced {} SST file(s)", partitionId, sstFileCount);
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to write SST file for partition " + partitionId, e);
    }
  }

  /**
   * Finish the current SST file, upload it to blob storage, and delete the local temp file.
   */
  private void finishAndUploadCurrentSstFile() throws IOException, RocksDBException {
    sstFileWriter.finish();
    uploadSstFile(currentSstFileIndex);
    sstFileCount++;
    deleteTempSstFile();
  }

  /**
   * Prepend a 4-byte big-endian schema ID to the value bytes.
   * This matches Venice's RocksDB value format: [4-byte schema ID][payload].
   */
  static byte[] prependSchemaId(byte[] value, int schemaId) {
    byte[] result = new byte[SCHEMA_ID_PREFIX_SIZE + value.length];
    ByteUtils.writeInt(result, schemaId, 0);
    System.arraycopy(value, 0, result, SCHEMA_ID_PREFIX_SIZE, value.length);
    return result;
  }

  /**
   * Prepend a 4-byte schema ID to value using a reusable buffer to avoid per-record allocation.
   * The buffer grows monotonically — once it reaches the max value size for the store, no further
   * allocations occur. Returns the (possibly grown) buffer.
   */
  static byte[] prependSchemaIdReusable(byte[] value, int schemaId, byte[] buffer) {
    int requiredSize = SCHEMA_ID_PREFIX_SIZE + value.length;
    if (buffer.length < requiredSize) {
      buffer = new byte[Math.max(requiredSize, buffer.length * 2)];
    }
    ByteUtils.writeInt(buffer, schemaId, 0);
    System.arraycopy(value, 0, buffer, SCHEMA_ID_PREFIX_SIZE, value.length);
    return buffer;
  }

  private void createSstFileWriter() throws IOException, RocksDBException {
    tempSstFile = Files.createTempFile("venice-blob-p" + partitionId + "-", ".sst").toFile();
    tempSstFile.deleteOnExit();

    Options options = new Options();
    if (PLAIN_TABLE.equals(sstTableFormat)) {
      options.setTableFormatConfig(new PlainTableConfig());
    } else {
      options.setTableFormatConfig(new BlockBasedTableConfig());
    }
    EnvOptions envOptions = new EnvOptions();
    sstFileWriter = new SstFileWriter(envOptions, options);
    sstFileWriter.open(tempSstFile.getAbsolutePath());
    sstFileHasData = false;

    LOGGER.info(
        "Created SST file writer for partition {} at {} with table format {}",
        partitionId,
        tempSstFile.getAbsolutePath(),
        sstTableFormat);
  }

  private void uploadSstFile(int fileIndex) throws IOException {
    String fileName = composeSstFileName(fileIndex);
    // blobStorageBaseUri is already the version dir (e.g., baseUri/storeName/v1), so just append /p{partition}/{file}
    String remotePath = blobStorageBaseUri + "/p" + partitionId + "/" + fileName;
    LOGGER.info("Uploading SST file {} for partition {} to {}", fileName, partitionId, remotePath);
    blobStorageClient.upload(tempSstFile.getAbsolutePath(), remotePath);
    LOGGER.info("Successfully uploaded SST file {} for partition {}", fileName, partitionId);
  }

  /**
   * Compose the SST file name for a given file index.
   * Files are named data_0.sst, data_1.sst, etc.
   */
  static String composeSstFileName(int index) {
    return "data_" + index + ".sst";
  }

  private void deleteTempSstFile() {
    if (tempSstFile != null && tempSstFile.exists()) {
      if (!tempSstFile.delete()) {
        LOGGER.warn("Failed to delete temp SST file: {}", tempSstFile.getAbsolutePath());
      }
      tempSstFile = null;
    }
  }

  /** Returns the total number of SST files produced by {@link #processRows}. */
  int getSstFileCount() {
    return sstFileCount;
  }

  @Override
  public void close() throws IOException {
    if (sstFileWriter != null) {
      sstFileWriter.close();
    }
    deleteTempSstFile();
    taskTracker.trackPartitionWriterClose();
  }
}
