package com.linkedin.venice.spark.datawriter.writer;

import static com.linkedin.venice.spark.SparkConstants.KEY_COLUMN_NAME;
import static com.linkedin.venice.spark.SparkConstants.VALUE_COLUMN_NAME;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_SST_TABLE_FORMAT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_STORAGE_BASE_URI;
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
  private static final String SST_FILE_NAME = "data_0.sst";
  private static final int SCHEMA_ID_PREFIX_SIZE = 4;

  private static final String PLAIN_TABLE = "PLAIN_TABLE";

  private final BlobStorageClient blobStorageClient;
  private final SparkDataWriterTaskTracker taskTracker;
  private final int valueSchemaId;
  private final String blobStorageBaseUri;
  private final String sstTableFormat;
  private final int partitionId;

  private SstFileWriter sstFileWriter;
  private File tempSstFile;
  private boolean sstFileHasData;

  public BlobPartitionWriter(
      Properties jobProperties,
      DataWriterAccumulators accumulators,
      BlobStorageClient blobStorageClient) {
    this.blobStorageClient = blobStorageClient;
    this.taskTracker = new SparkDataWriterTaskTracker(accumulators);

    this.valueSchemaId = Integer.parseInt(jobProperties.getProperty(VALUE_SCHEMA_ID_PROP));
    this.blobStorageBaseUri = jobProperties.getProperty(BLOB_STORAGE_BASE_URI);
    this.sstTableFormat = jobProperties.getProperty(BLOB_SST_TABLE_FORMAT, "BLOCK_BASED_TABLE");
    this.partitionId = TaskContext.get().partitionId();
  }

  /**
   * Process all rows for this partition: write them to an SST file and upload to blob storage.
   * Values are prefixed with a 4-byte schema ID in Venice's RocksDB format: [schema_id][payload].
   */
  public void processRows(Iterator<Row> rows) throws IOException {
    try {
      createSstFileWriter();

      while (rows.hasNext()) {
        Row row = rows.next();
        byte[] key = Objects.requireNonNull(row.getAs(KEY_COLUMN_NAME), "Key cannot be null");
        byte[] value = row.getAs(VALUE_COLUMN_NAME);

        if (value == null) {
          taskTracker.trackEmptyRecord();
          continue;
        }

        // Prepend 4-byte schema ID to value: [schema_id][payload]
        byte[] prefixedValue = prependSchemaId(value, valueSchemaId);

        sstFileWriter.put(key, prefixedValue);
        sstFileHasData = true;

        taskTracker.trackKeySize(key.length);
        taskTracker.trackUncompressedValueSize(prefixedValue.length);
        taskTracker.trackRecordSentToPubSub();
      }

      // Finish and upload if we wrote any data
      if (sstFileHasData) {
        sstFileWriter.finish();
        uploadSstFile();
      } else {
        LOGGER.info("Partition {} has no data, skipping SST file upload", partitionId);
      }
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to write SST file for partition " + partitionId, e);
    }
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

  private void uploadSstFile() throws IOException {
    // blobStorageBaseUri is already the version dir (e.g., baseUri/storeName/v1), so just append /p{partition}/{file}
    String remotePath = blobStorageBaseUri + "/p" + partitionId + "/" + SST_FILE_NAME;
    LOGGER.info("Uploading SST file for partition {} to {}", partitionId, remotePath);
    blobStorageClient.upload(tempSstFile.getAbsolutePath(), remotePath);
    LOGGER.info("Successfully uploaded SST file for partition {}", partitionId);
  }

  @Override
  public void close() throws IOException {
    if (sstFileWriter != null) {
      sstFileWriter.close();
    }
    if (tempSstFile != null && tempSstFile.exists()) {
      if (!tempSstFile.delete()) {
        LOGGER.warn("Failed to delete temp SST file: {}", tempSstFile.getAbsolutePath());
      }
    }
    taskTracker.trackPartitionWriterClose();
  }
}
