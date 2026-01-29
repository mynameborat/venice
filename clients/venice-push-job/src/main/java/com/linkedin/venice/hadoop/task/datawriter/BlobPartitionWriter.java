package com.linkedin.venice.hadoop.task.datawriter;

import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_PUSH_LOCAL_TEMP_DIR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_PUSH_MAX_RECORDS_PER_SST_FILE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_PUSH_MAX_SST_FILE_SIZE_BYTES;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_PUSH_STAGING_PATH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.TOPIC_PROP;
import static com.linkedin.venice.vpj.VenicePushJobConstants.VALUE_SCHEMA_ID_PROP;

import com.linkedin.davinci.store.rocksdb.VPJSstFileWriter;
import com.linkedin.venice.annotation.NotThreadsafe;
import com.linkedin.venice.blobtransfer.BlobTransferManifest;
import com.linkedin.venice.blobtransfer.client.BlobStorageClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.hadoop.output.HDFSBlobStorageClient;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.utils.VeniceProperties;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Iterator;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * BlobPartitionWriter writes key-value pairs directly to RocksDB SST files
 * instead of Kafka. This is used for blob-based push jobs where data is
 * transferred via blob transfer mechanism instead of Kafka.
 *
 * Data flow:
 * 1. MR reducer calls processValuesForKey() with sorted key-value pairs
 * 2. BlobPartitionWriter writes to local SST files via VPJSstFileWriter
 * 3. On close(), uploads SST files and manifest to HDFS staging directory
 *
 * Configuration:
 * - BLOB_PUSH_STAGING_PATH: HDFS path for staging SST files
 * - BLOB_PUSH_LOCAL_TEMP_DIR: Local temp directory for SST file generation
 * - BLOB_PUSH_MAX_SST_FILE_SIZE_BYTES: Max size per SST file before rotation
 * - BLOB_PUSH_MAX_RECORDS_PER_SST_FILE: Max records per SST file
 */
@NotThreadsafe
public class BlobPartitionWriter extends AbstractPartitionWriter {
  private static final Logger LOGGER = LogManager.getLogger(BlobPartitionWriter.class);

  private static final String TABLE_FORMAT = "BLOCK_BASED_TABLE";
  private static final long DEFAULT_MAX_SST_FILE_SIZE = 256 * 1024 * 1024L; // 256MB
  private static final long DEFAULT_MAX_RECORDS_PER_SST_FILE = 10_000_000L; // 10M records

  private VPJSstFileWriter sstFileWriter;
  private BlobStorageClient blobStorageClient;

  private String localTempDir;
  private String hdfsStagingPath;
  private String storeName;
  private int version;
  private int partition;
  private int valueSchemaId;

  private long maxSstFileSizeBytes;
  private long maxRecordsPerSstFile;

  private long recordsProcessed = 0;
  private long totalKeySize = 0;
  private long totalValueSize = 0;
  private boolean isConfigured = false;
  private boolean isClosed = false;

  @Override
  protected void configureTask(VeniceProperties props) {
    super.configureTask(props);

    String topicName = props.getString(TOPIC_PROP);
    this.storeName = Version.parseStoreFromKafkaTopicName(topicName);
    this.version = Version.parseVersionFromKafkaTopicName(topicName);
    this.partition = getTaskId();
    this.valueSchemaId = props.getInt(VALUE_SCHEMA_ID_PROP);

    this.hdfsStagingPath = props.getString(BLOB_PUSH_STAGING_PATH);
    this.localTempDir = props.getString(
        BLOB_PUSH_LOCAL_TEMP_DIR,
        System.getProperty("java.io.tmpdir") + "/venice-blob-push-" + storeName + "-v" + version + "-p" + partition);

    this.maxSstFileSizeBytes = props.getLong(BLOB_PUSH_MAX_SST_FILE_SIZE_BYTES, DEFAULT_MAX_SST_FILE_SIZE);
    this.maxRecordsPerSstFile = props.getLong(BLOB_PUSH_MAX_RECORDS_PER_SST_FILE, DEFAULT_MAX_RECORDS_PER_SST_FILE);

    // Initialize SST file writer
    initializeSstFileWriter();

    // Initialize blob storage client
    this.blobStorageClient = new HDFSBlobStorageClient();

    this.isConfigured = true;

    LOGGER.info(
        "Configured BlobPartitionWriter for store: {}, version: {}, partition: {}, "
            + "HDFS staging: {}, local temp: {}, max SST size: {}, max records per SST: {}",
        storeName,
        version,
        partition,
        hdfsStagingPath,
        localTempDir,
        maxSstFileSizeBytes,
        maxRecordsPerSstFile);
  }

  private void initializeSstFileWriter() {
    // Clean up any existing temp directory
    File tempDir = new File(localTempDir);
    if (tempDir.exists()) {
      try {
        FileUtils.deleteDirectory(tempDir);
      } catch (IOException e) {
        LOGGER.warn("Failed to clean up existing temp directory: {}", localTempDir, e);
      }
    }

    this.sstFileWriter = new VPJSstFileWriter(
        localTempDir,
        storeName,
        version,
        partition,
        BlobTransferManifest.DEFAULT_COLUMN_FAMILY,
        maxSstFileSizeBytes,
        maxRecordsPerSstFile);

    sstFileWriter.open();
  }

  @Override
  protected VeniceWriterMessage extract(
      byte[] keyBytes,
      Iterator<VeniceRecordWithMetadata> values,
      DataWriterTaskTracker dataWriterTaskTracker) {
    if (!values.hasNext()) {
      throw new VeniceException("No value for key in blob push");
    }

    VeniceRecordWithMetadata valueRecord = values.next();
    byte[] valueBytes = valueRecord.getValue();

    // Track sizes for quota checking
    totalKeySize += keyBytes.length;
    if (valueBytes != null) {
      totalValueSize += valueBytes.length;
    }

    // Write directly to SST file
    writeToBlobStorage(keyBytes, valueBytes, dataWriterTaskTracker);

    recordsProcessed++;

    // Log progress periodically
    if (recordsProcessed % 100000 == 0) {
      LOGGER.info(
          "BlobPartitionWriter progress - store: {}, partition: {}, records: {}, "
              + "total key size: {}, total value size: {}",
          storeName,
          partition,
          recordsProcessed,
          totalKeySize,
          totalValueSize);
    }

    // Return null since we're not using Kafka writer
    // The parent class handles null returns
    return null;
  }

  private void writeToBlobStorage(byte[] keyBytes, byte[] valueBytes, DataWriterTaskTracker dataWriterTaskTracker) {
    if (sstFileWriter == null) {
      throw new VeniceException("SST file writer is not initialized");
    }

    try {
      // Write to SST file
      sstFileWriter.put(keyBytes, valueBytes != null ? valueBytes : new byte[0]);

      // Track record sent (for compatibility with tracker)
      if (dataWriterTaskTracker != null) {
        dataWriterTaskTracker.trackRecordSentToPubSub();
      }
    } catch (Exception e) {
      throw new VeniceException("Failed to write record to SST file", e);
    }
  }

  @Override
  public void close() throws IOException {
    if (isClosed) {
      return;
    }

    LOGGER.info(
        "Closing BlobPartitionWriter for store: {}, version: {}, partition: {}, records processed: {}",
        storeName,
        version,
        partition,
        recordsProcessed);

    try {
      // Close SST file writer and finalize files
      if (sstFileWriter != null) {
        sstFileWriter.close();
      }

      // Upload SST files and manifest to HDFS
      uploadToHdfs();

      // Clean up local temp directory
      cleanupLocalTempDir();

      LOGGER.info(
          "Successfully closed BlobPartitionWriter for store: {}, version: {}, partition: {}, "
              + "total records: {}, SST files: {}",
          storeName,
          version,
          partition,
          recordsProcessed,
          sstFileWriter != null ? sstFileWriter.getSstFileCount() : 0);

    } finally {
      if (blobStorageClient != null) {
        try {
          blobStorageClient.close();
        } catch (Exception e) {
          LOGGER.warn("Failed to close blob storage client", e);
        }
      }
      isClosed = true;
    }

    // Update task tracker
    DataWriterTaskTracker tracker = getDataWriterTaskTracker();
    if (tracker != null) {
      tracker.trackPartitionWriterClose();
    }
  }

  private void uploadToHdfs() throws IOException {
    if (sstFileWriter == null || sstFileWriter.getTotalRecordCount() == 0) {
      LOGGER.info("No records written for partition {}, skipping HDFS upload", partition);
      return;
    }

    String hdfsPartitionDir = blobStorageClient.composePartitionPath(hdfsStagingPath, storeName, version, partition);

    // Create partition directory in HDFS
    blobStorageClient.createDirectory(hdfsPartitionDir);

    // Upload all SST files
    File localDir = new File(localTempDir);
    File[] sstFiles = localDir.listFiles((dir, name) -> name.endsWith(".sst"));

    if (sstFiles != null) {
      for (File sstFile: sstFiles) {
        String hdfsFilePath = hdfsPartitionDir + "/" + sstFile.getName();
        try (FileInputStream fis = new FileInputStream(sstFile)) {
          blobStorageClient.writeFile(hdfsFilePath, fis, sstFile.length());
          LOGGER.info("Uploaded SST file: {} ({} bytes)", hdfsFilePath, sstFile.length());
        }
      }
    }

    // Create and upload manifest
    BlobTransferManifest manifest = sstFileWriter.createManifest(TABLE_FORMAT);
    blobStorageClient.writeManifest(hdfsPartitionDir, manifest);
    LOGGER.info(
        "Uploaded manifest for partition {} with {} SST files, {} records",
        partition,
        manifest.getSstFiles().size(),
        manifest.getRecordCount());
  }

  private void cleanupLocalTempDir() {
    File tempDir = new File(localTempDir);
    if (tempDir.exists()) {
      try {
        FileUtils.deleteDirectory(tempDir);
        LOGGER.debug("Cleaned up local temp directory: {}", localTempDir);
      } catch (IOException e) {
        LOGGER.warn("Failed to clean up local temp directory: {}", localTempDir, e);
      }
    }
  }

  /**
   * Get the number of records processed.
   */
  public long getRecordsProcessed() {
    return recordsProcessed;
  }

  /**
   * Get the total key size in bytes.
   */
  public long getTotalKeySize() {
    return totalKeySize;
  }

  /**
   * Get the total value size in bytes.
   */
  public long getTotalValueSize() {
    return totalValueSize;
  }

  // Visible for testing
  protected void setBlobStorageClient(BlobStorageClient blobStorageClient) {
    this.blobStorageClient = blobStorageClient;
  }

  // Visible for testing
  protected void setSstFileWriter(VPJSstFileWriter sstFileWriter) {
    this.sstFileWriter = sstFileWriter;
  }
}
