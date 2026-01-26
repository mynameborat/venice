package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.blobtransfer.HDFSBlobPullService;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageMetadataService;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.blobtransfer.BlobFileMetadata;
import com.linkedin.venice.blobtransfer.BlobTransferManifest;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.offsets.OffsetRecord;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * BlobIngestionTask handles the ingestion of SST files for blob-based Venice push.
 * Instead of consuming data from Kafka, this task:
 * 1. Pulls SST files from HDFS
 * 2. Ingests them directly into RocksDB via ingestExternalFile()
 * 3. Updates the offset record
 *
 * This is a lightweight alternative to StoreIngestionTask for PushType.BLOB versions.
 */
public class BlobIngestionTask implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(BlobIngestionTask.class);

  private final String storeName;
  private final int version;
  private final int partition;
  private final String blobStagingPath;
  private final String localBaseDir;
  private final VeniceStoreVersionConfig storeConfig;
  private final AbstractStorageEngine storageEngine;
  private final StorageMetadataService storageMetadataService;
  private final VeniceNotifier notifier;

  private HDFSBlobPullService blobPullService;
  private boolean isCompleted = false;

  public BlobIngestionTask(
      String storeName,
      int version,
      int partition,
      String blobStagingPath,
      String localBaseDir,
      VeniceStoreVersionConfig storeConfig,
      AbstractStorageEngine storageEngine,
      StorageMetadataService storageMetadataService,
      VeniceNotifier notifier) {
    this.storeName = storeName;
    this.version = version;
    this.partition = partition;
    this.blobStagingPath = blobStagingPath;
    this.localBaseDir = localBaseDir;
    this.storeConfig = storeConfig;
    this.storageEngine = storageEngine;
    this.storageMetadataService = storageMetadataService;
    this.notifier = notifier;
    this.blobPullService = new HDFSBlobPullService();
  }

  /**
   * Execute the blob ingestion for this partition.
   * This method:
   * 1. Pulls SST files from HDFS to local directory
   * 2. Ingests them into RocksDB
   * 3. Updates the offset record
   * 4. Notifies completion
   */
  public void execute() {
    String kafkaTopic = Version.composeKafkaTopic(storeName, version);
    String hdfsPartitionPath = blobStagingPath + "/partition_" + partition;
    String localPartitionDir = localBaseDir + "/" + kafkaTopic + "/" + partition + "/blob_ingestion";

    LOGGER.info(
        "Starting blob ingestion for store: {}, version: {}, partition: {}, HDFS path: {}",
        storeName,
        version,
        partition,
        hdfsPartitionPath);

    try {
      // Check if blobs are available
      if (!blobPullService.isBlobAvailable(hdfsPartitionPath)) {
        throw new VeniceException(
            "Blob files not available at HDFS path: " + hdfsPartitionPath + " for partition " + partition);
      }

      // Pull SST files from HDFS
      BlobTransferManifest manifest = blobPullService.pullFromHDFS(hdfsPartitionPath, localPartitionDir);

      // Ingest SST files into RocksDB
      ingestSstFiles(manifest, localPartitionDir);

      // Update offset record to mark completion
      updateOffsetRecord(manifest);

      // Notify completion
      if (notifier != null) {
        notifier.completed(kafkaTopic, partition, 0, "Blob ingestion completed");
      }

      isCompleted = true;

      LOGGER.info(
          "Successfully completed blob ingestion for store: {}, version: {}, partition: {}, " + "records: {}",
          storeName,
          version,
          partition,
          manifest.getRecordCount());

    } catch (Exception e) {
      LOGGER.error("Blob ingestion failed for store: {}, version: {}, partition: {}", storeName, version, partition, e);

      // Notify error
      if (notifier != null) {
        notifier.error(kafkaTopic, partition, "Blob ingestion failed: " + e.getMessage(), e);
      }

      throw new VeniceException("Blob ingestion failed for partition " + partition, e);
    } finally {
      // Clean up local temp files
      cleanupLocalFiles(localPartitionDir);
    }
  }

  /**
   * Ingest SST files into RocksDB.
   * The actual ingestion is delegated to the storage engine's existing
   * blob ingestion mechanism which handles RocksDB-specific details.
   */
  private void ingestSstFiles(BlobTransferManifest manifest, String localDir) {
    List<BlobFileMetadata> sstFiles = manifest.getSstFiles();
    if (sstFiles.isEmpty()) {
      LOGGER.warn("No SST files to ingest for partition {}", partition);
      return;
    }

    // Get list of SST file paths for default column family
    List<String> defaultCfSstFilePaths = new java.util.ArrayList<>();
    List<String> rmdCfSstFilePaths = new java.util.ArrayList<>();

    for (BlobFileMetadata fileMetadata: sstFiles) {
      String localFilePath = localDir + "/" + fileMetadata.getFileName();
      File sstFile = new File(localFilePath);
      if (!sstFile.exists()) {
        LOGGER.warn("SST file not found: {}", localFilePath);
        continue;
      }

      if (fileMetadata.getColumnFamily() == null
          || fileMetadata.getColumnFamily().equals(BlobTransferManifest.DEFAULT_COLUMN_FAMILY)) {
        defaultCfSstFilePaths.add(localFilePath);
      } else if (fileMetadata.getColumnFamily().equals(BlobTransferManifest.REPLICATION_METADATA_COLUMN_FAMILY)) {
        rmdCfSstFilePaths.add(localFilePath);
      }
    }

    LOGGER.info(
        "Ingesting {} default CF and {} RMD CF SST files into RocksDB for partition {}",
        defaultCfSstFilePaths.size(),
        rmdCfSstFilePaths.size(),
        partition);

    // Ingest SST files using the storage engine's built-in mechanism
    // The storage engine will handle RocksDB-specific ingestion logic
    if (!defaultCfSstFilePaths.isEmpty()) {
      storageEngine.ingestExternalFiles(partition, defaultCfSstFilePaths, false);
    }
    if (!rmdCfSstFilePaths.isEmpty()) {
      storageEngine.ingestExternalFiles(partition, rmdCfSstFilePaths, true);
    }

    LOGGER.info("Successfully ingested SST files into RocksDB for partition {}", partition);
  }

  /**
   * Update the offset record to mark the partition as completed.
   */
  private void updateOffsetRecord(BlobTransferManifest manifest) {
    String kafkaTopic = Version.composeKafkaTopic(storeName, version);

    // Create an offset record that indicates blob ingestion is complete
    OffsetRecord offsetRecord = new OffsetRecord(storageMetadataService.getLocalPartitionIdForKafkaTopic(kafkaTopic));
    // Set a special marker to indicate blob-based ingestion
    offsetRecord.setDatabaseInfo(
        com.google.common.collect.ImmutableMap
            .of("blob_ingestion", "true", "record_count", String.valueOf(manifest.getRecordCount())));

    storageMetadataService.put(kafkaTopic, partition, offsetRecord);

    LOGGER.info("Updated offset record for blob ingestion - topic: {}, partition: {}", kafkaTopic, partition);
  }

  /**
   * Clean up local temporary files after ingestion.
   */
  private void cleanupLocalFiles(String localDir) {
    try {
      File dir = new File(localDir);
      if (dir.exists()) {
        File[] files = dir.listFiles();
        if (files != null) {
          for (File file: files) {
            file.delete();
          }
        }
        dir.delete();
        LOGGER.debug("Cleaned up local blob ingestion directory: {}", localDir);
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to clean up local blob ingestion directory: {}", localDir, e);
    }
  }

  public boolean isCompleted() {
    return isCompleted;
  }

  // Visible for testing
  void setBlobPullService(HDFSBlobPullService blobPullService) {
    this.blobPullService = blobPullService;
  }

  @Override
  public void close() throws IOException {
    if (blobPullService != null) {
      blobPullService.close();
    }
  }
}
