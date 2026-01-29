package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.blobtransfer.BlobPullService;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.blobtransfer.BlobFileMetadata;
import com.linkedin.venice.blobtransfer.BlobTransferManifest;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.meta.Version;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * BlobIngestionTask handles the ingestion of SST files for blob-based Venice push.
 * Instead of consuming data from Kafka, this task:
 * 1. Pulls SST files from blob storage (HDFS, S3, etc.)
 * 2. Ingests them directly into RocksDB via ingestExternalFile()
 * 3. Notifies completion
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
  private final VeniceNotifier notifier;

  private BlobPullService blobPullService;
  private boolean isCompleted = false;

  public BlobIngestionTask(
      String storeName,
      int version,
      int partition,
      String blobStagingPath,
      String localBaseDir,
      VeniceStoreVersionConfig storeConfig,
      AbstractStorageEngine storageEngine,
      VeniceNotifier notifier) {
    this.storeName = storeName;
    this.version = version;
    this.partition = partition;
    this.blobStagingPath = blobStagingPath;
    this.localBaseDir = localBaseDir;
    this.storeConfig = storeConfig;
    this.storageEngine = storageEngine;
    this.notifier = notifier;
  }

  /**
   * Execute the blob ingestion for this partition.
   * This method:
   * 1. Pulls SST files from blob storage to local directory
   * 2. Ingests them into RocksDB
   * 3. Notifies completion
   */
  public void execute() {
    if (blobPullService == null) {
      throw new VeniceException("BlobPullService must be set before executing blob ingestion");
    }

    String kafkaTopic = Version.composeKafkaTopic(storeName, version);
    String remotePartitionPath = blobStagingPath + "/partition_" + partition;
    String localPartitionDir = localBaseDir + "/" + kafkaTopic + "/" + partition + "/blob_ingestion";

    LOGGER.info(
        "Starting blob ingestion for store: {}, version: {}, partition: {}, remote path: {}",
        storeName,
        version,
        partition,
        remotePartitionPath);

    try {
      // Check if blobs are available
      if (!blobPullService.isBlobAvailable(remotePartitionPath)) {
        throw new VeniceException(
            "Blob files not available at path: " + remotePartitionPath + " for partition " + partition);
      }

      // Pull SST files from blob storage
      BlobTransferManifest manifest = blobPullService.pullBlobs(remotePartitionPath, localPartitionDir);

      // Ingest SST files into RocksDB
      ingestSstFiles(manifest, localPartitionDir);

      // Notify completion (pass null for PubSubPosition since blob ingestion doesn't have Kafka offsets)
      if (notifier != null) {
        notifier.completed(kafkaTopic, partition, null, "Blob ingestion completed");
      }

      isCompleted = true;

      LOGGER.info(
          "Successfully completed blob ingestion for store: {}, version: {}, partition: {}, records: {}",
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
    List<String> defaultCfSstFilePaths = new ArrayList<>();
    List<String> rmdCfSstFilePaths = new ArrayList<>();

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
   * Clean up local temporary files after ingestion.
   */
  private void cleanupLocalFiles(String localDir) {
    try {
      File dir = new File(localDir);
      if (dir.exists()) {
        File[] files = dir.listFiles();
        if (files != null) {
          for (File file: files) {
            if (!file.delete()) {
              LOGGER.warn("Failed to delete file: {}", file.getAbsolutePath());
            }
          }
        }
        if (!dir.delete()) {
          LOGGER.warn("Failed to delete directory: {}", dir.getAbsolutePath());
        }
        LOGGER.debug("Cleaned up local blob ingestion directory: {}", localDir);
      }
    } catch (Exception e) {
      LOGGER.warn("Failed to clean up local blob ingestion directory: {}", localDir, e);
    }
  }

  public boolean isCompleted() {
    return isCompleted;
  }

  public String getStoreName() {
    return storeName;
  }

  public int getVersion() {
    return version;
  }

  public int getPartition() {
    return partition;
  }

  /**
   * Set the blob pull service implementation.
   * This must be called before execute().
   */
  public void setBlobPullService(BlobPullService blobPullService) {
    this.blobPullService = blobPullService;
  }

  @Override
  public void close() throws IOException {
    if (blobPullService != null) {
      blobPullService.close();
    }
  }
}
