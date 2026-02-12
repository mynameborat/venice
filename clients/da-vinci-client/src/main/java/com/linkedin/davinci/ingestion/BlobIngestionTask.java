package com.linkedin.davinci.ingestion;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.AbstractStoragePartition;
import com.linkedin.davinci.store.rocksdb.RocksDBStoragePartition;
import com.linkedin.venice.blobtransfer.storage.BlobStorageClient;
import com.linkedin.venice.blobtransfer.storage.BlobStoragePaths;
import com.linkedin.venice.blobtransfer.storage.BlobStorageType;
import com.linkedin.venice.blobtransfer.storage.LocalFsBlobStorageClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.PushControlSignal;
import com.linkedin.venice.pushmonitor.PushControlSignalAccessor;
import com.linkedin.venice.pushmonitor.PushControlSignalType;
import com.linkedin.venice.store.rocksdb.RocksDBUtils;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Handles the full blob ingestion lifecycle for a single partition of a BATCH_BLOB version.
 *
 * Unlike P2P blob transfer (which is a bootstrap optimization that falls back to Kafka),
 * blob-based ingestion is the ONLY data path — no Kafka topic exists for BATCH_BLOB versions.
 *
 * Lifecycle:
 * 1. Report STARTED to notifiers
 * 2. Wait for BLOB_UPLOAD_COMPLETE signal in ZK
 * 3. Download SST files from blob storage
 * 4. Ingest SST files into RocksDB via ingestExternalFile()
 * 5. Report END_OF_PUSH_RECEIVED and COMPLETED to notifiers
 */
public class BlobIngestionTask implements Runnable {
  private static final Logger LOGGER = LogManager.getLogger(BlobIngestionTask.class);

  private final String storeName;
  private final int versionNumber;
  private final int partition;
  private final String kafkaTopic;
  private final String blobStorageUri;
  private final BlobStorageType blobStorageType;
  private final PushControlSignalAccessor pushControlSignalAccessor;
  private final StorageService storageService;
  private final VeniceStoreVersionConfig storeConfig;
  private final Supplier<StoreVersionState> svsSupplier;
  private final Queue<VeniceNotifier> notifiers;
  private final VeniceServerConfig serverConfig;
  private final int downloadMaxRetries;
  private final long pollIntervalMs;

  public BlobIngestionTask(
      String storeName,
      int versionNumber,
      int partition,
      String blobStorageUri,
      BlobStorageType blobStorageType,
      PushControlSignalAccessor pushControlSignalAccessor,
      StorageService storageService,
      VeniceStoreVersionConfig storeConfig,
      Supplier<StoreVersionState> svsSupplier,
      Queue<VeniceNotifier> notifiers,
      VeniceServerConfig serverConfig) {
    this.storeName = storeName;
    this.versionNumber = versionNumber;
    this.partition = partition;
    this.kafkaTopic = Version.composeKafkaTopic(storeName, versionNumber);
    this.blobStorageUri = blobStorageUri;
    this.blobStorageType = blobStorageType;
    this.pushControlSignalAccessor = pushControlSignalAccessor;
    this.storageService = storageService;
    this.storeConfig = storeConfig;
    this.svsSupplier = svsSupplier;
    this.notifiers = notifiers;
    this.serverConfig = serverConfig;
    this.downloadMaxRetries = serverConfig.getBlobIngestionDownloadMaxRetries();
    this.pollIntervalMs = serverConfig.getBlobIngestionPollIntervalMs();
  }

  @Override
  public void run() {
    try {
      LOGGER.info("Starting blob ingestion for {} partition {}", kafkaTopic, partition);

      // Step 1: Report STARTED
      reportStarted();

      // Step 2: Wait for blob upload complete signal
      waitForBlobUploadComplete();

      // Step 3: Download SST files
      List<String> sstFilePaths = downloadSSTFiles();

      // Step 4: Ingest SST files into RocksDB
      ingestSSTFiles(sstFilePaths);

      // Step 5: Report completion
      reportEndOfPushReceived();
      reportCompleted();

      LOGGER.info("Blob ingestion completed successfully for {} partition {}", kafkaTopic, partition);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.warn("Blob ingestion interrupted for {} partition {}", kafkaTopic, partition);
      reportError("Blob ingestion interrupted", e);
    } catch (Exception e) {
      LOGGER.error("Blob ingestion failed for {} partition {}", kafkaTopic, partition, e);
      reportError("Blob ingestion failed: " + e.getMessage(), e);
    }
  }

  /**
   * Poll ZK for the BLOB_UPLOAD_COMPLETE signal until it appears.
   */
  void waitForBlobUploadComplete() throws InterruptedException {
    LOGGER.info("Waiting for BLOB_UPLOAD_COMPLETE signal for {}", kafkaTopic);
    while (!Thread.currentThread().isInterrupted()) {
      PushControlSignal signal = pushControlSignalAccessor.getPushControlSignal(kafkaTopic);
      if (signal != null && signal.hasSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE)) {
        LOGGER.info(
            "Received BLOB_UPLOAD_COMPLETE signal for {} at timestamp {}",
            kafkaTopic,
            signal.getSignalTimestamp(PushControlSignalType.BLOB_UPLOAD_COMPLETE));
        return;
      }
      Thread.sleep(pollIntervalMs);
    }
    throw new InterruptedException("Interrupted while waiting for BLOB_UPLOAD_COMPLETE signal for " + kafkaTopic);
  }

  /**
   * Download SST files from blob storage to a local temp directory with retry logic.
   */
  List<String> downloadSSTFiles() throws IOException {
    String remotePartitionDir = BlobStoragePaths.partitionDir(blobStorageUri, storeName, versionNumber, partition);
    String localTempDir = RocksDBUtils.composeTempSSTFileDir(serverConfig.getRocksDBPath(), kafkaTopic, partition);

    File localTempDirFile = new File(localTempDir);
    if (!localTempDirFile.exists() && !localTempDirFile.mkdirs()) {
      throw new VeniceException("Failed to create local temp directory for blob ingestion: " + localTempDir);
    }

    LOGGER.info(
        "Downloading SST files from {} to {} for {} partition {}",
        remotePartitionDir,
        localTempDir,
        kafkaTopic,
        partition);

    BlobStorageClient client = createBlobStorageClient();
    try {
      // Download partition directory contents
      downloadWithRetry(client, remotePartitionDir, localTempDir);
    } finally {
      try {
        client.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close BlobStorageClient for {} partition {}", kafkaTopic, partition, e);
      }
    }

    // Collect all SST files from the temp directory
    List<String> sstFilePaths = new ArrayList<>();
    File[] files = localTempDirFile.listFiles((dir, name) -> name.endsWith(".sst"));
    if (files != null) {
      for (File file: files) {
        sstFilePaths.add(file.getAbsolutePath());
      }
    }

    if (sstFilePaths.isEmpty()) {
      throw new VeniceException(
          "No SST files found after download for " + kafkaTopic + " partition " + partition + " in " + localTempDir);
    }

    LOGGER.info(
        "Downloaded {} SST files for {} partition {}: {}",
        sstFilePaths.size(),
        kafkaTopic,
        partition,
        sstFilePaths);
    return sstFilePaths;
  }

  /**
   * Download with exponential backoff retry.
   */
  private void downloadWithRetry(BlobStorageClient client, String remotePath, String localPath) throws IOException {
    int attempt = 0;
    while (true) {
      try {
        client.download(remotePath, localPath);
        return;
      } catch (IOException e) {
        attempt++;
        if (attempt >= downloadMaxRetries) {
          throw new IOException(
              "Failed to download from " + remotePath + " after " + downloadMaxRetries + " attempts",
              e);
        }
        long backoffMs = (long) Math.pow(2, attempt) * 1000;
        LOGGER.warn(
            "Download attempt {}/{} failed for {} partition {}, retrying in {} ms",
            attempt,
            downloadMaxRetries,
            kafkaTopic,
            partition,
            backoffMs,
            e);
        try {
          Thread.sleep(backoffMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw new IOException("Interrupted during download retry backoff", ie);
        }
      }
    }
  }

  /**
   * Ingest downloaded SST files into RocksDB.
   */
  void ingestSSTFiles(List<String> sstFilePaths) {
    LOGGER.info("Opening store partition and ingesting SST files for {} partition {}", kafkaTopic, partition);
    storageService.openStoreForNewPartition(storeConfig, partition, svsSupplier);

    AbstractStoragePartition storagePartition =
        storageService.getStorageEngine(kafkaTopic).getPartitionOrThrow(partition);
    if (!(storagePartition instanceof RocksDBStoragePartition)) {
      throw new VeniceException(
          "Expected RocksDBStoragePartition but got " + storagePartition.getClass().getName() + " for " + kafkaTopic
              + " partition " + partition);
    }

    RocksDBStoragePartition rocksDBPartition = (RocksDBStoragePartition) storagePartition;
    rocksDBPartition.ingestExternalSSTFiles(sstFilePaths);
  }

  BlobStorageClient createBlobStorageClient() {
    switch (blobStorageType) {
      case LOCAL_FS:
        return new LocalFsBlobStorageClient();
      default:
        throw new VeniceException("Unsupported blob storage type for ingestion: " + blobStorageType);
    }
  }

  private void reportStarted() {
    for (VeniceNotifier notifier: notifiers) {
      try {
        notifier.started(kafkaTopic, partition);
      } catch (Exception e) {
        LOGGER.error("Failed to report STARTED to notifier for {} partition {}", kafkaTopic, partition, e);
      }
    }
  }

  private void reportEndOfPushReceived() {
    for (VeniceNotifier notifier: notifiers) {
      try {
        notifier.endOfPushReceived(kafkaTopic, partition, null);
      } catch (Exception e) {
        LOGGER.error("Failed to report END_OF_PUSH_RECEIVED to notifier for {} partition {}", kafkaTopic, partition, e);
      }
    }
  }

  private void reportCompleted() {
    for (VeniceNotifier notifier: notifiers) {
      try {
        notifier.completed(kafkaTopic, partition, null);
      } catch (Exception e) {
        LOGGER.error("Failed to report COMPLETED to notifier for {} partition {}", kafkaTopic, partition, e);
      }
    }
  }

  private void reportError(String message, Exception ex) {
    for (VeniceNotifier notifier: notifiers) {
      try {
        notifier.error(kafkaTopic, partition, message, ex);
      } catch (Exception e) {
        LOGGER.error("Failed to report ERROR to notifier for {} partition {}", kafkaTopic, partition, e);
      }
    }
  }
}
