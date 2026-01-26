package com.linkedin.davinci.blobtransfer;

import com.linkedin.venice.blobtransfer.BlobFileMetadata;
import com.linkedin.venice.blobtransfer.BlobTransferManifest;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.validation.checksum.CheckSum;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Service for pulling SST files from HDFS for blob-based Venice push.
 * This service is used by servers to download SST files that were generated
 * by the VPJ and uploaded to HDFS.
 *
 * The pull process:
 * 1. Read manifest from HDFS to get list of SST files
 * 2. Download each SST file to local directory
 * 3. Verify checksums
 * 4. Return metadata for ingestion
 */
public class HDFSBlobPullService implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(HDFSBlobPullService.class);

  private static final int BUFFER_SIZE = 64 * 1024; // 64KB buffer

  private final Configuration hadoopConf;
  private FileSystem fileSystem;

  public HDFSBlobPullService() {
    this(new Configuration());
  }

  public HDFSBlobPullService(Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
  }

  /**
   * Pull SST files from HDFS to local directory.
   *
   * @param hdfsPath        The HDFS path to the partition directory containing SST files and manifest
   * @param localDirectory  The local directory to download files to
   * @return The manifest containing metadata about the downloaded files
   * @throws IOException if the pull operation fails
   */
  public BlobTransferManifest pullFromHDFS(String hdfsPath, String localDirectory) throws IOException {
    org.apache.hadoop.fs.Path hdfsDir = new org.apache.hadoop.fs.Path(hdfsPath);
    FileSystem fs = getFileSystem(hdfsDir);

    LOGGER.info("Pulling blobs from HDFS path: {} to local directory: {}", hdfsPath, localDirectory);

    // Create local directory if it doesn't exist
    File localDir = new File(localDirectory);
    if (!localDir.exists()) {
      boolean created = localDir.mkdirs();
      if (!created) {
        throw new IOException("Failed to create local directory: " + localDirectory);
      }
    }

    // Read manifest
    String manifestPath = hdfsPath + "/" + BlobTransferManifest.MANIFEST_FILE_NAME;
    BlobTransferManifest manifest;
    try (FSDataInputStream manifestStream = fs.open(new org.apache.hadoop.fs.Path(manifestPath))) {
      manifest = BlobTransferManifest.fromInputStream(manifestStream);
    }

    LOGGER.info(
        "Read manifest for store: {}, version: {}, partition: {}, SST files: {}",
        manifest.getStoreName(),
        manifest.getVersion(),
        manifest.getPartition(),
        manifest.getSstFiles().size());

    // Download each SST file
    List<String> downloadedFiles = new ArrayList<>();
    for (BlobFileMetadata fileMetadata: manifest.getSstFiles()) {
      String hdfsFilePath = hdfsPath + "/" + fileMetadata.getFileName();
      String localFilePath = localDirectory + "/" + fileMetadata.getFileName();

      downloadFile(fs, hdfsFilePath, localFilePath, fileMetadata);
      downloadedFiles.add(localFilePath);
    }

    LOGGER.info(
        "Successfully pulled {} SST files from HDFS to local directory: {}",
        downloadedFiles.size(),
        localDirectory);

    return manifest;
  }

  /**
   * Download a single file from HDFS and verify its checksum.
   */
  private void downloadFile(FileSystem fs, String hdfsFilePath, String localFilePath, BlobFileMetadata metadata)
      throws IOException {
    org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(hdfsFilePath);

    LOGGER.info("Downloading file: {} ({} bytes) to {}", metadata.getFileName(), metadata.getFileSize(), localFilePath);

    CheckSum checkSum = metadata.getChecksum() != null ? CheckSum.getInstance(CheckSumType.MD5) : null;

    try (FSDataInputStream in = fs.open(hdfsPath, BUFFER_SIZE);
        FileOutputStream out = new FileOutputStream(localFilePath)) {

      byte[] buffer = new byte[BUFFER_SIZE];
      int bytesRead;
      long totalBytesRead = 0;

      while ((bytesRead = in.read(buffer)) != -1) {
        out.write(buffer, 0, bytesRead);
        totalBytesRead += bytesRead;

        if (checkSum != null) {
          checkSum.update(buffer, 0, bytesRead);
        }
      }

      LOGGER.info("Downloaded {} bytes for file: {}", totalBytesRead, metadata.getFileName());
    }

    // Verify checksum if provided
    if (metadata.getChecksum() != null && checkSum != null) {
      String computedChecksum = bytesToHex(checkSum.getCheckSum());
      if (!computedChecksum.equals(metadata.getChecksum())) {
        // Delete corrupted file
        Files.deleteIfExists(Path.of(localFilePath));
        throw new VeniceException(
            "Checksum mismatch for file " + metadata.getFileName() + ". Expected: " + metadata.getChecksum()
                + ", Computed: " + computedChecksum);
      }
      LOGGER.debug("Checksum verified for file: {}", metadata.getFileName());
    }
  }

  /**
   * Check if blob files are available in HDFS for a given partition.
   *
   * @param hdfsPath The HDFS path to the partition directory
   * @return true if manifest file exists, false otherwise
   */
  public boolean isBlobAvailable(String hdfsPath) {
    try {
      String manifestPath = hdfsPath + "/" + BlobTransferManifest.MANIFEST_FILE_NAME;
      org.apache.hadoop.fs.Path hdfsManifestPath = new org.apache.hadoop.fs.Path(manifestPath);
      FileSystem fs = getFileSystem(hdfsManifestPath);
      return fs.exists(hdfsManifestPath);
    } catch (IOException e) {
      LOGGER.warn("Failed to check blob availability at path: {}", hdfsPath, e);
      return false;
    }
  }

  /**
   * List all partitions available for a store version in HDFS.
   *
   * @param hdfsBasePath The HDFS base path for the store version
   * @return List of partition numbers
   */
  public List<Integer> listAvailablePartitions(String hdfsBasePath) throws IOException {
    org.apache.hadoop.fs.Path hdfsPath = new org.apache.hadoop.fs.Path(hdfsBasePath);
    FileSystem fs = getFileSystem(hdfsPath);

    List<Integer> partitions = new ArrayList<>();
    if (!fs.exists(hdfsPath)) {
      return partitions;
    }

    FileStatus[] statuses = fs.listStatus(hdfsPath);
    for (FileStatus status: statuses) {
      if (status.isDirectory()) {
        String dirName = status.getPath().getName();
        if (dirName.startsWith("partition_")) {
          try {
            int partitionNum = Integer.parseInt(dirName.substring("partition_".length()));
            partitions.add(partitionNum);
          } catch (NumberFormatException e) {
            LOGGER.warn("Invalid partition directory name: {}", dirName);
          }
        }
      }
    }

    return partitions;
  }

  private FileSystem getFileSystem(org.apache.hadoop.fs.Path path) throws IOException {
    if (fileSystem == null) {
      fileSystem = path.getFileSystem(hadoopConf);
    }
    return fileSystem;
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b: bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  @Override
  public void close() throws IOException {
    if (fileSystem != null) {
      fileSystem.close();
      fileSystem = null;
    }
  }
}
