package com.linkedin.davinci.blobtransfer;

import com.linkedin.venice.blobtransfer.BlobTransferManifest;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;


/**
 * Interface for pulling blob files (SST files) from a remote storage location.
 * This abstraction allows different implementations for HDFS, S3, GCS, or local file system.
 */
public interface BlobPullService extends Closeable {
  /**
   * Check if blob files are available at the given path.
   *
   * @param blobPath The path to check for blob availability
   * @return true if blobs are available, false otherwise
   */
  boolean isBlobAvailable(String blobPath);

  /**
   * Pull blob files from remote storage to local directory.
   *
   * @param remotePath The remote path containing blob files
   * @param localDirectory The local directory to download files to
   * @return The manifest containing metadata about the downloaded files
   * @throws IOException if the pull operation fails
   */
  BlobTransferManifest pullBlobs(String remotePath, String localDirectory) throws IOException;

  /**
   * List all partitions available at the given base path.
   *
   * @param basePath The base path for the store version
   * @return List of partition numbers
   * @throws IOException if listing fails
   */
  List<Integer> listAvailablePartitions(String basePath) throws IOException;
}
