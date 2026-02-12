package com.linkedin.venice.blobtransfer.storage;

import java.io.IOException;


/**
 * Pluggable abstraction for blob storage operations (upload, download, delete).
 * Implementations may target HDFS, S3, local filesystem, etc.
 */
public interface BlobStorageClient extends AutoCloseable {
  /**
   * Upload a local file or directory to a remote path.
   */
  BlobTransferResult upload(String localPath, String remotePath) throws IOException;

  /**
   * Download a remote file or directory to a local path.
   */
  BlobTransferResult download(String remotePath, String localPath) throws IOException;

  /**
   * Check whether a remote path exists.
   */
  boolean exists(String remotePath) throws IOException;

  /**
   * Delete a single file at the given remote path.
   */
  void delete(String remotePath) throws IOException;

  /**
   * Recursively delete a directory at the given remote path.
   */
  void deleteDirectory(String remotePath) throws IOException;
}
