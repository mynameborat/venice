package com.linkedin.venice.blobtransfer.client;

import com.linkedin.venice.blobtransfer.BlobFileMetadata;
import com.linkedin.venice.blobtransfer.BlobTransferManifest;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;


/**
 * Interface for blob storage operations used in blob-based Venice Push Job.
 * This abstraction allows for different storage backends (HDFS, S3, GCS, etc.)
 * without changing the VPJ or Server code.
 *
 * The storage layout for blob-based push is:
 * {basePath}/{storeName}_v{version}/partition_{N}/
 *   - manifest.json (the BlobTransferManifest)
 *   - 000001.sst
 *   - 000002.sst
 *   - ...
 */
public interface BlobStorageClient extends Closeable {
  /**
   * Write a file to blob storage.
   *
   * @param path    The full path to write the file to
   * @param data    The input stream containing the file data
   * @param size    The size of the data in bytes
   * @throws IOException if the write fails
   */
  void writeFile(String path, InputStream data, long size) throws IOException;

  /**
   * Create an output stream for writing to a file.
   * This is useful for streaming writes where the size is not known upfront.
   *
   * @param path The full path to write the file to
   * @return An OutputStream for writing data
   * @throws IOException if the stream creation fails
   */
  OutputStream createOutputStream(String path) throws IOException;

  /**
   * Read a file from blob storage.
   *
   * @param path The full path of the file to read
   * @return An InputStream containing the file data
   * @throws IOException if the read fails
   */
  InputStream readFile(String path) throws IOException;

  /**
   * List all files in a directory.
   *
   * @param directory The directory path to list
   * @return List of file metadata for files in the directory
   * @throws IOException if the list operation fails
   */
  List<BlobFileMetadata> listFiles(String directory) throws IOException;

  /**
   * Delete a file from blob storage.
   *
   * @param path The full path of the file to delete
   * @throws IOException if the delete fails
   */
  void deleteFile(String path) throws IOException;

  /**
   * Delete a directory and all its contents recursively.
   *
   * @param directory The directory path to delete
   * @throws IOException if the delete fails
   */
  void deleteDirectory(String directory) throws IOException;

  /**
   * Check if a file or directory exists.
   *
   * @param path The path to check
   * @return true if the path exists, false otherwise
   * @throws IOException if the check fails
   */
  boolean exists(String path) throws IOException;

  /**
   * Create a directory if it doesn't exist.
   *
   * @param directory The directory path to create
   * @throws IOException if the creation fails
   */
  void createDirectory(String directory) throws IOException;

  /**
   * Get the file size in bytes.
   *
   * @param path The file path
   * @return The file size in bytes
   * @throws IOException if the operation fails
   */
  long getFileSize(String path) throws IOException;

  /**
   * Write a manifest file to blob storage.
   *
   * @param directory The directory to write the manifest to
   * @param manifest  The manifest to write
   * @throws IOException if the write fails
   */
  void writeManifest(String directory, BlobTransferManifest manifest) throws IOException;

  /**
   * Read a manifest file from blob storage.
   *
   * @param directory The directory containing the manifest
   * @return The manifest
   * @throws IOException if the read fails
   */
  BlobTransferManifest readManifest(String directory) throws IOException;

  /**
   * Compose the full path for a partition's blob staging directory.
   *
   * @param basePath   The base path for blob storage
   * @param storeName  The store name
   * @param version    The version number
   * @param partition  The partition number
   * @return The full path to the partition directory
   */
  default String composePartitionPath(String basePath, String storeName, int version, int partition) {
    return basePath + "/" + storeName + "_v" + version + "/partition_" + partition;
  }

  /**
   * Compose the full path for a version's blob staging directory.
   *
   * @param basePath   The base path for blob storage
   * @param storeName  The store name
   * @param version    The version number
   * @return The full path to the version directory
   */
  default String composeVersionPath(String basePath, String storeName, int version) {
    return basePath + "/" + storeName + "_v" + version;
  }
}
