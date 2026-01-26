package com.linkedin.venice.hadoop.output;

import com.linkedin.venice.blobtransfer.BlobFileMetadata;
import com.linkedin.venice.blobtransfer.BlobTransferManifest;
import com.linkedin.venice.blobtransfer.client.BlobStorageClient;
import com.linkedin.venice.exceptions.VeniceException;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * HDFS implementation of BlobStorageClient for blob-based Venice Push Job.
 * This class handles writing SST files and manifests to HDFS.
 */
public class HDFSBlobStorageClient implements BlobStorageClient {
  private static final Logger LOGGER = LogManager.getLogger(HDFSBlobStorageClient.class);

  private static final short REPLICATION_FACTOR = 3;
  private static final int BUFFER_SIZE = 64 * 1024; // 64KB buffer
  private static final FsPermission DIRECTORY_PERMISSION = new FsPermission((short) 0700);
  private static final FsPermission FILE_PERMISSION = new FsPermission((short) 0600);

  private final Configuration hadoopConf;
  private FileSystem fileSystem;

  public HDFSBlobStorageClient() {
    this(new Configuration());
  }

  public HDFSBlobStorageClient(Configuration hadoopConf) {
    this.hadoopConf = hadoopConf;
  }

  private FileSystem getFileSystem(Path path) throws IOException {
    if (fileSystem == null) {
      fileSystem = path.getFileSystem(hadoopConf);
    }
    return fileSystem;
  }

  @Override
  public void writeFile(String path, InputStream data, long size) throws IOException {
    Path hdfsPath = new Path(path);
    FileSystem fs = getFileSystem(hdfsPath);

    LOGGER.info("Writing file to HDFS: {} (size: {} bytes)", path, size);
    try (FSDataOutputStream out = fs.create(
        hdfsPath,
        FILE_PERMISSION,
        true,
        BUFFER_SIZE,
        REPLICATION_FACTOR,
        fs.getDefaultBlockSize(hdfsPath),
        null)) {
      byte[] buffer = new byte[BUFFER_SIZE];
      int bytesRead;
      long totalBytesWritten = 0;
      while ((bytesRead = data.read(buffer)) != -1) {
        out.write(buffer, 0, bytesRead);
        totalBytesWritten += bytesRead;
      }
      LOGGER.info("Successfully wrote {} bytes to HDFS: {}", totalBytesWritten, path);
    }
  }

  @Override
  public OutputStream createOutputStream(String path) throws IOException {
    Path hdfsPath = new Path(path);
    FileSystem fs = getFileSystem(hdfsPath);

    LOGGER.info("Creating output stream to HDFS: {}", path);
    FSDataOutputStream out = fs.create(
        hdfsPath,
        FILE_PERMISSION,
        true,
        BUFFER_SIZE,
        REPLICATION_FACTOR,
        fs.getDefaultBlockSize(hdfsPath),
        null);
    return new BufferedOutputStream(out, BUFFER_SIZE);
  }

  @Override
  public InputStream readFile(String path) throws IOException {
    Path hdfsPath = new Path(path);
    FileSystem fs = getFileSystem(hdfsPath);

    LOGGER.info("Reading file from HDFS: {}", path);
    FSDataInputStream in = fs.open(hdfsPath, BUFFER_SIZE);
    return in;
  }

  @Override
  public List<BlobFileMetadata> listFiles(String directory) throws IOException {
    Path hdfsPath = new Path(directory);
    FileSystem fs = getFileSystem(hdfsPath);

    List<BlobFileMetadata> fileList = new ArrayList<>();
    if (!fs.exists(hdfsPath)) {
      LOGGER.warn("Directory does not exist: {}", directory);
      return fileList;
    }

    FileStatus[] statuses = fs.listStatus(hdfsPath);
    for (FileStatus status: statuses) {
      if (status.isFile()) {
        String fileName = status.getPath().getName();
        long fileSize = status.getLen();
        // Checksum will be computed separately if needed
        fileList.add(new BlobFileMetadata(fileName, fileSize, null, null));
      }
    }

    LOGGER.info("Listed {} files in directory: {}", fileList.size(), directory);
    return fileList;
  }

  @Override
  public void deleteFile(String path) throws IOException {
    Path hdfsPath = new Path(path);
    FileSystem fs = getFileSystem(hdfsPath);

    if (fs.exists(hdfsPath)) {
      boolean deleted = fs.delete(hdfsPath, false);
      if (!deleted) {
        throw new IOException("Failed to delete file: " + path);
      }
      LOGGER.info("Deleted file: {}", path);
    } else {
      LOGGER.warn("File does not exist, skipping delete: {}", path);
    }
  }

  @Override
  public void deleteDirectory(String directory) throws IOException {
    Path hdfsPath = new Path(directory);
    FileSystem fs = getFileSystem(hdfsPath);

    if (fs.exists(hdfsPath)) {
      boolean deleted = fs.delete(hdfsPath, true);
      if (!deleted) {
        throw new IOException("Failed to delete directory: " + directory);
      }
      LOGGER.info("Deleted directory: {}", directory);
    } else {
      LOGGER.warn("Directory does not exist, skipping delete: {}", directory);
    }
  }

  @Override
  public boolean exists(String path) throws IOException {
    Path hdfsPath = new Path(path);
    FileSystem fs = getFileSystem(hdfsPath);
    return fs.exists(hdfsPath);
  }

  @Override
  public void createDirectory(String directory) throws IOException {
    Path hdfsPath = new Path(directory);
    FileSystem fs = getFileSystem(hdfsPath);

    if (!fs.exists(hdfsPath)) {
      boolean created = fs.mkdirs(hdfsPath, DIRECTORY_PERMISSION);
      if (!created) {
        throw new IOException("Failed to create directory: " + directory);
      }
      // mkdirs doesn't always respect the permission, so set it explicitly
      fs.setPermission(hdfsPath, DIRECTORY_PERMISSION);
      LOGGER.info("Created directory: {}", directory);
    } else {
      LOGGER.info("Directory already exists: {}", directory);
    }
  }

  @Override
  public long getFileSize(String path) throws IOException {
    Path hdfsPath = new Path(path);
    FileSystem fs = getFileSystem(hdfsPath);

    FileStatus status = fs.getFileStatus(hdfsPath);
    return status.getLen();
  }

  @Override
  public void writeManifest(String directory, BlobTransferManifest manifest) throws IOException {
    String manifestPath = directory + "/" + BlobTransferManifest.MANIFEST_FILE_NAME;
    String manifestJson = manifest.toJson();
    byte[] manifestBytes = manifestJson.getBytes(StandardCharsets.UTF_8);

    Path hdfsPath = new Path(manifestPath);
    FileSystem fs = getFileSystem(hdfsPath);

    LOGGER.info(
        "Writing manifest to HDFS: {} (store: {}, version: {}, partition: {})",
        manifestPath,
        manifest.getStoreName(),
        manifest.getVersion(),
        manifest.getPartition());

    try (FSDataOutputStream out = fs.create(
        hdfsPath,
        FILE_PERMISSION,
        true,
        BUFFER_SIZE,
        REPLICATION_FACTOR,
        fs.getDefaultBlockSize(hdfsPath),
        null)) {
      out.write(manifestBytes);
    }
  }

  @Override
  public BlobTransferManifest readManifest(String directory) throws IOException {
    String manifestPath = directory + "/" + BlobTransferManifest.MANIFEST_FILE_NAME;
    Path hdfsPath = new Path(manifestPath);
    FileSystem fs = getFileSystem(hdfsPath);

    if (!fs.exists(hdfsPath)) {
      throw new VeniceException("Manifest file does not exist: " + manifestPath);
    }

    LOGGER.info("Reading manifest from HDFS: {}", manifestPath);
    try (FSDataInputStream in = fs.open(hdfsPath, BUFFER_SIZE)) {
      return BlobTransferManifest.fromInputStream(in);
    }
  }

  @Override
  public void close() throws IOException {
    if (fileSystem != null) {
      fileSystem.close();
      fileSystem = null;
    }
  }
}
