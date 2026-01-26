package com.linkedin.davinci.store.rocksdb;

import com.linkedin.venice.blobtransfer.BlobFileMetadata;
import com.linkedin.venice.blobtransfer.BlobTransferManifest;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.kafka.validation.checksum.CheckSum;
import com.linkedin.venice.kafka.validation.checksum.CheckSumType;
import java.io.Closeable;
import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;


/**
 * SST File Writer utility for Venice Push Job blob-based push.
 * This class provides a simplified interface for writing sorted key-value pairs
 * directly to RocksDB SST files, which can then be transferred to servers
 * for ingestion via the blob transfer mechanism.
 *
 * Key features:
 * - Writes SST files to a specified local directory
 * - Handles automatic file rotation based on record count or file size limits
 * - Computes checksums for data integrity verification
 * - Generates metadata for manifest creation
 *
 * Usage:
 * 1. Create instance with output directory and configuration
 * 2. Call open() to initialize
 * 3. Call put() for each sorted key-value pair (MUST be in sorted order)
 * 4. Call close() to finalize and get metadata
 */
public class VPJSstFileWriter implements Closeable {
  private static final Logger LOGGER = LogManager.getLogger(VPJSstFileWriter.class);

  private static final long DEFAULT_MAX_SST_FILE_SIZE_BYTES = 256 * 1024 * 1024L; // 256MB
  private static final long DEFAULT_MAX_RECORDS_PER_SST_FILE = 10_000_000L; // 10M records
  private static final String SST_FILE_EXTENSION = ".sst";
  private static final String SST_FILE_NAME_FORMAT = "%06d" + SST_FILE_EXTENSION;

  private final String outputDirectory;
  private final String storeName;
  private final int version;
  private final int partition;
  private final String columnFamily;
  private final long maxSstFileSizeBytes;
  private final long maxRecordsPerSstFile;

  private final EnvOptions envOptions;
  private final Options options;

  private SstFileWriter currentWriter;
  private int currentFileNumber;
  private long recordsInCurrentFile;
  private long totalRecordCount;
  private CheckSum currentChecksum;
  private final List<BlobFileMetadata> sstFileMetadataList;

  private boolean isOpen;
  private boolean isClosed;

  /**
   * Creates a new VPJ SST File Writer.
   *
   * @param outputDirectory The local directory to write SST files to
   * @param storeName       The Venice store name
   * @param version         The version number
   * @param partition       The partition number
   * @param columnFamily    The column family name (DEFAULT or REPLICATION_METADATA)
   */
  public VPJSstFileWriter(String outputDirectory, String storeName, int version, int partition, String columnFamily) {
    this(
        outputDirectory,
        storeName,
        version,
        partition,
        columnFamily,
        DEFAULT_MAX_SST_FILE_SIZE_BYTES,
        DEFAULT_MAX_RECORDS_PER_SST_FILE);
  }

  /**
   * Creates a new VPJ SST File Writer with custom size limits.
   *
   * @param outputDirectory       The local directory to write SST files to
   * @param storeName             The Venice store name
   * @param version               The version number
   * @param partition             The partition number
   * @param columnFamily          The column family name
   * @param maxSstFileSizeBytes   Maximum size per SST file in bytes before rotation
   * @param maxRecordsPerSstFile  Maximum records per SST file before rotation
   */
  public VPJSstFileWriter(
      String outputDirectory,
      String storeName,
      int version,
      int partition,
      String columnFamily,
      long maxSstFileSizeBytes,
      long maxRecordsPerSstFile) {
    this.outputDirectory = outputDirectory;
    this.storeName = storeName;
    this.version = version;
    this.partition = partition;
    this.columnFamily = columnFamily;
    this.maxSstFileSizeBytes = maxSstFileSizeBytes;
    this.maxRecordsPerSstFile = maxRecordsPerSstFile;

    this.envOptions = new EnvOptions();
    this.options = new Options();
    // Configure options for optimal SST file generation
    this.options.setCreateIfMissing(true);
    // Use default compression - can be made configurable

    this.currentFileNumber = 0;
    this.recordsInCurrentFile = 0;
    this.totalRecordCount = 0;
    this.sstFileMetadataList = new ArrayList<>();
    this.isOpen = false;
    this.isClosed = false;
  }

  /**
   * Configure custom RocksDB options for SST file generation.
   * Must be called before open().
   *
   * @param optionsConfigurator A function to configure the Options
   * @return this writer for chaining
   */
  public VPJSstFileWriter configureOptions(java.util.function.Consumer<Options> optionsConfigurator) {
    if (isOpen) {
      throw new IllegalStateException("Cannot configure options after writer is opened");
    }
    optionsConfigurator.accept(options);
    return this;
  }

  /**
   * Opens the writer and prepares for writing.
   * Creates the output directory if it doesn't exist.
   */
  public void open() {
    if (isOpen) {
      throw new IllegalStateException("Writer is already open");
    }
    if (isClosed) {
      throw new IllegalStateException("Writer has been closed and cannot be reopened");
    }

    // Create output directory
    File outputDir = new File(outputDirectory);
    if (!outputDir.exists()) {
      boolean created = outputDir.mkdirs();
      if (!created) {
        throw new VeniceException("Failed to create output directory: " + outputDirectory);
      }
    }

    try {
      openNewSstFile();
      isOpen = true;
      LOGGER.info(
          "Opened VPJSstFileWriter for store: {}, version: {}, partition: {}, column family: {}, output: {}",
          storeName,
          version,
          partition,
          columnFamily,
          outputDirectory);
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to open SST file writer", e);
    }
  }

  /**
   * Write a key-value pair to the SST file.
   * Keys MUST be written in sorted order (lexicographically ascending).
   *
   * @param key   The key bytes
   * @param value The value bytes
   */
  public void put(byte[] key, byte[] value) {
    if (!isOpen) {
      throw new IllegalStateException("Writer is not open");
    }
    if (isClosed) {
      throw new IllegalStateException("Writer has been closed");
    }

    try {
      currentWriter.put(key, value);
      recordsInCurrentFile++;
      totalRecordCount++;

      // Update checksum
      if (currentChecksum != null) {
        currentChecksum.update(key);
        currentChecksum.update(value);
      }

      // Check if we need to rotate to a new file
      if (shouldRotate()) {
        rotateSstFile();
      }
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to write to SST file", e);
    }
  }

  /**
   * Write a key-value pair to the SST file.
   *
   * @param key         The key bytes
   * @param valueBuffer The value as a ByteBuffer
   */
  public void put(byte[] key, ByteBuffer valueBuffer) {
    byte[] value = new byte[valueBuffer.remaining()];
    valueBuffer.mark();
    valueBuffer.get(value);
    valueBuffer.reset();
    put(key, value);
  }

  private boolean shouldRotate() {
    // Rotate based on record count
    if (recordsInCurrentFile >= maxRecordsPerSstFile) {
      return true;
    }

    // Check file size periodically (every 1000 records to avoid overhead)
    if (recordsInCurrentFile % 1000 == 0) {
      String currentFilePath = getCurrentSstFilePath();
      File currentFile = new File(currentFilePath);
      if (currentFile.exists() && currentFile.length() >= maxSstFileSizeBytes) {
        return true;
      }
    }

    return false;
  }

  private void rotateSstFile() throws RocksDBException {
    finishCurrentFile();
    currentFileNumber++;
    openNewSstFile();
  }

  private void openNewSstFile() throws RocksDBException {
    String filePath = getCurrentSstFilePath();
    currentWriter = new SstFileWriter(envOptions, options);
    currentWriter.open(filePath);
    recordsInCurrentFile = 0;
    currentChecksum = CheckSum.getInstance(CheckSumType.MD5);
    LOGGER.debug("Opened new SST file: {}", filePath);
  }

  private void finishCurrentFile() throws RocksDBException {
    if (currentWriter != null && recordsInCurrentFile > 0) {
      currentWriter.finish();

      String filePath = getCurrentSstFilePath();
      File sstFile = new File(filePath);

      // Create metadata for this file
      String fileName = String.format(SST_FILE_NAME_FORMAT, currentFileNumber);
      long fileSize = sstFile.length();
      String checksum = currentChecksum != null ? bytesToHex(currentChecksum.getCheckSum()) : null;

      BlobFileMetadata metadata = new BlobFileMetadata(fileName, fileSize, checksum, columnFamily);
      sstFileMetadataList.add(metadata);

      LOGGER.info(
          "Finished SST file: {}, size: {} bytes, records: {}, checksum: {}",
          filePath,
          fileSize,
          recordsInCurrentFile,
          checksum);
    }

    if (currentWriter != null) {
      currentWriter.close();
      currentWriter = null;
    }
    currentChecksum = null;
  }

  private String getCurrentSstFilePath() {
    return outputDirectory + File.separator + String.format(SST_FILE_NAME_FORMAT, currentFileNumber);
  }

  private static String bytesToHex(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b: bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }

  /**
   * Close the writer and finalize all SST files.
   * After calling close(), getMetadataList() and createManifest() can be called
   * to retrieve the generated file metadata.
   */
  @Override
  public void close() {
    if (isClosed) {
      return;
    }

    try {
      if (recordsInCurrentFile > 0) {
        finishCurrentFile();
      } else if (currentWriter != null) {
        // Close empty writer without finishing
        currentWriter.close();
        currentWriter = null;
      }
    } catch (RocksDBException e) {
      throw new VeniceException("Failed to close SST file writer", e);
    } finally {
      if (envOptions != null) {
        envOptions.close();
      }
      if (options != null) {
        options.close();
      }
      isClosed = true;
      isOpen = false;
    }

    LOGGER.info(
        "Closed VPJSstFileWriter for store: {}, version: {}, partition: {}, total files: {}, total records: {}",
        storeName,
        version,
        partition,
        sstFileMetadataList.size(),
        totalRecordCount);
  }

  /**
   * Get the list of SST file metadata generated by this writer.
   * Only valid after close() has been called.
   *
   * @return List of metadata for each SST file
   */
  public List<BlobFileMetadata> getMetadataList() {
    if (!isClosed) {
      throw new IllegalStateException("Writer must be closed before getting metadata");
    }
    return new ArrayList<>(sstFileMetadataList);
  }

  /**
   * Create a BlobTransferManifest for this partition.
   * Only valid after close() has been called.
   *
   * @param tableFormat The RocksDB table format (e.g., "BLOCK_BASED_TABLE")
   * @return A manifest containing all SST file metadata
   */
  public BlobTransferManifest createManifest(String tableFormat) {
    if (!isClosed) {
      throw new IllegalStateException("Writer must be closed before creating manifest");
    }

    Map<String, String> rocksDbOptions = new HashMap<>();
    // Add any relevant RocksDB options that servers need to know about
    rocksDbOptions.put("compression_type", options.compressionType().name());

    return new BlobTransferManifest(
        storeName,
        version,
        partition,
        new ArrayList<>(sstFileMetadataList),
        totalRecordCount,
        tableFormat,
        rocksDbOptions);
  }

  /**
   * Get the total number of records written.
   *
   * @return Total record count
   */
  public long getTotalRecordCount() {
    return totalRecordCount;
  }

  /**
   * Get the number of SST files generated.
   *
   * @return Number of SST files
   */
  public int getSstFileCount() {
    return sstFileMetadataList.size() + (recordsInCurrentFile > 0 ? 1 : 0);
  }

  /**
   * Get the output directory.
   *
   * @return The output directory path
   */
  public String getOutputDirectory() {
    return outputDirectory;
  }
}
