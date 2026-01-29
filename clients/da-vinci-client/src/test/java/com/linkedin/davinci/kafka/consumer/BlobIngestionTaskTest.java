package com.linkedin.davinci.kafka.consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.davinci.blobtransfer.BlobPullService;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.store.AbstractStorageEngine;
import com.linkedin.venice.blobtransfer.BlobFileMetadata;
import com.linkedin.venice.blobtransfer.BlobTransferManifest;
import com.linkedin.venice.exceptions.VeniceException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Unit tests for {@link BlobIngestionTask}.
 */
public class BlobIngestionTaskTest {
  private static final String STORE_NAME = "test-store";
  private static final int VERSION = 1;
  private static final int PARTITION = 0;
  private static final String BLOB_STAGING_PATH = "/test/staging/path";

  private VeniceStoreVersionConfig storeConfig;
  private AbstractStorageEngine storageEngine;
  private VeniceNotifier notifier;
  private BlobPullService blobPullService;
  private Path tempDir;
  private String localBaseDir;

  @BeforeMethod
  public void setUp() throws IOException {
    storeConfig = mock(VeniceStoreVersionConfig.class);
    storageEngine = mock(AbstractStorageEngine.class);
    notifier = mock(VeniceNotifier.class);
    blobPullService = mock(BlobPullService.class);

    // Create a temp directory for the test
    tempDir = Files.createTempDirectory("blob-ingestion-test");
    localBaseDir = tempDir.toString();
  }

  @AfterMethod
  public void tearDown() throws IOException {
    // Clean up temp directory
    if (tempDir != null) {
      FileUtils.deleteDirectory(tempDir.toFile());
    }
  }

  private BlobTransferManifest createManifest(long recordCount, List<BlobFileMetadata> sstFiles) {
    return new BlobTransferManifest(
        STORE_NAME,
        VERSION,
        PARTITION,
        sstFiles,
        recordCount,
        "BLOCK_BASED_TABLE",
        Collections.emptyMap());
  }

  private BlobFileMetadata createSstFileMetadata(String fileName, long size, String checksum, String columnFamily) {
    return new BlobFileMetadata(fileName, size, checksum, columnFamily);
  }

  /**
   * Creates a temp SST file in the expected location for the test.
   * The file path structure: {localBaseDir}/{storeName}_v{version}/{partition}/blob_ingestion/{fileName}
   */
  private void createTempSstFile(String fileName) throws IOException {
    String kafkaTopic = STORE_NAME + "_v" + VERSION;
    Path blobIngestionDir = tempDir.resolve(kafkaTopic).resolve(String.valueOf(PARTITION)).resolve("blob_ingestion");
    Files.createDirectories(blobIngestionDir);
    Files.createFile(blobIngestionDir.resolve(fileName));
  }

  @Test
  public void testSuccessfulBlobIngestion() throws IOException {
    // Set up manifest with SST files
    List<BlobFileMetadata> sstFiles = new ArrayList<>();
    sstFiles.add(createSstFileMetadata("000001.sst", 1024L, "checksum1", BlobTransferManifest.DEFAULT_COLUMN_FAMILY));
    sstFiles.add(
        createSstFileMetadata(
            "000002.sst",
            2048L,
            "checksum2",
            BlobTransferManifest.REPLICATION_METADATA_COLUMN_FAMILY));

    BlobTransferManifest manifest = createManifest(1000, sstFiles);

    // Create temp SST files so the existence check passes
    createTempSstFile("000001.sst");
    createTempSstFile("000002.sst");

    // Mock blob pull service
    when(blobPullService.isBlobAvailable(anyString())).thenReturn(true);
    when(blobPullService.pullBlobs(anyString(), anyString())).thenReturn(manifest);

    BlobIngestionTask task = new BlobIngestionTask(
        STORE_NAME,
        VERSION,
        PARTITION,
        BLOB_STAGING_PATH,
        localBaseDir,
        storeConfig,
        storageEngine,
        notifier);

    task.setBlobPullService(blobPullService);

    // Execute
    task.execute();

    // Verify
    assertTrue(task.isCompleted());
    verify(blobPullService, times(1)).isBlobAvailable(anyString());
    verify(blobPullService, times(1)).pullBlobs(anyString(), anyString());
    verify(storageEngine, times(1)).ingestExternalFiles(eq(PARTITION), anyList(), eq(false)); // Default CF
    verify(storageEngine, times(1)).ingestExternalFiles(eq(PARTITION), anyList(), eq(true)); // RMD CF
    verify(notifier, times(1)).completed(anyString(), eq(PARTITION), eq(null), anyString());
  }

  @Test
  public void testBlobNotAvailable() throws IOException {
    // Mock blob pull service - blobs not available
    when(blobPullService.isBlobAvailable(anyString())).thenReturn(false);

    BlobIngestionTask task = new BlobIngestionTask(
        STORE_NAME,
        VERSION,
        PARTITION,
        BLOB_STAGING_PATH,
        localBaseDir,
        storeConfig,
        storageEngine,
        notifier);

    task.setBlobPullService(blobPullService);

    // Execute - should throw exception
    try {
      task.execute();
      assertFalse(true, "Should have thrown VeniceException");
    } catch (VeniceException e) {
      // The exception is wrapped, so check the message contains "Blob ingestion failed"
      // and the cause contains the original error about blobs not being available
      assertTrue(e.getMessage().contains("Blob ingestion failed"));
      assertTrue(e.getCause() != null && e.getCause().getMessage().contains("Blob files not available"));
    }

    // Verify
    assertFalse(task.isCompleted());
    verify(blobPullService, times(1)).isBlobAvailable(anyString());
    verify(blobPullService, never()).pullBlobs(anyString(), anyString());
    verify(storageEngine, never()).ingestExternalFiles(anyInt(), anyList(), anyBoolean());
    verify(notifier, times(1)).error(anyString(), eq(PARTITION), anyString(), any());
  }

  @Test
  public void testEmptySstFiles() throws IOException {
    // Set up manifest with no SST files
    BlobTransferManifest manifest = createManifest(0, Collections.emptyList());

    // Mock blob pull service
    when(blobPullService.isBlobAvailable(anyString())).thenReturn(true);
    when(blobPullService.pullBlobs(anyString(), anyString())).thenReturn(manifest);

    BlobIngestionTask task = new BlobIngestionTask(
        STORE_NAME,
        VERSION,
        PARTITION,
        BLOB_STAGING_PATH,
        localBaseDir,
        storeConfig,
        storageEngine,
        notifier);

    task.setBlobPullService(blobPullService);

    // Execute
    task.execute();

    // Verify - should complete but no ingestion
    assertTrue(task.isCompleted());
    verify(storageEngine, never()).ingestExternalFiles(anyInt(), anyList(), anyBoolean());
    verify(notifier, times(1)).completed(anyString(), eq(PARTITION), eq(null), anyString());
  }

  @Test
  public void testPullFailure() throws IOException {
    // Mock blob pull service - pull fails
    when(blobPullService.isBlobAvailable(anyString())).thenReturn(true);
    when(blobPullService.pullBlobs(anyString(), anyString())).thenThrow(new IOException("Pull failed"));

    BlobIngestionTask task = new BlobIngestionTask(
        STORE_NAME,
        VERSION,
        PARTITION,
        BLOB_STAGING_PATH,
        localBaseDir,
        storeConfig,
        storageEngine,
        notifier);

    task.setBlobPullService(blobPullService);

    // Execute - should throw exception
    try {
      task.execute();
      assertFalse(true, "Should have thrown VeniceException");
    } catch (VeniceException e) {
      // The exception is wrapped with "Blob ingestion failed" message
      assertTrue(e.getMessage().contains("Blob ingestion failed"));
      // The cause should be the original IOException
      assertTrue(e.getCause() != null && e.getCause().getMessage().contains("Pull failed"));
    }

    // Verify
    assertFalse(task.isCompleted());
    verify(notifier, times(1)).error(anyString(), eq(PARTITION), anyString(), any());
  }

  @Test
  public void testNullNotifier() throws IOException {
    // Set up manifest with SST files
    List<BlobFileMetadata> sstFiles = new ArrayList<>();
    sstFiles.add(createSstFileMetadata("000001.sst", 1024L, "checksum1", BlobTransferManifest.DEFAULT_COLUMN_FAMILY));

    BlobTransferManifest manifest = createManifest(100, sstFiles);

    // Create temp SST file
    createTempSstFile("000001.sst");

    // Mock blob pull service
    when(blobPullService.isBlobAvailable(anyString())).thenReturn(true);
    when(blobPullService.pullBlobs(anyString(), anyString())).thenReturn(manifest);

    // Create task with null notifier
    BlobIngestionTask task = new BlobIngestionTask(
        STORE_NAME,
        VERSION,
        PARTITION,
        BLOB_STAGING_PATH,
        localBaseDir,
        storeConfig,
        storageEngine,
        null); // null notifier

    task.setBlobPullService(blobPullService);

    // Execute - should not throw NPE
    task.execute();

    // Verify
    assertTrue(task.isCompleted());
    verify(storageEngine, times(1)).ingestExternalFiles(eq(PARTITION), anyList(), eq(false));
  }

  @Test
  public void testClose() throws IOException {
    BlobIngestionTask task = new BlobIngestionTask(
        STORE_NAME,
        VERSION,
        PARTITION,
        BLOB_STAGING_PATH,
        localBaseDir,
        storeConfig,
        storageEngine,
        notifier);

    task.setBlobPullService(blobPullService);

    // Close should not throw
    task.close();

    verify(blobPullService, times(1)).close();
  }

  @Test
  public void testMissingSstFilesAreSkipped() throws IOException {
    // Set up manifest with SST files but don't create them on disk
    List<BlobFileMetadata> sstFiles = new ArrayList<>();
    sstFiles.add(createSstFileMetadata("missing.sst", 1024L, "checksum1", BlobTransferManifest.DEFAULT_COLUMN_FAMILY));

    BlobTransferManifest manifest = createManifest(100, sstFiles);

    // Mock blob pull service
    when(blobPullService.isBlobAvailable(anyString())).thenReturn(true);
    when(blobPullService.pullBlobs(anyString(), anyString())).thenReturn(manifest);

    BlobIngestionTask task = new BlobIngestionTask(
        STORE_NAME,
        VERSION,
        PARTITION,
        BLOB_STAGING_PATH,
        localBaseDir,
        storeConfig,
        storageEngine,
        notifier);

    task.setBlobPullService(blobPullService);

    // Execute - should complete without error even though files are missing
    task.execute();

    // Verify - should complete but no ingestion since files don't exist
    assertTrue(task.isCompleted());
    verify(storageEngine, never()).ingestExternalFiles(anyInt(), anyList(), anyBoolean());
    verify(notifier, times(1)).completed(anyString(), eq(PARTITION), eq(null), anyString());
  }

  @Test
  public void testNoBlobPullServiceThrowsException() {
    // Create task without setting blob pull service
    BlobIngestionTask task = new BlobIngestionTask(
        STORE_NAME,
        VERSION,
        PARTITION,
        BLOB_STAGING_PATH,
        localBaseDir,
        storeConfig,
        storageEngine,
        notifier);

    // Don't set blob pull service

    // Execute - should throw exception
    try {
      task.execute();
      assertFalse(true, "Should have thrown VeniceException");
    } catch (VeniceException e) {
      assertTrue(e.getMessage().contains("BlobPullService must be set"));
    }

    assertFalse(task.isCompleted());
  }

  @Test
  public void testGetters() {
    BlobIngestionTask task = new BlobIngestionTask(
        STORE_NAME,
        VERSION,
        PARTITION,
        BLOB_STAGING_PATH,
        localBaseDir,
        storeConfig,
        storageEngine,
        notifier);

    // Verify getters
    assertTrue(STORE_NAME.equals(task.getStoreName()));
    assertTrue(VERSION == task.getVersion());
    assertTrue(PARTITION == task.getPartition());
    assertFalse(task.isCompleted());
  }
}
