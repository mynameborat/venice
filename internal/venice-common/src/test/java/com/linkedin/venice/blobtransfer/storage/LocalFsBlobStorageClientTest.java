package com.linkedin.venice.blobtransfer.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class LocalFsBlobStorageClientTest {
  private LocalFsBlobStorageClient client;
  private Path tempDir;

  @BeforeMethod
  public void setUp() throws IOException {
    client = new LocalFsBlobStorageClient();
    tempDir = Files.createTempDirectory("blob-storage-test");
  }

  @AfterMethod
  public void tearDown() throws IOException {
    client.deleteDirectory(tempDir.toString());
    client.close();
  }

  @Test
  public void testUploadAndDownload() throws IOException {
    // Create a source file
    Path srcFile = tempDir.resolve("source.txt");
    Files.write(srcFile, "hello world".getBytes());

    // Upload
    Path remotePath = tempDir.resolve("remote/data.txt");
    BlobTransferResult uploadResult = client.upload(srcFile.toString(), remotePath.toString());
    assertTrue(uploadResult.isSuccess());
    assertEquals(uploadResult.getPath(), remotePath.toString());
    assertTrue(uploadResult.getBytesCopied() > 0);
    assertNull(uploadResult.getErrorMessage());
    assertTrue(Files.exists(remotePath));

    // Download
    Path localPath = tempDir.resolve("downloaded.txt");
    BlobTransferResult downloadResult = client.download(remotePath.toString(), localPath.toString());
    assertTrue(downloadResult.isSuccess());
    assertEquals(downloadResult.getPath(), localPath.toString());
    assertTrue(downloadResult.getBytesCopied() > 0);
    assertTrue(Files.exists(localPath));
    assertEquals(new String(Files.readAllBytes(localPath)), "hello world");
  }

  @Test
  public void testExists() throws IOException {
    Path file = tempDir.resolve("exists-test.txt");
    assertFalse(client.exists(file.toString()));
    Files.write(file, "data".getBytes());
    assertTrue(client.exists(file.toString()));
  }

  @Test
  public void testDelete() throws IOException {
    Path file = tempDir.resolve("delete-test.txt");
    Files.write(file, "data".getBytes());
    assertTrue(Files.exists(file));

    client.delete(file.toString());
    assertFalse(Files.exists(file));
  }

  @Test
  public void testDeleteNonExistent() throws IOException {
    // Should not throw
    client.delete(tempDir.resolve("nonexistent.txt").toString());
  }

  @Test
  public void testDeleteDirectory() throws IOException {
    Path subDir = tempDir.resolve("subdir");
    Files.createDirectories(subDir);
    Files.write(subDir.resolve("a.txt"), "aaa".getBytes());
    Files.write(subDir.resolve("b.txt"), "bbb".getBytes());
    Path nested = subDir.resolve("nested");
    Files.createDirectories(nested);
    Files.write(nested.resolve("c.txt"), "ccc".getBytes());

    assertTrue(Files.exists(subDir));
    client.deleteDirectory(subDir.toString());
    assertFalse(Files.exists(subDir));
  }

  @Test
  public void testDeleteDirectoryNonExistent() throws IOException {
    // Should not throw
    client.deleteDirectory(tempDir.resolve("nonexistent-dir").toString());
  }
}
