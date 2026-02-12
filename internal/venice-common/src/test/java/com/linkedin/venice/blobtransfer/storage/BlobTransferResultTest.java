package com.linkedin.venice.blobtransfer.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;


public class BlobTransferResultTest {
  @Test
  public void testSuccessResult() {
    BlobTransferResult result = new BlobTransferResult("/path/to/file", 1024L, true, null);
    assertEquals(result.getPath(), "/path/to/file");
    assertEquals(result.getBytesCopied(), 1024L);
    assertTrue(result.isSuccess());
    assertNull(result.getErrorMessage());
  }

  @Test
  public void testFailureResult() {
    BlobTransferResult result = new BlobTransferResult("/path/to/file", 0L, false, "File not found");
    assertEquals(result.getPath(), "/path/to/file");
    assertEquals(result.getBytesCopied(), 0L);
    assertFalse(result.isSuccess());
    assertEquals(result.getErrorMessage(), "File not found");
  }
}
