package com.linkedin.venice.blobtransfer.storage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.expectThrows;

import org.testng.annotations.Test;


public class BlobStorageTypeTest {
  @Test
  public void testFromStringValid() {
    assertEquals(BlobStorageType.fromString("HDFS"), BlobStorageType.HDFS);
    assertEquals(BlobStorageType.fromString("S3"), BlobStorageType.S3);
    assertEquals(BlobStorageType.fromString("LOCAL_FS"), BlobStorageType.LOCAL_FS);
  }

  @Test
  public void testFromStringCaseInsensitive() {
    assertEquals(BlobStorageType.fromString("hdfs"), BlobStorageType.HDFS);
    assertEquals(BlobStorageType.fromString("s3"), BlobStorageType.S3);
    assertEquals(BlobStorageType.fromString("local_fs"), BlobStorageType.LOCAL_FS);
  }

  @Test
  public void testFromStringInvalid() {
    expectThrows(IllegalArgumentException.class, () -> BlobStorageType.fromString("INVALID"));
  }
}
