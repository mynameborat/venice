package com.linkedin.venice.blobtransfer.storage;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;


public class BlobStoragePathsTest {
  private static final String BASE_URI = "hdfs:///venice/blob";
  private static final String STORE_NAME = "myStore";
  private static final int VERSION = 3;
  private static final int PARTITION = 7;

  @Test
  public void testVersionDir() {
    assertEquals(BlobStoragePaths.versionDir(BASE_URI, STORE_NAME, VERSION), "hdfs:///venice/blob/myStore/v3");
  }

  @Test
  public void testPartitionDir() {
    assertEquals(
        BlobStoragePaths.partitionDir(BASE_URI, STORE_NAME, VERSION, PARTITION),
        "hdfs:///venice/blob/myStore/v3/p7");
  }

  @Test
  public void testSstFile() {
    assertEquals(
        BlobStoragePaths.sstFile(BASE_URI, STORE_NAME, VERSION, PARTITION, "data_0.sst"),
        "hdfs:///venice/blob/myStore/v3/p7/data_0.sst");
  }

  @Test
  public void testPartitionManifest() {
    assertEquals(
        BlobStoragePaths.partitionManifest(BASE_URI, STORE_NAME, VERSION, PARTITION),
        "hdfs:///venice/blob/myStore/v3/p7/manifest.json");
  }

  @Test
  public void testVersionManifest() {
    assertEquals(
        BlobStoragePaths.versionManifest(BASE_URI, STORE_NAME, VERSION),
        "hdfs:///venice/blob/myStore/v3/manifest.json");
  }
}
