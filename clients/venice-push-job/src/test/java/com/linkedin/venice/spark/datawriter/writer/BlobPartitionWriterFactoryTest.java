package com.linkedin.venice.spark.datawriter.writer;

import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_STORAGE_TYPE;

import com.linkedin.venice.blobtransfer.storage.BlobStorageClient;
import com.linkedin.venice.blobtransfer.storage.LocalFsBlobStorageClient;
import com.linkedin.venice.exceptions.VeniceException;
import java.util.Properties;
import org.testng.Assert;
import org.testng.annotations.Test;


public class BlobPartitionWriterFactoryTest {
  @Test
  public void testCreateBlobStorageClient_localFs() {
    Properties props = new Properties();
    props.setProperty(BLOB_STORAGE_TYPE, "LOCAL_FS");

    BlobStorageClient client = BlobPartitionWriterFactory.createBlobStorageClient(props);
    Assert.assertNotNull(client);
    Assert.assertTrue(client instanceof LocalFsBlobStorageClient);
  }

  @Test
  public void testCreateBlobStorageClient_localFs_caseInsensitive() {
    Properties props = new Properties();
    props.setProperty(BLOB_STORAGE_TYPE, "local_fs");

    BlobStorageClient client = BlobPartitionWriterFactory.createBlobStorageClient(props);
    Assert.assertNotNull(client);
    Assert.assertTrue(client instanceof LocalFsBlobStorageClient);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*not yet implemented.*")
  public void testCreateBlobStorageClient_hdfs_throwsNotImplemented() {
    Properties props = new Properties();
    props.setProperty(BLOB_STORAGE_TYPE, "HDFS");

    BlobPartitionWriterFactory.createBlobStorageClient(props);
  }

  @Test(expectedExceptions = VeniceException.class, expectedExceptionsMessageRegExp = ".*not yet implemented.*")
  public void testCreateBlobStorageClient_s3_throwsNotImplemented() {
    Properties props = new Properties();
    props.setProperty(BLOB_STORAGE_TYPE, "S3");

    BlobPartitionWriterFactory.createBlobStorageClient(props);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCreateBlobStorageClient_invalidType() {
    Properties props = new Properties();
    props.setProperty(BLOB_STORAGE_TYPE, "UNKNOWN_TYPE");

    BlobPartitionWriterFactory.createBlobStorageClient(props);
  }
}
