package com.linkedin.venice.spark.datawriter.writer;

import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_STORAGE_TYPE;

import com.linkedin.venice.blobtransfer.storage.BlobStorageClient;
import com.linkedin.venice.blobtransfer.storage.BlobStorageType;
import com.linkedin.venice.blobtransfer.storage.LocalFsBlobStorageClient;
import com.linkedin.venice.exceptions.VeniceException;
import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;


/**
 * Factory that creates a {@link BlobPartitionWriter} for each Spark partition.
 * Mirrors the {@link SparkPartitionWriterFactory} pattern but produces SST files
 * instead of writing to Kafka.
 *
 * <p>The {@link BlobStorageClient} is created inside {@link #call(Iterator)} (not as a field)
 * to avoid serialization issues with Spark's task distribution.
 */
public class BlobPartitionWriterFactory implements MapPartitionsFunction<Row, Row> {
  private static final long serialVersionUID = 1L;
  private final Broadcast<Properties> jobProps;
  private final DataWriterAccumulators accumulators;

  public BlobPartitionWriterFactory(Broadcast<Properties> jobProps, DataWriterAccumulators accumulators) {
    this.jobProps = jobProps;
    this.accumulators = accumulators;
  }

  @Override
  public Iterator<Row> call(Iterator<Row> rows) throws Exception {
    Properties properties = jobProps.getValue();
    BlobStorageClient blobStorageClient = createBlobStorageClient(properties);
    try (BlobPartitionWriter partitionWriter = new BlobPartitionWriter(properties, accumulators, blobStorageClient)) {
      partitionWriter.processRows(rows);
    } finally {
      blobStorageClient.close();
    }
    return Collections.emptyIterator();
  }

  static BlobStorageClient createBlobStorageClient(Properties properties) {
    String typeStr = properties.getProperty(BLOB_STORAGE_TYPE);
    BlobStorageType type = BlobStorageType.fromString(typeStr);
    switch (type) {
      case LOCAL_FS:
        return new LocalFsBlobStorageClient();
      case HDFS:
      case S3:
        throw new VeniceException("BlobStorageClient for type " + type + " is not yet implemented");
      default:
        throw new VeniceException("Unknown blob storage type: " + type);
    }
  }
}
