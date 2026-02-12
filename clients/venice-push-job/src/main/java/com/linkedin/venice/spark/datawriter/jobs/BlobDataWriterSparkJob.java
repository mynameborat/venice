package com.linkedin.venice.spark.datawriter.jobs;

import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_SST_TABLE_FORMAT;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_STORAGE_BASE_URI;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_STORAGE_TYPE;

import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.spark.datawriter.task.DataWriterAccumulators;
import com.linkedin.venice.spark.datawriter.writer.BlobPartitionWriterFactory;
import com.linkedin.venice.utils.VeniceProperties;
import java.util.Properties;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;


/**
 * Spark job subclass for blob-based push. Extends {@link DataWriterSparkJob} to inherit
 * the input reading logic ({@code getUserInputDataFrame()}) while overriding the output path:
 * instead of writing to Kafka via VeniceWriter, this job writes RocksDB SST files and uploads
 * them to blob storage.
 *
 * <p>Overrides:
 * <ul>
 *   <li>{@link #configure} — injects blob config keys into SparkSession conf so they
 *       propagate to executor Properties via the existing broadcast mechanism</li>
 *   <li>{@link #createPartitionWriterFactory} — returns a {@link BlobPartitionWriterFactory}
 *       instead of {@code SparkPartitionWriterFactory}</li>
 * </ul>
 */
public class BlobDataWriterSparkJob extends DataWriterSparkJob {
  @Override
  public void configure(VeniceProperties props, PushJobSetting pushJobSetting) {
    super.configure(props, pushJobSetting);

    // Inject blob-specific config into SparkSession conf so it propagates to executors
    // via the broadcast Properties in runComputeJob()
    // blobStorageUri and blobStorageType are always set for blob-based pushes (from version metadata).
    // blobSstTableFormat may be null if the controller does not set it; hence the null guard.
    getSparkSession().conf().set(BLOB_STORAGE_BASE_URI, pushJobSetting.blobStorageUri);
    getSparkSession().conf().set(BLOB_STORAGE_TYPE, pushJobSetting.blobStorageType);
    if (pushJobSetting.blobSstTableFormat != null) {
      getSparkSession().conf().set(BLOB_SST_TABLE_FORMAT, pushJobSetting.blobSstTableFormat);
    }
  }

  @Override
  protected MapPartitionsFunction<Row, Row> createPartitionWriterFactory(
      Broadcast<Properties> broadcastProperties,
      DataWriterAccumulators accumulators) {
    return new BlobPartitionWriterFactory(broadcastProperties, accumulators);
  }
}
