package com.linkedin.venice.hadoop.mapreduce.datawriter.jobs;

import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_PUSH_LOCAL_TEMP_DIR;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_PUSH_MAX_RECORDS_PER_SST_FILE;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_PUSH_MAX_SST_FILE_SIZE_BYTES;
import static com.linkedin.venice.vpj.VenicePushJobConstants.BLOB_PUSH_STAGING_PATH;
import static com.linkedin.venice.vpj.VenicePushJobConstants.PARTITION_COUNT;

import com.linkedin.venice.hadoop.PushJobSetting;
import com.linkedin.venice.hadoop.mapreduce.datawriter.reduce.BlobVeniceReducer;
import com.linkedin.venice.utils.VeniceProperties;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * MapReduce job for blob-based Venice Push Job.
 * This job generates SST files directly in reducers and uploads them to HDFS
 * instead of writing to Kafka.
 *
 * Key differences from DataWriterMRJob:
 * - Uses BlobVeniceReducer instead of VeniceReducer
 * - Passes blob push specific configuration (staging path, SST file limits)
 * - Does not require Kafka producer configuration
 */
public class BlobDataWriterMRJob extends DataWriterMRJob {
  private static final Logger LOGGER = LogManager.getLogger(BlobDataWriterMRJob.class);

  private VeniceProperties vpjProperties;
  private PushJobSetting pushJobSetting;

  @Override
  public void configure(VeniceProperties props, PushJobSetting pushJobSetting) {
    this.vpjProperties = props;
    this.pushJobSetting = pushJobSetting;
    super.configure(props, pushJobSetting);
  }

  @Override
  void setupMRConf(JobConf jobConf, PushJobSetting pushJobSetting, VeniceProperties props) {
    // Call parent to set up common configuration
    super.setupMRConf(jobConf, pushJobSetting, props);

    // Override reducer to use BlobVeniceReducer
    setupBlobReducerConf(jobConf);

    // Set blob-specific configuration
    setupBlobPushConf(jobConf, pushJobSetting);
  }

  private void setupBlobReducerConf(JobConf jobConf) {
    // Override the reducer class to use BlobVeniceReducer
    jobConf.setReducerClass(BlobVeniceReducer.class);

    // Output format is not needed since we're writing to HDFS directly
    // The output collector is not used by BlobVeniceReducer
    jobConf.setMapOutputKeyClass(BytesWritable.class);
    jobConf.setMapOutputValueClass(BytesWritable.class);

    LOGGER.info("Configured blob-based MR job with BlobVeniceReducer");
  }

  private void setupBlobPushConf(JobConf jobConf, PushJobSetting pushJobSetting) {
    // Set HDFS staging path for blob push
    String stagingPath = pushJobSetting.blobPushStagingPath;
    if (stagingPath != null && !stagingPath.isEmpty()) {
      jobConf.set(BLOB_PUSH_STAGING_PATH, stagingPath);
    }

    // Set optional blob push configuration
    if (vpjProperties.containsKey(BLOB_PUSH_LOCAL_TEMP_DIR)) {
      jobConf.set(BLOB_PUSH_LOCAL_TEMP_DIR, vpjProperties.getString(BLOB_PUSH_LOCAL_TEMP_DIR));
    }

    if (vpjProperties.containsKey(BLOB_PUSH_MAX_SST_FILE_SIZE_BYTES)) {
      jobConf.setLong(BLOB_PUSH_MAX_SST_FILE_SIZE_BYTES, vpjProperties.getLong(BLOB_PUSH_MAX_SST_FILE_SIZE_BYTES));
    }

    if (vpjProperties.containsKey(BLOB_PUSH_MAX_RECORDS_PER_SST_FILE)) {
      jobConf.setLong(BLOB_PUSH_MAX_RECORDS_PER_SST_FILE, vpjProperties.getLong(BLOB_PUSH_MAX_RECORDS_PER_SST_FILE));
    }

    LOGGER.info(
        "Configured blob push with staging path: {}, partition count: {}",
        stagingPath,
        jobConf.getInt(PARTITION_COUNT, -1));
  }

  @Override
  public void runComputeJob() {
    LOGGER.info(
        "Starting blob-based MR job for store: {}, version: {}",
        pushJobSetting.storeName,
        pushJobSetting.version);
    super.runComputeJob();
    LOGGER.info(
        "Completed blob-based MR job for store: {}, version: {}. "
            + "SST files should now be in HDFS staging directory: {}",
        pushJobSetting.storeName,
        pushJobSetting.version,
        pushJobSetting.blobPushStagingPath);
  }
}
