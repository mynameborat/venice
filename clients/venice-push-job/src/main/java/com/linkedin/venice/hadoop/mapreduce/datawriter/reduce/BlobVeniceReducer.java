package com.linkedin.venice.hadoop.mapreduce.datawriter.reduce;

import com.linkedin.venice.hadoop.mapreduce.datawriter.task.ReporterBackedMapReduceDataWriterTaskTracker;
import com.linkedin.venice.hadoop.mapreduce.engine.MapReduceEngineTaskConfigProvider;
import com.linkedin.venice.hadoop.task.datawriter.BlobPartitionWriter;
import com.linkedin.venice.hadoop.task.datawriter.DataWriterTaskTracker;
import com.linkedin.venice.utils.IteratorUtils;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;


/**
 * BlobVeniceReducer writes data directly to RocksDB SST files instead of Kafka.
 * This reducer is used for blob-based push jobs where SST files are generated
 * in MR reducers and then transferred to servers via blob transfer.
 *
 * The key difference from VeniceReducer:
 * - Extends BlobPartitionWriter instead of AbstractPartitionWriter
 * - Writes to local SST files which are uploaded to HDFS on close()
 * - Does not require Kafka connectivity
 */
public class BlobVeniceReducer extends BlobPartitionWriter
    implements Reducer<BytesWritable, BytesWritable, BytesWritable, BytesWritable> {
  private Reporter reporter = null;
  private JobConf jobConf;

  protected JobConf getJobConf() {
    return jobConf;
  }

  @Override
  public void configure(JobConf job) {
    this.jobConf = job;
    super.configure(new MapReduceEngineTaskConfigProvider(job));
  }

  @Override
  public void reduce(
      BytesWritable key,
      Iterator<BytesWritable> values,
      OutputCollector<BytesWritable, BytesWritable> output,
      Reporter reporter) throws IOException {
    DataWriterTaskTracker dataWriterTaskTracker;
    if (updatePreviousReporter(reporter)) {
      dataWriterTaskTracker = new ReporterBackedMapReduceDataWriterTaskTracker(reporter);
    } else {
      dataWriterTaskTracker = getDataWriterTaskTracker();
    }

    processValuesForKey(
        key.copyBytes(),
        IteratorUtils.mapIterator(values, x -> new VeniceRecordWithMetadata(x.copyBytes(), null)),
        dataWriterTaskTracker);
  }

  private boolean updatePreviousReporter(Reporter reporter) {
    if (this.reporter == null || !this.reporter.equals(reporter)) {
      this.reporter = reporter;
      return true;
    }
    return false;
  }

  @Override
  protected DataWriterTaskTracker getDataWriterTaskTracker() {
    return super.getDataWriterTaskTracker();
  }
}
