package com.linkedin.davinci.kafka.consumer;

import com.linkedin.davinci.validation.PartitionTracker;
import com.linkedin.venice.pubsub.api.DefaultPubSubMessage;
import com.linkedin.venice.pubsub.api.PubSubTopicPartition;
import com.linkedin.venice.service.AbstractVeniceService;
import java.util.concurrent.CompletableFuture;


/**
 * Abstract class capturing the responsibilities of drainers threads doing store ingestion.
 */
public abstract class AbstractStoreBufferService extends AbstractVeniceService {
  public abstract void putConsumerRecord(
      DefaultPubSubMessage consumerRecord,
      StoreIngestionTask ingestionTask,
      LeaderProducedRecordContext leaderProducedRecordContext,
      int partition,
      String kafkaUrl,
      long beforeProcessingRecordTimestampNs) throws InterruptedException;

  /**
   * This method will wait for all the messages to be processed (persisted to disk) that are already
   * queued up to drainer till now.
   */
  public abstract void drainBufferedRecordsFromTopicPartition(PubSubTopicPartition topicPartition)
      throws InterruptedException;

  public abstract long getTotalMemoryUsage();

  public abstract long getTotalRemainingMemory();

  public abstract long getMaxMemoryUsagePerDrainer();

  public abstract long getMinMemoryUsagePerDrainer();

  /**
   * Returns the drainer index that would be used for the given topic partition.
   * This allows callers to check drainer capacity before records are queued.
   */
  public abstract int getDrainerIndexForTopicPartition(PubSubTopicPartition topicPartition);

  /**
   * Returns the fraction of remaining capacity for the given drainer index (0.0 = full, 1.0 = empty).
   */
  public abstract double getRemainingCapacityFraction(int drainerIndex);

  public abstract CompletableFuture<Void> execSyncOffsetCommandAsync(
      PubSubTopicPartition topicPartition,
      StoreIngestionTask ingestionTask) throws InterruptedException;

  public abstract void execSyncOffsetFromSnapshotAsync(
      PubSubTopicPartition topicPartition,
      PartitionTracker vtDivSnapshot,
      CompletableFuture<Void> lastRecordPersistedFuture,
      StoreIngestionTask ingestionTask) throws InterruptedException;
}
