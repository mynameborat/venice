package com.linkedin.venice.client.store;

import com.linkedin.venice.client.exceptions.VeniceClientException;
import com.linkedin.venice.client.stats.ClientStats;
import com.linkedin.venice.client.store.streaming.StreamingCallback;
import com.linkedin.venice.compute.ComputeRequestWrapper;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import org.apache.avro.generic.GenericRecord;


/**
 * This class includes some necessary functions to deal with certain metric-handling activities that only
 * the client implementation can be aware of. These metrics cannot be tracked from a purely-external
 * perspective (i.e.: from the {@link com.linkedin.venice.client.store.StatTrackingStoreClient}'s point of view).
 *
 * It is intentional for these functions to not be part of {@link AvroGenericStoreClient}, so that the
 * end-user does not see these extra functions on the instances they get back from the
 * {@link com.linkedin.venice.client.store.ClientFactory}.
 */
public abstract class InternalAvroStoreClient<K, V> implements AvroGenericReadComputeStoreClient<K, V> {
  public CompletableFuture<byte[]> getRaw(String requestPath) {
    return getRaw(requestPath, Optional.empty(), 0);
  }

  @Override
  public CompletableFuture<V> get(K key) throws VeniceClientException {
    return get(key, Optional.empty(), 0);
  }

  public abstract CompletableFuture<V> get(K key, Optional<ClientStats> stats, long preRequestTimeInNS)
      throws VeniceClientException;

  public abstract CompletableFuture<byte[]> getRaw(
      String requestPath,
      Optional<ClientStats> stats,
      long preRequestTimeInNS);

  public Executor getDeserializationExecutor() {
    throw new VeniceClientException("getDeserializationExecutor is not supported!");
  }

  @Override
  public void computeWithKeyPrefixFilter(
      byte[] keyPrefix,
      ComputeRequestWrapper computeRequestWrapper,
      StreamingCallback<GenericRecord, GenericRecord> callback) {
    throw new VeniceClientException("ComputeWithKeyPrefixFilter is not supported by Venice Avro Store Client");
  }

  /**
   * This method is mainly for internal use.
   * The default {#start()} method will not throw an exception if the client fails to start since it is a best
   * effort to make it compatible with the existing usage of the client (customers can trigger the start() method
   * even before the dependency is ready).
   * This method is mainly used to the internal startupAware callback, and it will indicate the startup failure
   * by throwing an exception.
   */
  public abstract void startWithExceptionThrownWhenFail();
}
