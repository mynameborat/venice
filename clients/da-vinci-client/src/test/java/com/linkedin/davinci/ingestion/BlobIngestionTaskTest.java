package com.linkedin.davinci.ingestion;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;

import com.linkedin.davinci.config.VeniceServerConfig;
import com.linkedin.davinci.config.VeniceStoreVersionConfig;
import com.linkedin.davinci.notifier.VeniceNotifier;
import com.linkedin.davinci.storage.StorageService;
import com.linkedin.davinci.store.StorageEngine;
import com.linkedin.venice.blobtransfer.storage.BlobStorageType;
import com.linkedin.venice.kafka.protocol.state.StoreVersionState;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.pushmonitor.PushControlSignal;
import com.linkedin.venice.pushmonitor.PushControlSignalAccessor;
import com.linkedin.venice.pushmonitor.PushControlSignalType;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class BlobIngestionTaskTest {
  private static final String STORE_NAME = "testStore";
  private static final int VERSION_NUMBER = 1;
  private static final int PARTITION = 0;
  private static final String KAFKA_TOPIC = Version.composeKafkaTopic(STORE_NAME, VERSION_NUMBER);
  private static final String BLOB_STORAGE_URI = "/tmp/venice-blob";

  @Mock
  private PushControlSignalAccessor pushControlSignalAccessor;
  @Mock
  private StorageService storageService;
  @Mock
  private VeniceStoreVersionConfig storeConfig;
  @Mock
  private VeniceServerConfig serverConfig;
  @Mock
  private StorageEngine storageEngine;

  private Queue<VeniceNotifier> notifiers;
  private Supplier<StoreVersionState> svsSupplier;

  @BeforeMethod
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    notifiers = new ConcurrentLinkedQueue<>();
    svsSupplier = () -> mock(StoreVersionState.class);

    when(storeConfig.getStoreVersionName()).thenReturn(KAFKA_TOPIC);
    when(serverConfig.getBlobIngestionDownloadMaxRetries()).thenReturn(3);
    when(serverConfig.getBlobIngestionPollIntervalMs()).thenReturn(10L); // Short poll for tests
    when(serverConfig.getRocksDBPath()).thenReturn("/tmp/rocksdb");
    when(serverConfig.getBlobIngestionDownloadJitterMaxMs()).thenReturn(0L); // No jitter by default in tests
    when(serverConfig.getBlobIngestionMaxConcurrentDownloads()).thenReturn(8);
  }

  @Test
  public void testWaitForBlobUploadComplete_signalAlreadyPresent() throws InterruptedException {
    PushControlSignal signal = new PushControlSignal(KAFKA_TOPIC);
    signal.emitSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE);
    when(pushControlSignalAccessor.getPushControlSignal(KAFKA_TOPIC)).thenReturn(signal);

    BlobIngestionTask task = createTask();
    task.waitForBlobUploadComplete();

    // Should subscribe, find signal immediately, then unsubscribe
    verify(pushControlSignalAccessor).subscribePushControlSignalChange(eq(KAFKA_TOPIC), any(IZkDataListener.class));
    verify(pushControlSignalAccessor).unsubscribePushControlSignalChange(eq(KAFKA_TOPIC), any(IZkDataListener.class));
    // Immediate check finds signal
    verify(pushControlSignalAccessor).getPushControlSignal(KAFKA_TOPIC);
  }

  @Test
  public void testWaitForBlobUploadComplete_signalAppearsViaWatcher() throws InterruptedException {
    PushControlSignal noSignal = new PushControlSignal(KAFKA_TOPIC);
    PushControlSignal withSignal = new PushControlSignal(KAFKA_TOPIC);
    withSignal.emitSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE);

    // First call (immediate check) returns no signal; subsequent calls return signal
    when(pushControlSignalAccessor.getPushControlSignal(KAFKA_TOPIC)).thenReturn(noSignal).thenReturn(withSignal);

    // Capture the listener and simulate a ZK data change
    AtomicReference<IZkDataListener> capturedListener = new AtomicReference<>();
    doAnswer(invocation -> {
      capturedListener.set(invocation.getArgument(1));
      return null;
    }).when(pushControlSignalAccessor).subscribePushControlSignalChange(eq(KAFKA_TOPIC), any(IZkDataListener.class));

    BlobIngestionTask task = createTask();

    // Run in a separate thread since waitForBlobUploadComplete blocks
    Thread waitThread = new Thread(() -> {
      try {
        task.waitForBlobUploadComplete();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    waitThread.start();

    // Wait for the listener to be registered, then simulate ZK data change
    while (capturedListener.get() == null) {
      Thread.sleep(5);
    }
    try {
      capturedListener.get().handleDataChange("", null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    waitThread.join(5000);
    assertFalse(waitThread.isAlive(), "waitThread should have completed after watcher fired");

    verify(pushControlSignalAccessor).subscribePushControlSignalChange(eq(KAFKA_TOPIC), any(IZkDataListener.class));
    verify(pushControlSignalAccessor).unsubscribePushControlSignalChange(eq(KAFKA_TOPIC), any(IZkDataListener.class));
  }

  @Test
  public void testWaitForBlobUploadComplete_unsubscribesOnInterrupt() throws InterruptedException {
    // Signal never arrives
    when(pushControlSignalAccessor.getPushControlSignal(KAFKA_TOPIC)).thenReturn(null);

    BlobIngestionTask task = createTask();

    Thread waitThread = new Thread(() -> {
      try {
        task.waitForBlobUploadComplete();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
    waitThread.start();

    // Give it time to subscribe and start waiting, then interrupt
    Thread.sleep(50);
    waitThread.interrupt();
    waitThread.join(5000);
    assertFalse(waitThread.isAlive(), "waitThread should have completed after interrupt");

    // Should always unsubscribe, even on interruption
    verify(pushControlSignalAccessor).subscribePushControlSignalChange(eq(KAFKA_TOPIC), any(IZkDataListener.class));
    verify(pushControlSignalAccessor).unsubscribePushControlSignalChange(eq(KAFKA_TOPIC), any(IZkDataListener.class));
  }

  @Test
  public void testReportStatusOrder() {
    // Set up signal so waitForBlobUploadComplete returns immediately
    PushControlSignal signal = new PushControlSignal(KAFKA_TOPIC);
    signal.emitSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE);
    when(pushControlSignalAccessor.getPushControlSignal(KAFKA_TOPIC)).thenReturn(signal);

    // Add a mock notifier to track calls
    VeniceNotifier notifier = mock(VeniceNotifier.class);
    notifiers.add(notifier);

    // Override downloadSSTFiles and ingestSSTFiles to avoid RocksDB dependency
    BlobIngestionTask task = createTaskWithNoOpDownloadAndIngest();

    task.run();

    // Verify notifier was called in order: started -> endOfPushReceived -> completed
    InOrder order = inOrder(notifier);
    order.verify(notifier).started(KAFKA_TOPIC, PARTITION);
    order.verify(notifier).endOfPushReceived(KAFKA_TOPIC, PARTITION, null);
    order.verify(notifier).completed(KAFKA_TOPIC, PARTITION, null);
  }

  @Test
  public void testReportError_onFailure() {
    // Set up signal to be present
    PushControlSignal signal = new PushControlSignal(KAFKA_TOPIC);
    signal.emitSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE);
    when(pushControlSignalAccessor.getPushControlSignal(KAFKA_TOPIC)).thenReturn(signal);

    VeniceNotifier notifier = mock(VeniceNotifier.class);
    notifiers.add(notifier);

    // Override downloadSSTFiles to throw, simulating a download failure
    BlobIngestionTask task = new BlobIngestionTask(
        STORE_NAME,
        VERSION_NUMBER,
        PARTITION,
        BLOB_STORAGE_URI,
        BlobStorageType.LOCAL_FS,
        pushControlSignalAccessor,
        storageService,
        storeConfig,
        svsSupplier,
        notifiers,
        serverConfig,
        null,
        0L,
        null,
        null) {
      @Override
      List<String> downloadSSTFiles() {
        throw new RuntimeException("Download failed");
      }
    };

    task.run();

    // Should report started, then error (not completed)
    verify(notifier).started(KAFKA_TOPIC, PARTITION);
    verify(notifier).error(eq(KAFKA_TOPIC), eq(PARTITION), anyString(), any(Exception.class));
    verify(notifier, never()).completed(eq(KAFKA_TOPIC), eq(PARTITION), any());
  }

  @Test
  public void testApplyDownloadJitter_delayWithinRange() throws InterruptedException {
    long jitterMaxMs = 200;
    BlobIngestionTask task = createTaskWithJitter(jitterMaxMs);

    long start = System.currentTimeMillis();
    task.applyDownloadJitter();
    long elapsed = System.currentTimeMillis() - start;

    // Jitter should be in [0, jitterMaxMs). Allow some slack for scheduling.
    assert elapsed < jitterMaxMs + 50 : "Jitter delay " + elapsed + " ms exceeded max " + jitterMaxMs;
  }

  @Test
  public void testApplyDownloadJitter_zeroSkipsSleep() throws InterruptedException {
    BlobIngestionTask task = createTaskWithJitter(0L);

    long start = System.currentTimeMillis();
    task.applyDownloadJitter();
    long elapsed = System.currentTimeMillis() - start;

    // With jitter=0, should return immediately (well under 50ms)
    assert elapsed < 50 : "Jitter=0 should not sleep, but took " + elapsed + " ms";
  }

  @Test
  public void testDownloadWithConcurrencyLimit_respectsSemaphore() throws Exception {
    Semaphore semaphore = new Semaphore(2);
    AtomicInteger concurrentDownloads = new AtomicInteger(0);
    AtomicInteger maxConcurrentDownloads = new AtomicInteger(0);
    CountDownLatch allStarted = new CountDownLatch(3);
    CountDownLatch gate = new CountDownLatch(1);

    // Set up signal so waitForBlobUploadComplete returns immediately
    PushControlSignal signal = new PushControlSignal(KAFKA_TOPIC);
    signal.emitSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE);
    when(pushControlSignalAccessor.getPushControlSignal(KAFKA_TOPIC)).thenReturn(signal);

    // Create 3 tasks that block during download until we release the gate
    Thread[] threads = new Thread[3];
    for (int i = 0; i < 3; i++) {
      BlobIngestionTask task = new BlobIngestionTask(
          STORE_NAME,
          VERSION_NUMBER,
          i,
          BLOB_STORAGE_URI,
          BlobStorageType.LOCAL_FS,
          pushControlSignalAccessor,
          storageService,
          storeConfig,
          svsSupplier,
          notifiers,
          serverConfig,
          semaphore,
          0L,
          null,
          null) {
        @Override
        List<String> downloadSSTFiles() {
          int current = concurrentDownloads.incrementAndGet();
          maxConcurrentDownloads.updateAndGet(max -> Math.max(max, current));
          allStarted.countDown();
          try {
            gate.await(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          concurrentDownloads.decrementAndGet();
          return Collections.singletonList("/tmp/fake.sst");
        }
      };
      threads[i] = new Thread(() -> {
        try {
          task.downloadWithConcurrencyLimit();
        } catch (Exception e) {
          // ignore
        }
      });
      threads[i].start();
    }

    // Wait a bit for threads to try acquiring permits
    Thread.sleep(200);

    // Only 2 should be downloading concurrently (semaphore permits = 2)
    assert concurrentDownloads.get() <= 2 : "Expected at most 2 concurrent downloads, got " + concurrentDownloads.get();

    // Release the gate so all downloads complete
    gate.countDown();
    for (Thread t: threads) {
      t.join(5000);
    }

    assert maxConcurrentDownloads.get() <= 2
        : "Max concurrent downloads should be <= 2, got " + maxConcurrentDownloads.get();
    // All permits should be released
    assert semaphore.availablePermits() == 2 : "All semaphore permits should be released";
  }

  @Test
  public void testDownloadWithConcurrencyLimit_nullSemaphoreNoBlocking() throws Exception {
    // With null semaphore, download should proceed without blocking
    BlobIngestionTask task = new BlobIngestionTask(
        STORE_NAME,
        VERSION_NUMBER,
        PARTITION,
        BLOB_STORAGE_URI,
        BlobStorageType.LOCAL_FS,
        pushControlSignalAccessor,
        storageService,
        storeConfig,
        svsSupplier,
        notifiers,
        serverConfig,
        null,
        0L,
        null,
        null) {
      @Override
      List<String> downloadSSTFiles() {
        return Collections.singletonList("/tmp/fake.sst");
      }
    };

    List<String> result = task.downloadWithConcurrencyLimit();
    assert result.size() == 1 : "Should download successfully with null semaphore";
  }

  @Test
  public void testDownloadWithConcurrencyLimit_releasesSemaphoreOnFailure() throws Exception {
    Semaphore semaphore = new Semaphore(1);

    BlobIngestionTask task = new BlobIngestionTask(
        STORE_NAME,
        VERSION_NUMBER,
        PARTITION,
        BLOB_STORAGE_URI,
        BlobStorageType.LOCAL_FS,
        pushControlSignalAccessor,
        storageService,
        storeConfig,
        svsSupplier,
        notifiers,
        serverConfig,
        semaphore,
        0L,
        null,
        null) {
      @Override
      List<String> downloadSSTFiles() throws IOException {
        throw new IOException("Download failed");
      }
    };

    try {
      task.downloadWithConcurrencyLimit();
      assert false : "Should have thrown IOException";
    } catch (IOException e) {
      // expected
    }

    // Semaphore should be released even after failure
    assert semaphore.availablePermits() == 1 : "Semaphore permit should be released on failure";
  }

  @Test
  public void testSubmitAndAwaitIngest_runsOnIngestPool() throws Exception {
    ExecutorService ingestPool = Executors.newSingleThreadExecutor();
    Semaphore pendingIngest = new Semaphore(2);
    AtomicBoolean ingestRan = new AtomicBoolean(false);
    AtomicReference<String> ingestThread = new AtomicReference<>();

    try {
      BlobIngestionTask task = new BlobIngestionTask(
          STORE_NAME,
          VERSION_NUMBER,
          PARTITION,
          BLOB_STORAGE_URI,
          BlobStorageType.LOCAL_FS,
          pushControlSignalAccessor,
          storageService,
          storeConfig,
          svsSupplier,
          notifiers,
          serverConfig,
          null,
          0L,
          ingestPool,
          pendingIngest) {
        @Override
        void ingestSSTFiles(List<String> sstFilePaths) {
          ingestRan.set(true);
          ingestThread.set(Thread.currentThread().getName());
        }
      };

      String callerThread = Thread.currentThread().getName();
      task.submitAndAwaitIngest(Collections.singletonList("/tmp/fake.sst"));

      assert ingestRan.get() : "Ingest should have run";
      assert !ingestThread.get().equals(callerThread) : "Ingest should run on a different thread than caller";
      assert pendingIngest.availablePermits() == 2 : "Pending-ingest permit should be released";
    } finally {
      ingestPool.shutdownNow();
    }
  }

  @Test
  public void testSubmitAndAwaitIngest_inlineWhenNoExecutor() throws Exception {
    AtomicBoolean ingestRan = new AtomicBoolean(false);

    BlobIngestionTask task = new BlobIngestionTask(
        STORE_NAME,
        VERSION_NUMBER,
        PARTITION,
        BLOB_STORAGE_URI,
        BlobStorageType.LOCAL_FS,
        pushControlSignalAccessor,
        storageService,
        storeConfig,
        svsSupplier,
        notifiers,
        serverConfig,
        null,
        0L,
        null,
        null) {
      @Override
      void ingestSSTFiles(List<String> sstFilePaths) {
        ingestRan.set(true);
      }
    };

    task.submitAndAwaitIngest(Collections.singletonList("/tmp/fake.sst"));
    assert ingestRan.get() : "Ingest should run inline when no executor is set";
  }

  @Test
  public void testSubmitAndAwaitIngest_releasesSemaphoreOnFailure() throws Exception {
    ExecutorService ingestPool = Executors.newSingleThreadExecutor();
    Semaphore pendingIngest = new Semaphore(1);

    try {
      BlobIngestionTask task = new BlobIngestionTask(
          STORE_NAME,
          VERSION_NUMBER,
          PARTITION,
          BLOB_STORAGE_URI,
          BlobStorageType.LOCAL_FS,
          pushControlSignalAccessor,
          storageService,
          storeConfig,
          svsSupplier,
          notifiers,
          serverConfig,
          null,
          0L,
          ingestPool,
          pendingIngest) {
        @Override
        void ingestSSTFiles(List<String> sstFilePaths) {
          throw new RuntimeException("Ingest failed");
        }
      };

      try {
        task.submitAndAwaitIngest(Collections.singletonList("/tmp/fake.sst"));
        assert false : "Should have thrown";
      } catch (RuntimeException e) {
        assert e.getMessage().equals("Ingest failed") : "Should propagate ingest exception";
      }

      // Permit should be released even after failure
      assert pendingIngest.availablePermits() == 1 : "Pending-ingest permit should be released on failure";
    } finally {
      ingestPool.shutdownNow();
    }
  }

  @Test
  public void testSubmitAndAwaitIngest_backpressureLimitsConcurrency() throws Exception {
    ExecutorService ingestPool = Executors.newFixedThreadPool(2);
    // Only allow 1 pending ingest
    Semaphore pendingIngest = new Semaphore(1);
    CountDownLatch ingestStarted = new CountDownLatch(1);
    CountDownLatch gate = new CountDownLatch(1);
    AtomicBoolean secondTaskAcquired = new AtomicBoolean(false);

    try {
      // First task: blocks during ingest until gate opens
      BlobIngestionTask task1 = new BlobIngestionTask(
          STORE_NAME,
          VERSION_NUMBER,
          0,
          BLOB_STORAGE_URI,
          BlobStorageType.LOCAL_FS,
          pushControlSignalAccessor,
          storageService,
          storeConfig,
          svsSupplier,
          notifiers,
          serverConfig,
          null,
          0L,
          ingestPool,
          pendingIngest) {
        @Override
        void ingestSSTFiles(List<String> sstFilePaths) {
          ingestStarted.countDown();
          try {
            gate.await(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
      };

      // Second task: tries to acquire pending-ingest permit
      BlobIngestionTask task2 = new BlobIngestionTask(
          STORE_NAME,
          VERSION_NUMBER,
          1,
          BLOB_STORAGE_URI,
          BlobStorageType.LOCAL_FS,
          pushControlSignalAccessor,
          storageService,
          storeConfig,
          svsSupplier,
          notifiers,
          serverConfig,
          null,
          0L,
          ingestPool,
          pendingIngest) {
        @Override
        void ingestSSTFiles(List<String> sstFilePaths) {
          secondTaskAcquired.set(true);
        }
      };

      Thread t1 = new Thread(() -> {
        try {
          task1.submitAndAwaitIngest(Collections.singletonList("/tmp/fake.sst"));
        } catch (Exception e) {
          // ignore
        }
      });
      t1.start();

      // Wait for the first ingest to start (it holds the permit)
      ingestStarted.await(5, TimeUnit.SECONDS);

      // Try to submit second task in background — should block on semaphore
      Thread t2 = new Thread(() -> {
        try {
          task2.submitAndAwaitIngest(Collections.singletonList("/tmp/fake.sst"));
        } catch (Exception e) {
          // ignore
        }
      });
      t2.start();

      // Give t2 a moment to try acquiring
      Thread.sleep(200);
      assert !secondTaskAcquired.get() : "Second task should be blocked by pending-ingest semaphore";

      // Release the gate so first ingest completes and releases permit
      gate.countDown();
      t1.join(5000);
      t2.join(5000);

      assert secondTaskAcquired.get() : "Second task should have run after first released permit";
      assert pendingIngest.availablePermits() == 1 : "All permits should be released";
    } finally {
      ingestPool.shutdownNow();
    }
  }

  private BlobIngestionTask createTask() {
    return new BlobIngestionTask(
        STORE_NAME,
        VERSION_NUMBER,
        PARTITION,
        BLOB_STORAGE_URI,
        BlobStorageType.LOCAL_FS,
        pushControlSignalAccessor,
        storageService,
        storeConfig,
        svsSupplier,
        notifiers,
        serverConfig,
        null,
        0L,
        null,
        null);
  }

  private BlobIngestionTask createTaskWithJitter(long jitterMaxMs) {
    return new BlobIngestionTask(
        STORE_NAME,
        VERSION_NUMBER,
        PARTITION,
        BLOB_STORAGE_URI,
        BlobStorageType.LOCAL_FS,
        pushControlSignalAccessor,
        storageService,
        storeConfig,
        svsSupplier,
        notifiers,
        serverConfig,
        null,
        jitterMaxMs,
        null,
        null);
  }

  /**
   * Creates a task with download and ingest steps stubbed out to avoid RocksDB native library dependencies.
   */
  private BlobIngestionTask createTaskWithNoOpDownloadAndIngest() {
    return new BlobIngestionTask(
        STORE_NAME,
        VERSION_NUMBER,
        PARTITION,
        BLOB_STORAGE_URI,
        BlobStorageType.LOCAL_FS,
        pushControlSignalAccessor,
        storageService,
        storeConfig,
        svsSupplier,
        notifiers,
        serverConfig,
        null,
        0L,
        null,
        null) {
      @Override
      List<String> downloadSSTFiles() {
        return Collections.singletonList("/tmp/fake.sst");
      }

      @Override
      void ingestSSTFiles(List<String> sstFilePaths) {
        // No-op for testing
      }
    };
  }
}
