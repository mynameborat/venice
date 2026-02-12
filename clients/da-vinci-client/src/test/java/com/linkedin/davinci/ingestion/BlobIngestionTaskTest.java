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
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
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
        serverConfig) {
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
        serverConfig);
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
        serverConfig) {
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
