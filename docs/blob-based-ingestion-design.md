# Blob-Based Ingestion for Venice Batch-Only Stores

## Context

Venice batch pushes currently route all data through Kafka: VenicePushJob (VPJ) writes records to a Kafka version topic, and servers consume from that topic to build RocksDB SST files. For batch-only stores (no hybrid, no incremental push, no DaVinci), Kafka serves purely as a data transfer medium with no streaming semantics needed. This design eliminates Kafka from the batch push data path entirely -- VPJ generates SST files directly and uploads them to blob storage, servers fetch and ingest those SST files.

**Note:** The existing P2P blob transfer in `da-vinci-client/src/main/java/com/linkedin/davinci/blobtransfer/` is a server-to-server mechanism for replica bootstrap when a replica goes down. It is unrelated to this design, which addresses the VPJ-to-server push data path.

---

## Current Flow vs Proposed Flow

```
CURRENT:  VPJ → Kafka topic → Server consumes → builds SST → serves
PROPOSED: VPJ → generates SST → blob storage → Server fetches SST → ingests → serves
```

---

## Design

### 1. Store & Version Model Changes

**New store-level flag: `blobBasedIngestionEnabled`**
- Added to `Store` interface and `AbstractStore` (`internal/venice-common/src/main/java/com/linkedin/venice/meta/AbstractStore.java`)
- Distinct from existing `blobTransferEnabled` (which controls P2P server-to-server transfer)
- Validation rules enforced at store update time and version creation time:
  - Must be batch-only (`hybridStoreConfig == null`)
  - Must not have DaVinci push status store enabled
  - Incremental push must not be enabled

**New PushType: `BATCH_BLOB`**
- Added to `PushType` enum in `Version.java` (`internal/venice-common/src/main/java/com/linkedin/venice/meta/Version.java`)
- Controller selects this when store has `blobBasedIngestionEnabled=true` and push is BATCH

**Version metadata additions** (`VersionImpl.java`):
- `blobBasedIngestion: boolean` — immutable configuration set at version creation
- `blobStorageUri: String` (e.g., `hdfs:///venice/blobs/`) — immutable configuration
- `blobStorageType: String` (e.g., `HDFS`, `S3`) — immutable configuration

Note: `blobUploadComplete` is NOT a version metadata field. It is a lifecycle signal tracked via the PushControlSignal mechanism (see section 3a).

### 2. VersionCreationResponse Changes

File: `internal/venice-common/src/main/java/com/linkedin/venice/controllerapi/VersionCreationResponse.java`

Add new fields (backward-compatible -- old clients ignore unknown JSON fields):
```java
private boolean blobBasedPush = false;
private String blobStorageUri = null;
private String blobStorageType = null;
private Map<String, String> blobStorageConfig = null;
```

Existing fields (`kafkaTopic`, `kafkaBootstrapServers`) will be null for blob pushes. Fields like `partitions`, `partitionerClass`, `partitionerParams`, `compressionStrategy` are still populated.

### 3. Controller Changes

**Version creation** (`VeniceHelixAdmin.addVersion()` at `services/venice-controller/src/main/java/com/linkedin/venice/controller/VeniceHelixAdmin.java`):
- When `pushType == BATCH_BLOB`: skip `createBatchTopics()` (no Kafka topic)
- Still create Helix resource (servers need partition assignments)
- Still start push monitor (for status aggregation)
- Store blob URI in version metadata (ZK)

#### 3a. PushControlSignal: Controller→Server Notification Mechanism

The existing `OfflinePushStatus` infrastructure carries server→controller notifications (servers write replica statuses, controller aggregates). For blob-based ingestion, the controller needs the reverse: a way to notify servers that blob data is ready. Storing mutable lifecycle state in version metadata is inappropriate since version configuration is immutable for a given version.

**Solution: `PushControlSignal`** — a general-purpose controller→server signaling mechanism stored at a dedicated ZK path (`/PushControlSignals/$topic`), separate from `OfflinePushStatus` (`/OfflinePushes/$topic`).

- `PushControlSignalType` enum — extensible set of signal types. Initially contains `BLOB_UPLOAD_COMPLETE`.
- `PushControlSignal` class — per-push container mapping signal types to emission timestamps. JSON-serialized to ZK.
- Controller writes signals; servers watch the ZK node and react.

```
Data flow comparison:

  Server→Controller (existing):   Servers write to /OfflinePushes/$topic/$partitionId
                                  Controller watches partition ZK nodes

  Controller→Server (new):        Controller writes to /PushControlSignals/$topic
                                  Servers watch push signal ZK node
```

**New endpoint: `notifyBlobPushComplete`** (in `CreateVersion.java` or new routes class):
- Called by VPJ after all SST files are uploaded
- Controller emits `BLOB_UPLOAD_COMPLETE` signal via `PushControlSignal` (ZK write)
- Servers watching the `/PushControlSignals/$topic` ZK node begin fetching SST files
- In multi-region: parent propagates signal to child controllers via admin channel

**Push status monitoring**:
- No Kafka lag tracking needed
- Controller waits for all replicas to report COMPLETED via Helix customized view (same mechanism as today, just without lag-based progress)
- `getOffLinePushStatus()` in `VeniceParentHelixAdmin.java` works unchanged -- it aggregates ExecutionStatus from child regions

**Blob cleanup in `killOfflinePush()`**:
- On push failure, controller triggers cleanup of blob data at `{blobStorageUri}/{storeName}/v{version}/`

### 4. VPJ Changes

**Main flow** (`VenicePushJob.java` at `clients/venice-push-job/src/main/java/com/linkedin/venice/hadoop/VenicePushJob.java`):

After `createNewStoreVersion()`, branch on `blobBasedPush`:
```
if blobBasedPush:
  1. Skip VeniceWriter creation (no Kafka)
  2. Skip SOP control message
  3. Run Spark job with BlobPartitionWriter (generates SST files, uploads to blob)
  4. Upload version manifest (storeName, version, partitionCount, timestamp)
  5. Call controller.notifyBlobPushComplete()
  6. pollStatusUntilComplete() (unchanged)
else:
  existing Kafka flow (unchanged)
```

#### 4a. BlobPartitionWriter — SST File Generation with Splitting

`BlobPartitionWriter` (in `clients/venice-push-job/src/main/java/com/linkedin/venice/spark/datawriter/writer/`) implements `Closeable` and replaces `AbstractPartitionWriter` + `VeniceWriter` for blob pushes. It uses RocksDB `SstFileWriter` (from RocksJava) to generate SST files locally on Spark executors.

**SST file splitting:** Rather than producing one SST file per partition (which could be arbitrarily large), the writer splits output into multiple files with a configurable size threshold:

- **Threshold:** Default 64MB, configurable via `blob.sst.file.size.threshold.bytes` (`VenicePushJobConstants.DEFAULT_BLOB_SST_FILE_SIZE_THRESHOLD_BYTES = 64 * 1024 * 1024`)
- **Check frequency:** Every 1000 records (`FILE_SIZE_CHECK_INTERVAL`) via `SstFileWriter.fileSize()`. Checking every record would be expensive due to JNI overhead; checking every 1000 amortizes that cost.
- **On threshold exceeded:** Finish current SST → upload to blob storage → delete temp file from executor disk → start new SST with a fresh `SstFileWriter`
- **File naming:** `data_0.sst`, `data_1.sst`, `data_2.sst`, etc. via `composeSstFileName(int index)` which returns `"data_" + index + ".sst"`
- **Disk bound:** Only one SST temp file exists on executor disk at a time. Each file is uploaded and deleted before the next one starts.
- **Sorted key invariant:** Rows arrive pre-sorted by Venice's `repartitionAndSortWithinPartitions()`. Splitting is sequential, so file N's last key is strictly less than file N+1's first key. No additional sorting is needed.

**No chunking** needed (no Kafka message size limits). Compression applied at value level (same as today).

#### 4b. BlobPartitionWriter — Buffer Reuse

Each value written to the SST file needs a 4-byte schema ID prefix (`SCHEMA_ID_PREFIX_SIZE = 4`). Rather than allocating a new `byte[]` per record, `BlobPartitionWriter` uses a monotonically growing reusable heap buffer:

- **Initial size:** 4096 bytes (`new byte[4096]`)
- **Growth:** When a value exceeds the buffer, the buffer doubles (or grows to required size, whichever is larger)
- **Heap `byte[]` chosen over `DirectByteBuffer`:** Direct buffers are off-heap and can conflict with Spark shuffle / Netty on shared executors. Heap buffers avoid `-XX:MaxDirectMemorySize` contention.
- **Steady state:** For stores with uniform value sizes (typical), the buffer stabilizes at the max value size after a few records. Once stabilized, no allocations occur — the same buffer is reused for every record.

The static `prependSchemaId()` method is retained for test use; production code uses `prependSchemaIdReusable()`.

#### 4c. Blob Path Convention

Paths are constructed via the `BlobStoragePaths` utility class (`internal/venice-common/src/main/java/com/linkedin/venice/blobtransfer/storage/BlobStoragePaths.java`):

```
{baseUri}/{storeName}/v{version}/
    manifest.json                    -- version manifest (written by VPJ after all partitions)
    p0/data_0.sst                    -- first SST file for partition 0
    p0/data_1.sst                    -- second SST file (if partition exceeded split threshold)
    p0/manifest.json                 -- partition manifest
    p1/data_0.sst
    p1/manifest.json
    ...
```

Key path methods:
- `BlobStoragePaths.versionDir(baseUri, storeName, version)` → `{baseUri}/{storeName}/v{version}`
- `BlobStoragePaths.partitionDir(baseUri, storeName, version, partition)` → `…/p{partition}`
- `BlobStoragePaths.sstFile(baseUri, storeName, version, partition, fileName)` → `…/p{partition}/{fileName}`
- `BlobStoragePaths.versionManifest(baseUri, storeName, version)` → `…/manifest.json`
- `BlobStoragePaths.partitionManifest(baseUri, storeName, version, partition)` → `…/p{partition}/manifest.json`

#### 4d. Version Manifest

After all Spark tasks complete, `VenicePushJob.writeVersionManifest()` writes a JSON manifest and uploads it via `BlobStorageClient`:

```json
{
  "storeName": "my_store",
  "version": 3,
  "partitionCount": 16,
  "timestamp": 1700000000000
}
```

The manifest is written to a temp file, uploaded to `BlobStoragePaths.versionManifest(...)`, and the temp file is deleted.

### 5. BlobStorageClient Abstraction

Interface in `internal/venice-common/src/main/java/com/linkedin/venice/blobtransfer/storage/BlobStorageClient.java`:

```java
public interface BlobStorageClient extends AutoCloseable {
    BlobTransferResult upload(String localPath, String remotePath) throws IOException;
    BlobTransferResult download(String remotePath, String localPath) throws IOException;
    boolean exists(String remotePath) throws IOException;
    void delete(String remotePath) throws IOException;
    void deleteDirectory(String remotePath) throws IOException;
}
```

All methods throw `IOException`. The return type is `BlobTransferResult` (not separate upload/download result types), which contains:
- `String path` — the resulting path
- `long bytesCopied` — bytes transferred
- `boolean success` — whether the operation succeeded
- `String errorMessage` — error details if failed

**First implementation: `LocalFsBlobStorageClient`** — uses Java NIO `Files` API (`Files.copy()`, `Files.exists()`, `Files.deleteIfExists()`, `Files.walkFileTree()` for recursive directory operations). This supports local-filesystem-based testing and single-node deployments.

**`BlobStorageType` enum** (`HDFS`, `S3`, `LOCAL_FS`) determines which implementation to use. Factory methods in `BlobPartitionWriterFactory` and `BlobIngestionTask` create the appropriate client.

Controller cluster config determines the blob backend:
```
blob.storage.type=LOCAL_FS
blob.storage.base.uri=/tmp/venice/blob-ingestion/
```

### 6. Server Changes

**Entry point** (`DefaultIngestionBackend.startConsumption()` at `clients/da-vinci-client/src/main/java/com/linkedin/davinci/ingestion/DefaultIngestionBackend.java`):

Add new code path before existing blob transfer and Kafka consumption:
```
if version.isBlobBasedIngestion():
  startBlobBasedIngestion(storeConfig, partition, version)
elif shouldEnableBlobTransfer(store):
  existing P2P blob transfer bootstrap → then Kafka
else:
  existing Kafka consumption
```

#### 6a. Thread Pool Architecture

`DefaultIngestionBackend` creates two dedicated thread pools for blob ingestion, separating I/O-bound downloads from CPU/disk-bound ingestion:

```
                                                    ┌─────────────────────┐
  BlobIngestionTask (partition A) ──────────────►   │  blobDownloadExecutor │
  BlobIngestionTask (partition B) ──────────────►   │  (8 threads, I/O)     │
  BlobIngestionTask (partition C) ──────────────►   │                       │
                                                    └─────────┬─────────────┘
                                                              │ download complete
                                                              ▼
                                                    ┌─────────────────────────┐
                                              ┌──── │  pendingIngestSemaphore  │
                                              │     │  (permits = ingestPool   │
                                              │     │   × 2 = 8 permits)      │
                                              │     └─────────┬───────────────┘
                                              │               │ permit acquired
                                              │               ▼
                                              │     ┌─────────────────────┐
                                              │     │  blobIngestExecutor   │
                                              │     │  (4 threads, CPU/disk) │
                                              │     └─────────┬─────────────┘
                                              │               │ ingest complete
                                              │               ▼
                                              └───── permit released
```

- **`blobDownloadExecutor`**: 8 threads (default), configurable via `blob.ingestion.download.pool.size`. Higher concurrency because downloads are I/O-bound (network wait).
- **`blobIngestExecutor`**: 4 threads (default), configurable via `blob.ingestion.ingest.pool.size`. Lower concurrency because `ingestExternalSSTFiles()` is CPU/disk-bound.
- **`pendingIngestSemaphore`**: Permits = `ingestPoolSize × 2` (default 8). Provides backpressure — limits how many partitions can be downloaded-but-not-yet-ingested, bounding disk usage from staging downloaded SST files.
- **Cross-partition parallelism**: Partition A can be ingesting while partition B is downloading. This is the primary benefit. Within-partition pipelining is not used because `ingestExternalSSTFiles()` is `synchronized` per partition, and RocksDB ingests multiple non-overlapping files more efficiently in a single bulk call.
- **Graceful shutdown**: `close()` calls `shutdown()` → `awaitTermination(30, SECONDS)` → `shutdownNow()` on both pools.

#### 6b. Thundering Herd Prevention

When `BLOB_UPLOAD_COMPLETE` fires, all servers for all partitions wake up simultaneously. Two mechanisms prevent thundering herd on blob storage:

**Random jitter:** Each `BlobIngestionTask` sleeps for `ThreadLocalRandom.current().nextLong(downloadJitterMaxMs)` before starting download. Default range: 0–5000ms, configurable via `blob.ingestion.download.jitter.max.ms`. Set to 0 to disable.

**Server-wide download semaphore:** `downloadSemaphore` with default 8 permits (configurable via `blob.ingestion.max.concurrent.downloads`). Acquired before download, released after download completes (not after ingest — the semaphore bounds concurrent network requests, not disk usage). Set to 0 to disable (no semaphore created).

**Request rate analysis** (worst-case): 4096 partitions × 3 replicas ÷ ~30 servers ≈ 410 partitions per server. With 8-permit download semaphore per server, fleet-wide concurrent downloads = 30 × 8 = 240. S3 per-prefix limit is ~5,500 GET/s, so 240 concurrent requests is well within limits. No cross-server coordination needed.

#### 6c. BlobIngestionTask — 7-Step Run Flow

`BlobIngestionTask` implements `Runnable` and executes on the `blobDownloadExecutor` thread pool. Each task handles one partition:

```
┌──────────────────────────────────────────────────────────────┐
│  1. reportStarted()                                          │
│     └─ Notifies listeners that ingestion has started         │
│                                                              │
│  2. waitForBlobUploadComplete()                              │
│     └─ ZK watcher on /PushControlSignals/$topic              │
│     └─ Race-condition-safe: checks signal immediately in     │
│        case it was emitted before watcher was registered     │
│     └─ Polls with configurable interval (default 5s)         │
│                                                              │
│  3. applyDownloadJitter()                                    │
│     └─ Thread.sleep(ThreadLocalRandom.nextLong(jitterMaxMs)) │
│                                                              │
│  4. downloadWithConcurrencyLimit()                           │
│     └─ Acquire downloadSemaphore permit                      │
│     └─ Download all SST files for partition from blob storage│
│     └─ Retry with exponential backoff (2^attempt × 1000ms)   │
│     └─ Release downloadSemaphore permit (in finally block)   │
│                                                              │
│  5. submitAndAwaitIngest(sstFilePaths)                       │
│     └─ Acquire pendingIngestSemaphore permit (backpressure)  │
│     └─ Submit ingestSSTFiles() to blobIngestExecutor         │
│     └─ Future.get() — block until ingest completes           │
│     └─ Release pendingIngestSemaphore permit (in finally)    │
│     └─ Bulk ingest: all SST files in one                     │
│        ingestExternalSSTFiles(allFiles) call                 │
│                                                              │
│  6. reportEndOfPushReceived()                                │
│                                                              │
│  7. reportCompleted()                                        │
└──────────────────────────────────────────────────────────────┘
```

If any step fails, the task reports an error via `IngestionNotificationDispatcher` and cleans up temp files.

**SST compatibility**: VPJ-generated SST files must match server RocksDB config (table format, compression). Controller passes RocksDB table format info in version metadata.

**State transitions** (simpler than Kafka path):
```
NOT_STARTED → STARTED → (watch PushControlSignal for BLOB_UPLOAD_COMPLETE) → (jitter) → (download) → (ingest) → END_OF_PUSH_RECEIVED → COMPLETED
```

### 7. Error Handling & Recovery

| Scenario | Handling |
|----------|----------|
| VPJ SST upload fails | Spark task retries (standard Spark fault tolerance). If all retries exhausted, Spark job fails, VPJ calls `killJob()` |
| Server blob download fails | Retry with exponential backoff (configurable max retries). After exhaustion, report ERROR to controller |
| SST ingestion fails (corrupt/incompatible) | Report ERROR. Controller can retry on different replica |
| Partial push failure | Controller's `PushStatusDecider` handles per-partition status. If any partition has zero COMPLETED replicas, push fails |
| Checksum mismatch | Re-download and retry. Persistent mismatch → report ERROR |

### 8. Cleanup

- **Failed pushes**: Controller's `killOfflinePush()` triggers blob data deletion
- **Successful pushes**: Background cleanup job deletes blob data for versions older than current-1
- **Server-side**: On partition drop, delete locally downloaded SST files (existing `RocksDBUtils.cleanupBothPartitionDirAndTempTransferredDir()`)

### 9. Multi-Region

Initial approach: VPJ uploads to one blob location (analogous to how it writes to source region Kafka for native replication). All servers across regions fetch from the same URI. The `blobStorageUri` is set at parent controller and propagated to child controllers.

Future optimization: VPJ could upload to per-region blob storage, or a replication layer could copy blobs across regions.

### 10. DaVinci Exclusion

- Controller rejects `blobBasedIngestionEnabled=true` if `isDaVinciPushStatusStoreEnabled()` is true
- Controller rejects DaVinci client registration for stores with `blobBasedIngestionEnabled`

### 11. Migration & Rollout

**Deployment order**: Servers → Controllers → VPJ clients → enable flag per store

**Backward compatibility**:
- Old VPJ clients see `blobBasedPush=false` (default) and use Kafka path
- Old servers don't understand blob ingestion; feature must not be enabled until servers are deployed
- `VersionCreationResponse` uses JSON -- unknown fields are ignored by old clients

---

## Design Decisions

1. **Data-ready signal**: **Dedicated ZK path via `PushControlSignal`**. The existing `OfflinePushStatus` carries server→controller notifications (replica status reporting). Blob readiness is a controller→server notification — the opposite direction. Rather than overloading `OfflinePushStatus` or storing mutable lifecycle state in immutable version metadata, a dedicated `/PushControlSignals/$topic` ZK path cleanly separates the two communication channels. The `PushControlSignal` abstraction is general-purpose and can carry future signal types beyond blob readiness.

2. **Multi-region**: **Single blob location for v1**. VPJ uploads to one region's blob storage, all servers fetch from there. Simple, can optimize with per-region upload later.

3. **Kafka fallback**: **Feature-flag approach**. No automatic fallback. If blob push fails, the user disables `blobBasedIngestionEnabled` on the store and re-runs the push using the existing Kafka code path. This keeps the design clean -- no dual-path complexity within a single push.

4. **SST file splitting with upload-and-delete**: Rather than writing one SST file per partition (unbounded size), the push job splits at a configurable threshold (default 64MB) and uploads+deletes each file before starting the next. This bounds executor disk usage to one SST file at a time and avoids all-or-nothing upload failure for large partitions.

5. **Download/ingest thread pool separation**: Two pools instead of one. The primary benefit is cross-partition parallelism: partition A ingests (CPU/disk) while partition B downloads (I/O). Within-partition pipelining (download file N+1 while ingesting file N) was considered but rejected because `ingestExternalSSTFiles()` is `synchronized` per partition, making per-file interleaving impossible.

6. **Bulk ingest**: All SST files for a partition are ingested in one `ingestExternalSSTFiles(allFiles)` call rather than one file at a time. RocksDB places all files optimally in a single pass when given multiple non-overlapping key-range files.

7. **Heap `byte[]` buffer reuse**: The per-record schema-ID prefix uses a reusable heap buffer instead of allocating a new `byte[]` per record or using `DirectByteBuffer`. Direct buffers are off-heap and can conflict with Spark shuffle / Netty for `-XX:MaxDirectMemorySize`. Heap buffers grow monotonically and stabilize at max value size.

8. **Per-server jitter + semaphore for thundering herd**: Random download jitter (0–5s) plus a per-server download semaphore (8 permits) are sufficient for expected request rates (~240 fleet-wide concurrent requests). Cross-server coordination would require a distributed rate limiter, which is disproportionate complexity.

9. **Backpressure via pending-ingest semaphore**: A semaphore with `ingestPoolSize × 2` permits limits how many partitions can be downloaded-but-not-yet-ingested. This bounds disk usage from staging downloaded SST files and prevents download threads from outpacing the ingest pool.

---

## Configuration Parameters

| Config Key | Default | Component | Validation |
|---|---|---|---|
| `blob.sst.file.size.threshold.bytes` | 67108864 (64MB) | Push job | Must be > 0 |
| `blob.sst.table.format` | `BLOCK_BASED_TABLE` | Push job | — |
| `blob.ingestion.download.pool.size` | 8 | Server | min 1 (`Math.max(value, 1)`) |
| `blob.ingestion.ingest.pool.size` | 4 | Server | min 1 (`Math.max(value, 1)`) |
| `blob.ingestion.download.jitter.max.ms` | 5000 | Server | Clamped to [0, 60000] |
| `blob.ingestion.max.concurrent.downloads` | 8 | Server | Clamped to [0, 256]; 0 = no limit |
| `blob.ingestion.download.max.retries` | 3 | Server | — |
| `blob.ingestion.poll.interval.ms` | 5000 | Server | — |

Push-job config keys are defined in `VenicePushJobConstants.java`. Server config keys are defined in `ConfigKeys.java` and parsed in `VeniceServerConfig.java`.

---

## Key Files

| Component | File |
|-----------|------|
| VPJ main | `clients/venice-push-job/src/main/java/com/linkedin/venice/hadoop/VenicePushJob.java` |
| Partition writer (abstract) | `clients/venice-push-job/src/main/java/com/linkedin/venice/hadoop/task/datawriter/AbstractPartitionWriter.java` |
| Spark partition writer | `clients/venice-push-job/src/main/java/com/linkedin/venice/spark/datawriter/writer/SparkPartitionWriter.java` |
| Blob partition writer | `clients/venice-push-job/src/main/java/com/linkedin/venice/spark/datawriter/writer/BlobPartitionWriter.java` |
| Blob partition writer factory | `clients/venice-push-job/src/main/java/com/linkedin/venice/spark/datawriter/writer/BlobPartitionWriterFactory.java` |
| Blob Spark job | `clients/venice-push-job/src/main/java/com/linkedin/venice/spark/datawriter/jobs/BlobDataWriterSparkJob.java` |
| Push job constants | `clients/venice-push-job/src/main/java/com/linkedin/venice/vpj/VenicePushJobConstants.java` |
| Version creation API | `services/venice-controller/src/main/java/com/linkedin/venice/controller/server/CreateVersion.java` |
| Controller admin | `services/venice-controller/src/main/java/com/linkedin/venice/controller/VeniceHelixAdmin.java` |
| Parent controller | `services/venice-controller/src/main/java/com/linkedin/venice/controller/VeniceParentHelixAdmin.java` |
| Ingestion backend | `clients/da-vinci-client/src/main/java/com/linkedin/davinci/ingestion/DefaultIngestionBackend.java` |
| Blob ingestion task | `clients/da-vinci-client/src/main/java/com/linkedin/davinci/ingestion/BlobIngestionTask.java` |
| Server config | `clients/da-vinci-client/src/main/java/com/linkedin/davinci/config/VeniceServerConfig.java` |
| SST writer (reuse pattern) | `clients/da-vinci-client/src/main/java/com/linkedin/davinci/store/rocksdb/RocksDBSstFileWriter.java` |
| Version response | `internal/venice-common/src/main/java/com/linkedin/venice/controllerapi/VersionCreationResponse.java` |
| Store model | `internal/venice-common/src/main/java/com/linkedin/venice/meta/AbstractStore.java` |
| Version model | `internal/venice-common/src/main/java/com/linkedin/venice/meta/VersionImpl.java` |
| PushType enum | `internal/venice-common/src/main/java/com/linkedin/venice/meta/Version.java` |
| Config keys | `internal/venice-common/src/main/java/com/linkedin/venice/ConfigKeys.java` |
| Blob storage client | `internal/venice-common/src/main/java/com/linkedin/venice/blobtransfer/storage/BlobStorageClient.java` |
| Local FS client | `internal/venice-common/src/main/java/com/linkedin/venice/blobtransfer/storage/LocalFsBlobStorageClient.java` |
| Transfer result | `internal/venice-common/src/main/java/com/linkedin/venice/blobtransfer/storage/BlobTransferResult.java` |
| Storage paths | `internal/venice-common/src/main/java/com/linkedin/venice/blobtransfer/storage/BlobStoragePaths.java` |
| Storage type enum | `internal/venice-common/src/main/java/com/linkedin/venice/blobtransfer/storage/BlobStorageType.java` |
| Push control signal | `internal/venice-common/src/main/java/com/linkedin/venice/pushmonitor/PushControlSignal.java` |
| Push signal types | `internal/venice-common/src/main/java/com/linkedin/venice/pushmonitor/PushControlSignalType.java` |
| Push signal accessor | `internal/venice-common/src/main/java/com/linkedin/venice/pushmonitor/PushControlSignalAccessor.java` |
| Push signal accessor (ZK) | `internal/venice-common/src/main/java/com/linkedin/venice/helix/VenicePushControlSignalAccessor.java` |
