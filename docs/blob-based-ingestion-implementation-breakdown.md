# Blob-Based Ingestion: Implementation Breakdown

This document breaks the design into **12 self-contained work packages (WPs)**. WP1–WP8 cover the MVP implementation. WP9–WP12 cover scalability improvements (SST splitting, buffer reuse, download jitter, thread pool separation). Each WP describes its scope, files to change, inputs/outputs at its boundary, and which other WPs it depends on.

---

## Implementation Progress

### WP1: Avro Schema Changes — COMPLETED

**Commit:** `fceec7956` on branch `blob`

**What was done:**
- Created `StoreMetaValue/v41/StoreMetaValue.avsc` — added `blobBasedIngestionEnabled` (boolean, default false) to `StoreProperties`
- Created `AdminOperation/v96/AdminOperation.avsc` — added `blobBasedIngestionEnabled` (boolean, default false) to `UpdateStore`
- Bumped protocol versions: `ADMIN_OPERATION` 94→96, `METADATA_SYSTEM_SCHEMA_STORE` 39→41
- Updated Avro codegen pins in `build.gradle` to v41/v96
- Added `isBlobBasedIngestionEnabled()` / `setBlobBasedIngestionEnabled()` to `Store` interface
- Implemented in `ZKStore`, `SystemStore`, `ReadOnlyStore`, `StoreInfo`
- Added `BLOB_BASED_INGESTION_ENABLED` constant and `UpdateStoreQueryParams` getter/setter
- Wired through `VeniceParentHelixAdmin` and `VeniceHelixAdmin` updateStore paths
- Fixed `blobDbEnabled` NPE (v95 String field) in production code and tests

**Deviations from original plan:**
- Protocol versions bumped to 41/96 (not 40/95) because codegen must include the new field for Java code to compile
- Additional files modified beyond original plan: `ReadOnlyStore.java`, `StoreInfo.java`, `build.gradle`, `AdminOperationSerializerTest.java`, `AdminConsumptionTaskTest.java`
- `blobDbEnabled = "NOT_SPECIFIED"` initialization required in `VeniceParentHelixAdmin` to fix NPE from v95's unwired String field

**Verification:** Full `compileJava` and `compileTestJava` pass. Tests pass (excluding 2 pre-existing flaky tests unrelated to changes).

### WP2: Data Model Layer — COMPLETED

**Commit:** `391f7c7bd` on branch `blob`

**What was done:**
- Added `BATCH_BLOB(4)` to `PushType` enum with `isBatchBlob()` and `isBatchOrBatchBlob()` helpers
- Added `isBlobBasedIngestion()`, `getBlobStorageUri()`, `getBlobStorageType()` to `Version` interface and `VersionImpl`
- Added `ReadOnlyStore` delegation for blob getters, `UnsupportedOperationException` for setters
- Propagated store-level `blobBasedIngestionEnabled` to version in `AbstractStore.addVersion()`
- Added blob fields to `VersionCreationResponse` (`blobBasedPush`, `blobStorageUri`, `blobStorageType`)
- Propagated `blobBasedIngestion` in `AddVersion` admin message via `VeniceParentHelixAdmin`
- Created `PushControlSignal` and `PushControlSignalType` for controller→server ZK signaling

### WP3: BlobStorageClient Abstraction — COMPLETED

**Commit:** `aefe32f19` on branch `blob` (combined with WP4)

**What was done:**
- Created `BlobStorageType` enum (`HDFS`, `S3`, `LOCAL_FS`) with `fromString()`
- Created `BlobStorageClient` interface with `upload()`, `download()`, `exists()`, `delete()`, `deleteDirectory()` — all returning `BlobTransferResult` and throwing `IOException`
- Created `BlobTransferResult` with `path`, `bytesCopied`, `success`, `errorMessage`
- Created `LocalFsBlobStorageClient` using Java NIO `Files` API
- Created `BlobStoragePaths` utility for path construction

**Deviations from original plan:**
- Separate `BlobUploadResult`/`BlobDownloadResult` replaced with unified `BlobTransferResult`
- `BlobStorageClientFactory` not created as a standalone class — factory logic lives in `BlobPartitionWriterFactory` and `BlobIngestionTask`
- `HdfsBlobStorageClient` not yet implemented

### WP4: Controller — Store Update Validation & Version Creation — COMPLETED

**Commit:** `aefe32f19` on branch `blob` (combined with WP3)

**What was done:**
- Wired `blobBasedIngestionEnabled` through `AdminExecutionTask`
- Added store update validation: rejects hybrid, DaVinci push status, and incremental push stores
- Added `BLOB_STORAGE_TYPE` and `BLOB_STORAGE_BASE_URI` to cluster config
- Upgraded BATCH to BATCH_BLOB push type and skipped Kafka topic creation for blob stores in `addVersion()`
- Populated blob fields on `VersionCreationResponse` for BATCH_BLOB pushes

### WP5: Controller — Blob Push Completion & Cleanup — COMPLETED

**Commit:** `8336fd71f` on branch `blob`

**What was done:**
- Added `BLOB_PUSH_COMPLETE` route to `ControllerRoute`
- Added handler in `CreateVersion.java` that validates version and emits signal
- Created `VenicePushControlSignalAccessor` (ZK accessor for `/PushControlSignals/$topic`) following `VeniceOfflinePushMonitorAccessor` pattern
- Created `PushControlSignalJSONSerializer` for ZK serialization
- Added `PushControlSignalAccessor` interface with create/get/update/delete/subscribe/unsubscribe methods
- Added `notifyBlobPushComplete()` to `ControllerClient`
- Wired accessor into `HelixVeniceClusterResources` and `AdminSparkServer`

### WP6: VPJ — Blob Push Flow — COMPLETED

**Commit:** `623007122` on branch `blob`

**What was done:**
- Added blob fields to `PushJobSetting` (`blobBasedPush`, `blobStorageUri`, `blobStorageType`, `blobStorageConfig`)
- Added blob branch in `VenicePushJob.pushFile()`: skips VeniceWriter/Kafka, runs `BlobDataWriterSparkJob`, calls `notifyBlobPushComplete()`
- Created `BlobPartitionWriter` using RocksDB `SstFileWriter` to generate SST files per partition
- Created `BlobPartitionWriterFactory` (Spark `MapPartitionsFunction`)
- Created `BlobDataWriterSparkJob` extending `DataWriterSparkJob`
- Added `writeVersionManifest()` for uploading version manifest JSON
- Added blob-related constants to `VenicePushJobConstants`

### WP7: Server — Blob Ingestion Path — COMPLETED

**Commit:** `868de71bb` on branch `blob`

**What was done:**
- Created `BlobIngestionTask` implementing full lifecycle: reportStarted → poll ZK for `BLOB_UPLOAD_COMPLETE` → download SSTs with exponential backoff → ingest via `ingestExternalFile()` → reportCompleted
- Added `ingestExternalSSTFiles()` to `RocksDBStoragePartition`
- Added blob ingestion branch in `DefaultIngestionBackend.startConsumption()` gated on `version.isBlobBasedIngestion()`
- Added `BLOB_INGESTION_DOWNLOAD_MAX_RETRIES` and `BLOB_INGESTION_POLL_INTERVAL_MS` config keys
- Wired `PushControlSignalAccessor` in `HelixParticipationService.asyncStart()`
- Exposed `leaderFollowerNotifiers` queue from `KafkaStoreIngestionService`

### Bug Fix Commits (post-WP7, pre-scalability)

**Commit:** `8f219b4dc` — Fix P0 and P1 review issues for blob-based ingestion
**Commit:** `5fd386f91` — Fix prefix collision bug and add test coverage
**Commit:** `ce543bc7c` — Fix runtime bugs in blob-based ingestion pipeline

### WP9–WP12: Scalability Improvements — COMPLETED

**Commit:** `fd1d296bf` on branch `blob`

All four scalability work packages were implemented and committed together. See individual WP sections below for details.

---

```
Dependency graph:

  WP1 (Avro Schema)
   |
   v
  WP2 (Data Model)       WP3 (BlobStorageClient)  <-- independent
   |                        |
   +---+----+----+----------+
   |   |    |    |
   v   v    v    v
  WP4 WP5  WP6  WP7
   |   |    |    |
   +---+----+----+
        |
        v
       WP8 (Integration test skeleton)
        |
        v
  WP9 (Buffer Reuse)  -- push-job only, no deps on WP8
  WP10 (SST Splitting)  -- push-job only
  WP11 (Jitter + Concurrency Cap)  -- server only
  WP12 (Thread Pool Separation)  -- server only

  Note: WP9-WP12 are scalability improvements that can be
  implemented independently of each other. They depend on
  WP6 (push-job) and WP7 (server) being complete.
```

---

## WP1: Avro Schema Changes

**Goal:** Add all new fields to the Avro schemas so that generated Java classes include them. This unblocks all other model and logic work.

**Depends on:** Nothing

**Scope:**

1. **Create `StoreMetaValue/v41/StoreMetaValue.avsc`** (copy from v40, add fields):
   - In `StoreProperties`: add `{"name": "blobBasedIngestionEnabled", "type": "boolean", "default": false, "doc": "Flag to enable blob-based ingestion for batch-only stores. When enabled, VPJ generates SST files and uploads them to blob storage instead of writing to Kafka."}`
   - In `StoreVersion`: add 3 fields (immutable version configuration only — `blobUploadComplete` is tracked via `PushControlSignal`, not version metadata):
     ```json
     {"name": "blobBasedIngestion", "type": "boolean", "default": false, "doc": "Whether this version uses blob-based ingestion (SST files from blob storage instead of Kafka)."}
     {"name": "blobStorageUri", "type": "string", "default": "", "doc": "URI for blob storage location for this version (e.g. hdfs:///venice/blob-ingestion/)."}
     {"name": "blobStorageType", "type": "string", "default": "", "doc": "Blob storage backend type (e.g. HDFS, S3, LOCAL_FS)."}
     ```

2. **Create `AdminOperation/v85/AdminOperation.avsc`** (copy from v84, add fields):
   - In `AddVersion` record: add `{"name": "blobBasedIngestion", "type": "boolean", "default": false}`
   - In `UpdateStore` record: add `{"name": "blobBasedIngestionEnabled", "type": ["null", "boolean"], "default": null, "doc": "Flag to enable blob-based ingestion for batch-only stores."}`

3. **Run Avro code generation** to produce updated Java classes (`StoreVersion`, `StoreProperties`, `AddVersion`, `UpdateStore`).

**Verification:** `./gradlew :internal:venice-common:compileJava` succeeds; generated classes have the new fields with correct defaults.

**Files:**
- `internal/venice-common/src/main/resources/avro/StoreMetaValue/v41/StoreMetaValue.avsc` (new)
- `services/venice-controller/src/main/resources/avro/AdminOperation/v85/AdminOperation.avsc` (new)
- Any build config that references the latest schema version (grep for `v40` / `v84` in build.gradle or constants)

**Output contract:** Generated Java classes `StoreVersion`, `StoreProperties`, `AddVersion`, `UpdateStore` have the new fields accessible via getters/setters with correct defaults.

---

## WP2: Data Model Layer (Version, Store, PushType, VersionCreationResponse)

**Goal:** Expose the new Avro fields through the Venice Java model interfaces so that controller, VPJ, and server code can use them.

**Depends on:** WP1

**Scope:**

### 2a. PushType enum

**File:** `internal/venice-common/src/main/java/com/linkedin/venice/meta/Version.java`

Add `BATCH_BLOB(4)` to `PushType` enum. Update map size from 4 to 5. Add helper:
```java
BATCH_BLOB(4); // VPJ generates SST files and uploads to blob storage.

public boolean isBatchBlob() {
  return this == BATCH_BLOB;
}
```
Update `isBatchOrStreamReprocessing()` and any method that checks `isBatch()` where `BATCH_BLOB` should also match. Add `isBatchOrBatchBlob()` helper.

### 2b. Version interface + VersionImpl

**File:** `internal/venice-common/src/main/java/com/linkedin/venice/meta/Version.java`

Add to interface (immutable version configuration only — no `blobUploadComplete` here):
```java
boolean isBlobBasedIngestion();
void setBlobBasedIngestion(boolean blobBasedIngestion);
String getBlobStorageUri();
void setBlobStorageUri(String uri);
String getBlobStorageType();
void setBlobStorageType(String type);
```

**File:** `internal/venice-common/src/main/java/com/linkedin/venice/meta/VersionImpl.java`

Implement the 6 methods using `this.storeVersion.blobBasedIngestion`, etc. Add the 3 fields to `cloneVersion()`.

### 2b-2. PushControlSignal — Controller→Server notification model

Blob upload completion is a lifecycle signal (controller notifying servers that data is ready), not version configuration. The existing `OfflinePushStatus` carries server→controller notifications. A new `PushControlSignal` mechanism provides the reverse direction.

**File (new):** `internal/venice-common/src/main/java/com/linkedin/venice/pushmonitor/PushControlSignalType.java`

Extensible enum of signal types the controller can emit:
```java
public enum PushControlSignalType {
  BLOB_UPLOAD_COMPLETE  // emitted when VPJ signals all SST files uploaded
}
```

**File (new):** `internal/venice-common/src/main/java/com/linkedin/venice/pushmonitor/PushControlSignal.java`

Per-push container for active signals, stored at dedicated ZK path `/PushControlSignals/$topic`:
```java
public class PushControlSignal {
  private final String kafkaTopic;
  private final Map<PushControlSignalType, Long> signals; // type → timestamp

  public void emitSignal(PushControlSignalType type);
  public boolean hasSignal(PushControlSignalType type);
  public long getSignalTimestamp(PushControlSignalType type);
}
```

JSON-serializable for ZK storage. The ZK accessor and watcher infrastructure is added in WP5 (controller) and WP7 (server).

### 2c. Store interface + ZKStore + SystemStore

**File:** `internal/venice-common/src/main/java/com/linkedin/venice/meta/Store.java`

Add:
```java
boolean isBlobBasedIngestionEnabled();
void setBlobBasedIngestionEnabled(boolean enabled);
```

**File:** `internal/venice-common/src/main/java/com/linkedin/venice/meta/ZKStore.java`

Implement using `this.storeProperties.blobBasedIngestionEnabled`. Add to clone/copy constructor.

**File:** `internal/venice-common/src/main/java/com/linkedin/venice/meta/SystemStore.java`

Implement: getter delegates to `zkSharedStore`, setter throws `UnsupportedOperationException`.

### 2d. AbstractStore - propagate flag to version

**File:** `internal/venice-common/src/main/java/com/linkedin/venice/meta/AbstractStore.java`

In the `addVersion()` private method, inside the `if (!isClonedVersion)` block, add:
```java
version.setBlobBasedIngestion(isBlobBasedIngestionEnabled());
```

### 2e. VersionCreationResponse

**File:** `internal/venice-common/src/main/java/com/linkedin/venice/controllerapi/VersionCreationResponse.java`

Add fields with getters/setters:
```java
private boolean blobBasedPush = false;
private String blobStorageUri = null;
private String blobStorageType = null;
private Map<String, String> blobStorageConfig = null;
```
Update `toString()`.

### 2f. UpdateStoreQueryParams

**File:** `internal/venice-common/src/main/java/com/linkedin/venice/controllerapi/UpdateStoreQueryParams.java`

Add:
```java
public UpdateStoreQueryParams setBlobBasedIngestionEnabled(boolean enabled) {
  return putBoolean(BLOB_BASED_INGESTION_ENABLED, enabled);
}
public Optional<Boolean> getBlobBasedIngestionEnabled() {
  return getBoolean(BLOB_BASED_INGESTION_ENABLED);
}
```

**File:** `internal/venice-common/src/main/java/com/linkedin/venice/controllerapi/ControllerApiConstants.java` (or wherever `BLOB_TRANSFER_ENABLED` is defined)

Add constant: `public static final String BLOB_BASED_INGESTION_ENABLED = "blob_based_ingestion_enabled";`

### 2g. StoreInfo (DTO used in admin tools)

**File:** grep for `class StoreInfo` -- likely `internal/venice-common/src/main/java/com/linkedin/venice/controllerapi/StoreResponse.java` or similar.

Add `blobBasedIngestionEnabled` field so admin tools can display it.

**Verification:** `./gradlew :internal:venice-common:compileJava` succeeds. Unit tests in `VersionImplTest`, `ZKStoreTest` pass (add simple get/set tests).

**Output contract:** All model interfaces expose blob ingestion fields. Controller, VPJ, and server code can call `version.isBlobBasedIngestion()`, `store.isBlobBasedIngestionEnabled()`, etc.

---

## WP3: BlobStorageClient Abstraction

**Goal:** Create a pluggable blob storage client that both VPJ (upload) and server (download) will use. This WP is fully independent of the model changes.

**Depends on:** Nothing (can be done in parallel with WP1 and WP2)

**Scope:**

### 3a. BlobStorageType enum

**File (new):** `internal/venice-common/src/main/java/com/linkedin/venice/blobtransfer/storage/BlobStorageType.java`

```java
public enum BlobStorageType {
  HDFS, S3, LOCAL_FS;

  public static BlobStorageType fromString(String type) { ... }
}
```

### 3b. Result classes

**File (new):** `internal/venice-common/src/main/java/com/linkedin/venice/blobtransfer/storage/BlobUploadResult.java`

```java
public class BlobUploadResult {
  private final String remotePath;
  private final long bytesCopied;
  private final boolean success;
  // constructor, getters
}
```

**File (new):** `internal/venice-common/src/main/java/com/linkedin/venice/blobtransfer/storage/BlobDownloadResult.java`

Similar structure with `localPath`, `bytesCopied`, `success`.

### 3c. BlobStorageClient interface

**File (new):** `internal/venice-common/src/main/java/com/linkedin/venice/blobtransfer/storage/BlobStorageClient.java`

```java
public interface BlobStorageClient extends AutoCloseable {
  BlobUploadResult upload(String localPath, String remotePath) throws IOException;
  BlobDownloadResult download(String remotePath, String localPath) throws IOException;
  boolean exists(String remotePath) throws IOException;
  void delete(String remotePath) throws IOException;
  void deleteDirectory(String remotePath) throws IOException;
}
```

### 3d. HdfsBlobStorageClient

**File (new):** `internal/venice-common/src/main/java/com/linkedin/venice/blobtransfer/storage/HdfsBlobStorageClient.java`

Wraps Hadoop `FileSystem`. Uses `copyFromLocalFile()` / `copyToLocalFile()`. Constructor takes `Configuration` + base URI.

### 3e. LocalFsBlobStorageClient

**File (new):** `internal/venice-common/src/main/java/com/linkedin/venice/blobtransfer/storage/LocalFsBlobStorageClient.java`

Uses `java.nio.file.Files.copy()`. For integration tests.

### 3f. BlobStorageClientFactory

**File (new):** `internal/venice-common/src/main/java/com/linkedin/venice/blobtransfer/storage/BlobStorageClientFactory.java`

```java
public class BlobStorageClientFactory {
  public static BlobStorageClient create(BlobStorageType type, Map<String, String> config) {
    switch (type) {
      case HDFS: return new HdfsBlobStorageClient(config);
      case LOCAL_FS: return new LocalFsBlobStorageClient(config);
      default: throw new VeniceException("Unsupported: " + type);
    }
  }
}
```

### 3g. Blob path convention helper

**File (new):** `internal/venice-common/src/main/java/com/linkedin/venice/blobtransfer/storage/BlobStoragePaths.java`

```java
public class BlobStoragePaths {
  public static String versionDir(String baseUri, String storeName, int version) {
    return baseUri + "/" + storeName + "/v" + version;
  }
  public static String partitionDir(String baseUri, String storeName, int version, int partition) {
    return versionDir(baseUri, storeName, version) + "/p" + partition;
  }
  public static String sstFile(String baseUri, String storeName, int version, int partition) {
    return partitionDir(baseUri, storeName, version, partition) + "/data_0.sst";
  }
  public static String partitionManifest(String baseUri, String storeName, int version, int partition) {
    return partitionDir(baseUri, storeName, version, partition) + "/metadata.json";
  }
  public static String versionManifest(String baseUri, String storeName, int version) {
    return versionDir(baseUri, storeName, version) + "/manifest.json";
  }
}
```

**Verification:** Unit tests for each client implementation (HDFS test with MiniDFSCluster or mocked FileSystem, LocalFs test with temp directory). Test path generation.

**Output contract:** Any component can create a `BlobStorageClient` via the factory and upload/download files using the path conventions.

---

## WP4: Controller - Store Update Validation & Version Creation

**Goal:** Wire the new store flag through the controller update path and modify version creation to handle `BATCH_BLOB` push type (skip Kafka topic creation).

**Depends on:** WP2

**Scope:**

### 4a. Store update validation

**File:** `services/venice-controller/src/main/java/com/linkedin/venice/controller/VeniceHelixAdmin.java`

In the `updateStore()` method (near where `blobTransferEnabled` is handled around line 6207), add handling for `blobBasedIngestionEnabled`:
```java
blobBasedIngestionEnabled.ifPresent(enabled -> {
  if (enabled) {
    // Validate: must be batch-only
    if (store.getHybridStoreConfig() != null) {
      throw new VeniceException("blobBasedIngestionEnabled requires batch-only store (no hybrid config)");
    }
    if (store.isDaVinciPushStatusStoreEnabled()) {
      throw new VeniceException("blobBasedIngestionEnabled not compatible with DaVinci push status store");
    }
    if (store.isIncrementalPushEnabled()) {
      throw new VeniceException("blobBasedIngestionEnabled not compatible with incremental push");
    }
  }
  store.setBlobBasedIngestionEnabled(enabled);
});
```

Also add to the admin operation handling (parent->child propagation) where `UpdateStore` admin messages are processed.

### 4b. Version creation: select BATCH_BLOB push type

**File:** `services/venice-controller/src/main/java/com/linkedin/venice/controller/VeniceHelixAdmin.java`

In `addVersion()`, after resolving the push type, add logic:
```java
if (pushType == PushType.BATCH && store.isBlobBasedIngestionEnabled()) {
  pushType = PushType.BATCH_BLOB;
}
```

### 4c. Version creation: skip Kafka topics for BATCH_BLOB

**File:** `services/venice-controller/src/main/java/com/linkedin/venice/controller/VeniceHelixAdmin.java`

In `addVersion()`, wrap the two `createBatchTopics()` calls with a guard:
```java
if (!pushType.isBatchBlob()) {
  createBatchTopics(...);
}
```

The Helix resource creation and push monitor start must still happen for BATCH_BLOB.

### 4d. Version creation: set blob metadata

In `addVersion()`, after creating the version but before storing it:
```java
if (pushType.isBatchBlob()) {
  String blobUri = clusterConfig.getBlobStorageBaseUri(); // new cluster config
  String blobType = clusterConfig.getBlobStorageType();   // new cluster config
  version.setBlobBasedIngestion(true);
  version.setBlobStorageUri(blobUri + "/" + storeName + "/v" + version.getNumber());
  version.setBlobStorageType(blobType);
}
```

### 4e. Populate VersionCreationResponse

**File:** `services/venice-controller/src/main/java/com/linkedin/venice/controller/server/CreateVersion.java`

In `handleNonStreamPushType()`, after building the response, populate the blob fields when `BATCH_BLOB`:
```java
if (version.isBlobBasedIngestion()) {
  response.setBlobBasedPush(true);
  response.setBlobStorageUri(version.getBlobStorageUri());
  response.setBlobStorageType(version.getBlobStorageType());
  // kafkaTopic and kafkaBootstrapServers will be null
}
```

### 4f. Cluster config for blob storage

**File:** `services/venice-controller/src/main/java/com/linkedin/venice/controller/VeniceControllerClusterConfig.java`

Add two new config keys and their getters:
```java
// ConfigKeys.java
public static final String BLOB_STORAGE_TYPE = "blob.storage.type";
public static final String BLOB_STORAGE_BASE_URI = "blob.storage.base.uri";
```

### 4g. Admin operation propagation for AddVersion

Where the parent controller creates `AddVersion` admin messages to send to child controllers, propagate the `blobBasedIngestion` field.

**Verification:** Controller unit tests: create a store with `blobBasedIngestionEnabled=true`, request a BATCH version, verify no Kafka topic is created and response contains blob fields. Verify validation rejects hybrid stores.

**Output contract:** When VPJ calls `requestTopicForWrites()` for a blob-enabled store, the `VersionCreationResponse` has `blobBasedPush=true`, `blobStorageUri`, `blobStorageType` populated and `kafkaTopic=null`.

---

## WP5: Controller - Blob Push Completion & Cleanup

**Goal:** Add the endpoint that VPJ calls after uploading all SST files, and add blob cleanup logic.

**Depends on:** WP2, WP3

**Scope:**

### 5a. New controller route

**File:** `internal/venice-common/src/main/java/com/linkedin/venice/controllerapi/ControllerRoute.java`

Add:
```java
BLOB_PUSH_COMPLETE("/blob_push_complete", HttpMethod.POST, Arrays.asList(NAME, VERSION))
```

### 5b. New controller endpoint handler

**File (new or in existing):** `services/venice-controller/src/main/java/com/linkedin/venice/controller/server/CreateVersion.java` (or a new `BlobPushRoutes.java`)

Handler that:
1. Validates the version exists and is a blob-based version
2. Calls `admin.notifyBlobPushComplete(clusterName, storeName, versionNumber)`

### 5c. VeniceHelixAdmin.notifyBlobPushComplete

**File:** `services/venice-controller/src/main/java/com/linkedin/venice/controller/VeniceHelixAdmin.java`

New method:
```java
public void notifyBlobPushComplete(String clusterName, String storeName, int versionNumber) {
  // 1. Get store and version from ZK
  // 2. Validate version.isBlobBasedIngestion()
  // 3. Emit BLOB_UPLOAD_COMPLETE signal via PushControlSignal
  //    - Load or create PushControlSignal for this topic from /PushControlSignals/$topic
  //    - Call signal.emitSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE)
  //    - Write back to ZK
  // 4. Servers watching /PushControlSignals/$topic ZK node will be notified
}
```

For parent controller (`VeniceParentHelixAdmin`): propagate via admin channel to child controllers.

### 5c-2. PushControlSignal ZK accessor

**File (new):** ZK accessor for reading/writing `PushControlSignal` objects.

Follows the same pattern as `VeniceOfflinePushMonitorAccessor` but for the `/PushControlSignals/` ZK path:
- `createPushControlSignal(topic)` — create the ZK node when push starts
- `updatePushControlSignal(signal)` — write updated signal to ZK
- `loadPushControlSignal(topic)` — read current signal from ZK
- `deletePushControlSignal(topic)` — clean up when push completes or is killed
- Subscribe/unsubscribe for ZK data change notifications (servers use this)

### 5d. ControllerClient method

**File:** `internal/venice-common/src/main/java/com/linkedin/venice/controllerapi/ControllerClient.java`

Add:
```java
public ControllerResponse notifyBlobPushComplete(String storeName, int versionNumber) {
  QueryParams params = newParams().add(NAME, storeName).add(VERSION, versionNumber);
  return request(ControllerRoute.BLOB_PUSH_COMPLETE, params, ControllerResponse.class);
}
```

### 5e. Blob cleanup in killOfflinePush

**File:** `services/venice-controller/src/main/java/com/linkedin/venice/controller/VeniceHelixAdmin.java`

In `killOfflinePush()`, add:
```java
if (version.isBlobBasedIngestion()) {
  BlobStorageClient blobClient = BlobStorageClientFactory.create(...);
  blobClient.deleteDirectory(version.getBlobStorageUri());
}
```

### 5f. Background cleanup for old version blob data

Can be added to the existing version cleanup job or deferred to a follow-up. When a version is retired (deleteOldVersions flow), also delete its blob data.

**Verification:** Unit test: call `notifyBlobPushComplete`, verify `BLOB_UPLOAD_COMPLETE` signal is emitted in `/PushControlSignals/$topic` ZK node. Test cleanup on kill.

**Output contract:** VPJ can call `controllerClient.notifyBlobPushComplete(storeName, version)` and servers watching `/PushControlSignals/$topic` will observe the `BLOB_UPLOAD_COMPLETE` signal.

---

## WP6: VPJ - Blob Push Flow

**Goal:** Modify VenicePushJob to branch into a blob-based push path: skip VeniceWriter/Kafka, generate SST files via Spark, upload to blob storage, and notify controller.

**Depends on:** WP2, WP3

**Scope:**

### 6a. PushJobSetting blob fields

**File:** `clients/venice-push-job/src/main/java/com/linkedin/venice/hadoop/PushJobSetting.java`

Add:
```java
public boolean blobBasedPush;
public String blobStorageUri;
public String blobStorageType;
public Map<String, String> blobStorageConfig;
```

### 6b. VenicePushJob.createNewStoreVersion - parse blob response

**File:** `clients/venice-push-job/src/main/java/com/linkedin/venice/hadoop/VenicePushJob.java`

After receiving `VersionCreationResponse`, populate the new PushJobSetting fields:
```java
pushJobSetting.blobBasedPush = response.isBlobBasedPush();
pushJobSetting.blobStorageUri = response.getBlobStorageUri();
pushJobSetting.blobStorageType = response.getBlobStorageType();
pushJobSetting.blobStorageConfig = response.getBlobStorageConfig();
```

### 6c. VenicePushJob.pushFile - branch on blobBasedPush

**File:** `clients/venice-push-job/src/main/java/com/linkedin/venice/hadoop/VenicePushJob.java`

In `pushFile()` (around lines 832-894), add a new branch:
```java
if (pushJobSetting.blobBasedPush) {
  // No VeniceWriter, no SOP/EOP control messages
  runJobAndUpdateStatus();  // Uses blob-specific DataWriterComputeJob
  controllerClient.notifyBlobPushComplete(pushJobSetting.storeResponse.getName(), pushJobSetting.version);
} else if (pushJobSetting.isIncrementalPush) {
  // existing incremental push flow
} else {
  // existing batch Kafka flow
}
```

### 6d. BlobPartitionWriter

**File (new):** `clients/venice-push-job/src/main/java/com/linkedin/venice/spark/datawriter/writer/BlobPartitionWriter.java`

Replaces `SparkPartitionWriter` for blob pushes. Instead of writing to Kafka via VeniceWriter:
1. Creates a RocksDB `SstFileWriter` (from `org.rocksdb.SstFileWriter`)
2. For each key-value pair (already sorted by Spark), writes to SST file
3. After all records for this partition are written, closes SST file
4. Uploads SST file to blob storage via `BlobStorageClient`
5. Writes partition metadata (record count, checksum, byte size)

Key points:
- Data is already sorted by Venice partitioner in Spark (same as existing flow)
- No chunking needed (no Kafka message size limit)
- Compression applied at value level consistent with store config
- Uses `EnvOptions` and `Options` matching server-side RocksDB config

### 6e. BlobPartitionWriterFactory

**File (new):** `clients/venice-push-job/src/main/java/com/linkedin/venice/spark/datawriter/writer/BlobPartitionWriterFactory.java`

Spark `MapPartitionsFunction` that creates `BlobPartitionWriter` instances per partition. Analogous to `SparkPartitionWriterFactory`.

### 6f. DataWriterSparkJob modification (or subclass)

**File:** `clients/venice-push-job/src/main/java/com/linkedin/venice/spark/datawriter/jobs/DataWriterSparkJob.java`

Override `createPartitionWriterFactory()` to return `BlobPartitionWriterFactory` when `blobBasedPush=true`. Or create a `BlobDataWriterSparkJob` subclass and set `pushJobSetting.dataWriterComputeJobClass` accordingly.

### 6g. Version manifest upload

After `runJobAndUpdateStatus()` completes, VPJ aggregates per-partition metadata (from Spark accumulators or similar) into a version manifest and uploads it:
```json
{
  "storeName": "...",
  "version": 5,
  "partitions": {
    "0": {"recordCount": 1000, "checksum": "abc123", "byteSize": 5242880},
    "1": {"recordCount": 950, "checksum": "def456", "byteSize": 4980736}
  }
}
```

**Verification:** Unit test with `LocalFsBlobStorageClient`: run a small push job, verify SST files are generated, manifest is written, `notifyBlobPushComplete` is called.

**Output contract:** After VPJ completes, blob storage contains valid SST files per partition + manifest. Controller has been notified.

---

## WP7: Server - Blob Ingestion Path

**Goal:** Add a new ingestion code path in the server that downloads SST files from blob storage and ingests them into RocksDB via `ingestExternalFile()`.

**Depends on:** WP2, WP3

**Scope:**

### 7a. DefaultIngestionBackend - add blob ingestion branch

**File:** `clients/da-vinci-client/src/main/java/com/linkedin/davinci/ingestion/DefaultIngestionBackend.java`

In `startConsumption()`, add a new branch BEFORE the existing blob transfer check:
```java
if (version.isBlobBasedIngestion()) {
  startBlobIngestion(storeConfig, partition, version);
  return;
}
// existing P2P blob transfer and Kafka paths unchanged
```

### 7b. BlobIngestionTask

**File (new):** `clients/da-vinci-client/src/main/java/com/linkedin/davinci/ingestion/BlobIngestionTask.java`

Async task that:
1. Watches `/PushControlSignals/$topic` ZK node for `BLOB_UPLOAD_COMPLETE` signal (via `PushControlSignal` accessor)
2. Downloads SST file from `version.getBlobStorageUri()` via `BlobStorageClient`
3. Validates checksum against manifest
4. Opens storage partition via `StorageService.openStoreForNewPartition()`
5. Ingests SST file via `RocksDB.ingestExternalFile()` (reference: `RocksDBSstFileWriter.ingestSSTFiles()` at `clients/da-vinci-client/src/main/java/com/linkedin/davinci/store/rocksdb/RocksDBSstFileWriter.java`)
6. Reports status via `IngestionNotificationDispatcher`:
   - `END_OF_PUSH_RECEIVED` after ingestion
   - `COMPLETED` after state transitions

### 7c. RocksDB SST ingestion integration

The existing `RocksDBSstFileWriter.ingestSSTFiles()` method shows the pattern:
```java
rocksDB.ingestExternalFile(sstFileList, ingestExternalFileOptions);
```

`BlobIngestionTask` should reuse this pattern but with externally-downloaded SST files instead of locally-generated ones. Key concern: the SST file must be compatible with the server's RocksDB configuration (table format, compression codec). The version metadata should carry any necessary RocksDB config parameters.

### 7d. Error handling and retry

- Download failure: retry with exponential backoff (configurable max retries via server config)
- Checksum mismatch: re-download, then report ERROR if persistent
- Ingestion failure: report ERROR to controller
- Use existing `VeniceServerConfig` for retry configuration

### 7e. Status reporting

Uses the existing `IngestionNotificationDispatcher` (same as Kafka path). State machine:
```
NOT_STARTED -> STARTED -> (watch PushControlSignal for BLOB_UPLOAD_COMPLETE) -> (download + ingest) -> END_OF_PUSH_RECEIVED -> COMPLETED
```

Report ERROR on unrecoverable failure so `PushStatusDecider` can handle it.

**Verification:** Unit test with `LocalFsBlobStorageClient` and a pre-generated SST file: verify download, ingestion, and status reporting.

**Output contract:** When a server receives a partition assignment for a blob-based version, it autonomously downloads and ingests the SST file, then reports COMPLETED.

---

## WP8: Integration Test Skeleton

**Goal:** Create an end-to-end integration test that exercises the full blob push flow using `LOCAL_FS` blob storage.

**Depends on:** WP4, WP5, WP6, WP7

**Scope:**

1. Create a test store with `blobBasedIngestionEnabled=true`
2. Run a VPJ that generates SST files to a temp directory (using `LOCAL_FS` blob storage)
3. Verify controller created version with `BATCH_BLOB` push type, no Kafka topic
4. Verify SST files and manifest exist in the expected blob path
5. Verify servers download, ingest, and report COMPLETED
6. Verify the store is queryable after version swap

**File (new):** test file in the integration test module (e.g., `internal/venice-test-common/src/integrationTest/.../BlobBasedIngestionEndToEndTest.java`)

This test can initially be a skeleton that validates the happy path. Edge cases (failures, retries, cleanup) can be added incrementally.

---

## WP9: Buffer Reuse in prependSchemaId (Scalability)

**Goal:** Eliminate per-record `byte[]` allocation in `prependSchemaId()` using a reusable heap buffer.

**Depends on:** WP6

**Context:** This addresses scalability issue #7 from the scalability review. Every record written to an SST file required a new `byte[]` allocation to prepend the 4-byte schema ID. For large partitions (millions of records), this creates significant GC pressure.

**Scope:**

### 9a. Reusable heap buffer

**File:** `clients/venice-push-job/src/main/java/com/linkedin/venice/spark/datawriter/writer/BlobPartitionWriter.java`

Add an instance-level reusable buffer that grows monotonically:

```java
private byte[] reusableValueBuffer = new byte[4096]; // initial 4KB
```

New method `prependSchemaIdReusable(byte[] value, int schemaId, byte[] buffer)`:
- If `buffer.length < SCHEMA_ID_PREFIX_SIZE + value.length`, grow to `Math.max(requiredSize, buffer.length * 2)`
- Write 4-byte big-endian schema ID at offset 0
- `System.arraycopy()` value at offset 4
- Return the buffer (caller uses only the first `requiredSize` bytes)

The static `prependSchemaId()` method is retained for test use. Production `processRows()` uses `prependSchemaIdReusable()`.

**Design choice: heap `byte[]` over `DirectByteBuffer`:**
- Direct buffers are off-heap, limited by `-XX:MaxDirectMemorySize`, and can conflict with Spark shuffle / Netty on shared executors
- Growing a direct buffer requires allocating a new one (expensive, not promptly GC'd)
- Heap buffer growth via `new byte[newSize]` is cheap and GC-friendly

**Steady state:** For stores with uniform value sizes (typical), the buffer stabilizes at max value size after a few records. Once stabilized, zero allocations per record.

**Verification:** Unit tests verify byte-for-byte equivalence with the non-reuse path, buffer growth with variable-size values, and zero-allocation steady state with uniform values.

**Output contract:** `BlobPartitionWriter.processRows()` produces identical SST output with significantly less GC pressure.

---

## WP10: SST File Splitting (Scalability)

**Goal:** Split SST output into multiple files (default 64MB each), uploading and deleting each before starting the next. Bounds executor disk usage and eliminates all-or-nothing upload failure.

**Depends on:** WP6

**Context:** This addresses scalability issues #1 and #2 from the scalability review. The MVP produced a single SST file per partition, which could grow to arbitrary size (10s of GB for large partitions), consuming executor disk and requiring all-or-nothing upload.

**Scope:**

### 10a. SST size tracking and splitting

**File:** `clients/venice-push-job/src/main/java/com/linkedin/venice/spark/datawriter/writer/BlobPartitionWriter.java`

- Add `int currentSstFileIndex` and `long sstFileSizeThresholdBytes` fields
- Read threshold from config `blob.sst.file.size.threshold.bytes` (default 64MB via `VenicePushJobConstants.DEFAULT_BLOB_SST_FILE_SIZE_THRESHOLD_BYTES = 64 * 1024 * 1024`)
- In `processRows()`, every `FILE_SIZE_CHECK_INTERVAL` (1000) records, call `sstFileWriter.fileSize()` to check actual on-disk SST size. This amortizes JNI overhead — checking every record would be expensive.
- When threshold exceeded: finish current SST → upload to blob storage → delete temp file → increment `currentSstFileIndex` → create new `SstFileWriter`
- After the loop, finish and upload the last file if it has data

### 10b. File naming

**File:** `clients/venice-push-job/src/main/java/com/linkedin/venice/spark/datawriter/writer/BlobPartitionWriter.java`

Add static method:
```java
static String composeSstFileName(int index) {
    return "data_" + index + ".sst";
}
```

Produces: `data_0.sst`, `data_1.sst`, `data_2.sst`, etc.

### 10c. Config propagation

**File:** `clients/venice-push-job/src/main/java/com/linkedin/venice/vpj/VenicePushJobConstants.java`

```java
public static final String BLOB_SST_FILE_SIZE_THRESHOLD_BYTES = "blob.sst.file.size.threshold.bytes";
public static final long DEFAULT_BLOB_SST_FILE_SIZE_THRESHOLD_BYTES = 64 * 1024 * 1024; // 64MB
```

**File:** `clients/venice-push-job/src/main/java/com/linkedin/venice/spark/datawriter/jobs/BlobDataWriterSparkJob.java`

Propagate `BLOB_SST_FILE_SIZE_THRESHOLD_BYTES` from `PushJobSetting` to Spark session config in `configure()`.

**File:** `clients/venice-push-job/src/main/java/com/linkedin/venice/hadoop/PushJobSetting.java`

Add `public long blobSstFileSizeThresholdBytes;`

**File:** `clients/venice-push-job/src/main/java/com/linkedin/venice/hadoop/VenicePushJob.java`

Parse the threshold from properties, populate `PushJobSetting`.

### 10d. Sorted key invariant

SST splitting preserves key ordering because `processRows()` receives data pre-sorted by `repartitionAndSortWithinPartitions()` and files are split sequentially. File N's last key is strictly less than file N+1's first key. No additional sorting needed.

### 10e. Server-side backward compatibility

`BlobIngestionTask.downloadSSTFiles()` already downloads the entire partition directory and collects all `.sst` files into a list. Multi-file partitions are handled automatically — no server changes needed.

**Config validation:** `blob.sst.file.size.threshold.bytes` must be > 0.

**Verification:** Unit tests with small threshold (1KB) verify multiple `data_N.sst` files are uploaded, key ranges are non-overlapping and ordered across files, and existing single-file tests still pass.

**Output contract:** Push jobs produce bounded-size SST files. Servers ingest them all via bulk `ingestExternalSSTFiles(allFiles)`.

---

## WP11: Download Jitter + Concurrency Cap (Scalability)

**Goal:** Prevent thundering herd when `BLOB_UPLOAD_COMPLETE` fires by adding random jitter and a server-wide download concurrency semaphore.

**Depends on:** WP7

**Context:** This addresses scalability issue #5 from the scalability review. When the controller emits `BLOB_UPLOAD_COMPLETE`, all servers for all partitions wake up simultaneously, potentially overwhelming blob storage.

**Scope:**

### 11a. Random download jitter

**File:** `clients/da-vinci-client/src/main/java/com/linkedin/davinci/ingestion/BlobIngestionTask.java`

After `waitForBlobUploadComplete()`, add:
```java
void applyDownloadJitter() throws InterruptedException {
    if (downloadJitterMaxMs > 0) {
        long jitterMs = ThreadLocalRandom.current().nextLong(downloadJitterMaxMs);
        Thread.sleep(jitterMs);
    }
}
```

Default jitter: 0–5000ms. Set to 0 to disable.

### 11b. Server-wide download semaphore

**File:** `clients/da-vinci-client/src/main/java/com/linkedin/davinci/ingestion/DefaultIngestionBackend.java`

Create a shared `Semaphore downloadSemaphore` (default 8 permits) passed to each `BlobIngestionTask`.

**File:** `clients/da-vinci-client/src/main/java/com/linkedin/davinci/ingestion/BlobIngestionTask.java`

New method `downloadWithConcurrencyLimit()`:
- Acquire permit from `downloadSemaphore` before download
- Release in `finally` after download completes (not after ingest — bounds concurrent network requests, not disk usage)
- If semaphore is null (max concurrent downloads = 0), skip permit acquisition

### 11c. Config keys

**File:** `internal/venice-common/src/main/java/com/linkedin/venice/ConfigKeys.java`

```java
public static final String BLOB_INGESTION_DOWNLOAD_JITTER_MAX_MS = "blob.ingestion.download.jitter.max.ms";
public static final String BLOB_INGESTION_MAX_CONCURRENT_DOWNLOADS = "blob.ingestion.max.concurrent.downloads";
```

**File:** `clients/da-vinci-client/src/main/java/com/linkedin/davinci/config/VeniceServerConfig.java`

```java
blobIngestionDownloadJitterMaxMs =
    Math.min(Math.max(serverProperties.getLong(BLOB_INGESTION_DOWNLOAD_JITTER_MAX_MS, 5000L), 0), 60000);
blobIngestionMaxConcurrentDownloads =
    Math.min(Math.max(serverProperties.getInt(BLOB_INGESTION_MAX_CONCURRENT_DOWNLOADS, 8), 0), 256);
```

**Config validation:**
- `jitterMaxMs`: clamped to [0, 60000]. 0 disables jitter.
- `maxConcurrentDownloads`: clamped to [0, 256]. 0 means no limit (no semaphore created).

### 11d. Request rate analysis

Worst-case: 4096 partitions × 3 replicas ÷ ~30 servers ≈ 410 partitions per server. With 8-permit semaphore per server, fleet-wide concurrent downloads = 30 × 8 = 240. S3 per-prefix limit is ~5,500 GET/s. No cross-server coordination needed.

**Rollback:** Set jitter to 0 and concurrent downloads to a high value (e.g. 256) to effectively disable. No redeployment needed.

**Verification:** Unit tests verify jitter is within [0, jitterMaxMs), jitter=0 skips sleep, and concurrency cap blocks excess downloads.

**Output contract:** Download requests are spread in time and bounded in concurrency per server.

---

## WP12: Server-Side Thread Pool Separation (Scalability)

**Goal:** Decouple I/O-bound downloads from CPU/disk-bound ingestion using separate thread pools, enabling cross-partition parallelism.

**Depends on:** WP7

**Context:** This addresses scalability issues #3 and #4 from the scalability review. The MVP used a single executor for both download and ingest. Separating these enables partition A to ingest while partition B downloads.

**Scope:**

### 12a. Two-pool architecture

**File:** `clients/da-vinci-client/src/main/java/com/linkedin/davinci/ingestion/DefaultIngestionBackend.java`

Replace single `blobIngestionExecutor` with two pools:
- `blobDownloadExecutor`: 8 threads (default), configurable via `blob.ingestion.download.pool.size`. I/O-bound, higher concurrency.
- `blobIngestExecutor`: 4 threads (default), configurable via `blob.ingestion.ingest.pool.size`. CPU/disk-bound, lower concurrency.

Both use `DaemonThreadFactory` with descriptive names (`blob-download`, `blob-ingest`).

### 12b. Backpressure semaphore

**File:** `clients/da-vinci-client/src/main/java/com/linkedin/davinci/ingestion/DefaultIngestionBackend.java`

Add `Semaphore pendingIngestSemaphore` with permits = `ingestPoolSize × 2` (default 8). Bounds the number of partitions that have been downloaded but not yet ingested, limiting disk usage from staging SST files.

### 12c. BlobIngestionTask — revised run() flow

**File:** `clients/da-vinci-client/src/main/java/com/linkedin/davinci/ingestion/BlobIngestionTask.java`

Add constructor params: `ExecutorService ingestExecutor`, `Semaphore pendingIngestSemaphore`.

New method `submitAndAwaitIngest(List<String> sstFilePaths)`:
1. Acquire `pendingIngestSemaphore` permit (backpressure — wait if too many partitions pending ingest)
2. Submit `ingestSSTFiles()` to `blobIngestExecutor`, get `Future`
3. `Future.get()` — block until ingest completes (download pool thread freed for other partitions)
4. Release `pendingIngestSemaphore` permit in `finally` block

If `ingestExecutor` is null (e.g. in tests), falls back to inline ingest on the current thread.

**Bulk ingest:** All SST files for a partition are ingested in one `ingestExternalSSTFiles(allFiles)` call. Within-partition pipelining was considered but rejected because `ingestExternalSSTFiles()` is `synchronized` per partition, and RocksDB ingests multiple non-overlapping files more efficiently in a single pass.

### 12d. Full 7-step run() flow

```
1. reportStarted()
2. waitForBlobUploadComplete()           -- ZK watcher + immediate check
3. applyDownloadJitter()                 -- random sleep [0, jitterMaxMs)
4. downloadWithConcurrencyLimit()        -- semaphore-gated download with retry
5. submitAndAwaitIngest(sstFilePaths)    -- backpressure semaphore + ingest pool
6. reportEndOfPushReceived()
7. reportCompleted()
```

### 12e. Config keys

**File:** `internal/venice-common/src/main/java/com/linkedin/venice/ConfigKeys.java`

```java
public static final String BLOB_INGESTION_DOWNLOAD_POOL_SIZE = "blob.ingestion.download.pool.size";
public static final String BLOB_INGESTION_INGEST_POOL_SIZE = "blob.ingestion.ingest.pool.size";
```

**File:** `clients/da-vinci-client/src/main/java/com/linkedin/davinci/config/VeniceServerConfig.java`

```java
blobIngestionDownloadPoolSize = Math.max(serverProperties.getInt(BLOB_INGESTION_DOWNLOAD_POOL_SIZE, 8), 1);
blobIngestionIngestPoolSize = Math.max(serverProperties.getInt(BLOB_INGESTION_INGEST_POOL_SIZE, 4), 1);
```

Both pool sizes validated to be >= 1 via `Math.max(value, 1)`.

### 12f. Graceful shutdown

**File:** `clients/da-vinci-client/src/main/java/com/linkedin/davinci/ingestion/DefaultIngestionBackend.java`

In `close()`:
```java
blobDownloadExecutor.shutdown();
blobIngestExecutor.shutdown();
blobDownloadExecutor.awaitTermination(30, SECONDS);
blobIngestExecutor.awaitTermination(30, SECONDS);
blobDownloadExecutor.shutdownNow();
blobIngestExecutor.shutdownNow();
```

**Rollback:** Revert to single pool by setting both config values to the same number, or revert the code. No data format changes.

**Verification:** Unit tests verify download and ingest execute on different thread pools (captured thread names), `pendingIngestSemaphore` blocks when ingest pool is saturated, graceful shutdown waits for in-flight ingests, and ingest failure is reported correctly.

**Output contract:** Blob ingestion uses separate download (8 threads) and ingest (4 threads) pools with backpressure, enabling cross-partition parallelism.

---

## Parallelism Summary

### MVP (WP1–WP8) — ALL COMPLETED

| WP | Status | Blocked by |
|----|--------|------------|
| WP1 (Avro Schema) | COMPLETED | - |
| WP2 (Data Model) | COMPLETED | WP1 |
| WP3 (BlobStorageClient) | COMPLETED | - |
| WP4 (Controller version creation) | COMPLETED | WP2 |
| WP5 (Controller completion + cleanup) | COMPLETED | WP2, WP3 |
| WP6 (VPJ blob push) | COMPLETED | WP2, WP3 |
| WP7 (Server blob ingestion) | COMPLETED | WP2, WP3 |
| WP8 (Integration test) | Not started | WP4, WP5, WP6, WP7 |

### Scalability (WP9–WP12) — ALL COMPLETED

| WP | Status | Blocked by |
|----|--------|------------|
| WP9 (Buffer reuse) | COMPLETED | WP6 |
| WP10 (SST file splitting) | COMPLETED | WP6 |
| WP11 (Jitter + concurrency cap) | COMPLETED | WP7 |
| WP12 (Thread pool separation) | COMPLETED | WP7 |

**Implementation order used:** WP9 → WP10 → WP11 → WP12 (all committed together in `fd1d296bf`)

**Minimum critical path:** WP1 → WP2 → WP6 (VPJ) + WP7 (server) → WP9–WP12 (scalability)
