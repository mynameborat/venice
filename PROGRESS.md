# Venice Feature Flag Catalog & Integration Test Matrix - Progress

## Branch: `feature/feature-flag-catalog-and-matrix-test`

---

## Phase 0: Persist Design & Module README

**Status**: COMPLETED

**Summary**:

- Created `tests/venice-feature-matrix-test/DESIGN.md` — Full design document capturing all 42 dimensions, constraints,
  architecture, cluster grouping strategy, test flow, dependency chains, and failure classification.
- Created `tests/venice-feature-matrix-test/README.md` — User-facing guide with prerequisites (PICT tool), how to run
  tests, how to add new dimensions (step-by-step), how to read reports, architecture diagram, and AI agent instructions.

**Files created**:

- `tests/venice-feature-matrix-test/DESIGN.md`
- `tests/venice-feature-matrix-test/README.md`

---

## Phase 1: Feature Flag Catalog

**Status**: COMPLETED

**Summary**:

- Created `docs/dev-guide/feature-flag-catalog.md` (989 lines) cataloging Venice's 618 config keys from
  `ConfigKeys.java`.
- Organized into 10 sections: Store Profile Prerequisites, Replication & Multi-Region, Router Read Path, Server
  Ingestion & Storage, Server AA/WC Optimization, Server Kafka Fetch, Controller Lifecycle, Controller System Stores,
  PubSub Infrastructure, Independent Knobs.
- Documented per-flag entries with Source, Type, Default, Scope, Set By, Affects, Path Impact, Dependencies, Dependents,
  Description.
- Mapped 5 dependency chains with code references (ParentControllerConfigUpdateUtils, VeniceParentHelixAdmin).
- Built Store Profile Prerequisites Map (9 cross-layer prerequisites).
- Documented Mutual Exclusions (6 enum groups) and 8 hard constraints.
- Created Auto-Enablement Rules table (5 rules with source locations).
- Built Component Impact Matrix (7 components x 14 flag categories).
- Defined 42-Dimension Feature Matrix with 21 PICT constraints.

**Files created**:

- `docs/dev-guide/feature-flag-catalog.md`

---

## Phase 2: New Module Setup

**Status**: COMPLETED

**Summary**:

- Created `tests/venice-feature-matrix-test/` module directory structure.
- Created `build.gradle` with dependencies on `venice-test-common` (including `integrationTestUtils` configuration for
  access to ServiceFactory, VeniceTwoLayerMultiRegionMultiClusterWrapper, etc.), all client modules, service modules,
  and test libraries.
- Added `include 'tests:venice-feature-matrix-test'` to `settings.gradle`.
- Configured `featureMatrixTest` Gradle task with TestNG listener, forkEvery=1, 4GB heap, and report output directory.
- Default `test` task excludes all tests (feature matrix tests run only via dedicated task).

**Files created/modified**:

- `tests/venice-feature-matrix-test/build.gradle`
- `settings.gradle` (modified — added include)

---

## Phase 3: PICT Model & Test Generation

**Status**: COMPLETED

**Summary**:

- Created `feature-matrix-model.pict` with all 42 dimensions across 5 component groups (Write Path 13, Read Path 6,
  Server 6, Controller 9, Router 8) and 21 constraints encoding the replication hierarchy, compression rules,
  cross-layer prerequisites, client-specific rules, and router/server dependencies.
- Implemented `FeatureDimensions.java` — Enums for all dimension value types (Topology, OnOff, Compression, PushEngine,
  ClientType, DaVinciStorageClass, RoutingStrategy, OptionalOnOff) plus `DimensionId` enum with 42 entries mapping to
  PICT column names and component groups.
- Implemented `TestCaseConfig.java` — Immutable config class with all 42 typed fields, parsing from string values,
  cluster config key generation for (S,RT,C) grouping, and human-readable test name encoding.
- Implemented `PictModelParser.java` — Reads PICT-generated TSV files from classpath or filesystem, maps headers to
  DimensionId enums, groups test cases by cluster config key.

**Files created**:

- `tests/venice-feature-matrix-test/src/test/resources/feature-matrix-model.pict`
- `tests/venice-feature-matrix-test/src/test/java/com/linkedin/venice/featurematrix/model/FeatureDimensions.java`
- `tests/venice-feature-matrix-test/src/test/java/com/linkedin/venice/featurematrix/model/TestCaseConfig.java`
- `tests/venice-feature-matrix-test/src/test/java/com/linkedin/venice/featurematrix/model/PictModelParser.java`

---

## Phase 4: Cluster & Store Setup

**Status**: COMPLETED

**Summary**:

- Implemented `ClusterConfigBuilder.java` — Builds Properties for Server (S1-S6), Router (RT1-RT8), and Controller
  (C1-C9) dimensions using verified ConfigKeys constants (SERVER_ENABLE_PARALLEL_BATCH_GET,
  SERVER_COMPUTE_FAST_AVRO_ENABLED, SERVER_AA_WC_WORKLOAD_PARALLEL_PROCESSING_ENABLED, ENABLE_BLOB_TRANSFER,
  SERVER_QUOTA_ENFORCEMENT_ENABLED, SERVER_ADAPTIVE_THROTTLER_ENABLED, ROUTER_ENABLE_READ_THROTTLING,
  ROUTER_EARLY_THROTTLE_ENABLED, ROUTER_SMART_LONG_TAIL_RETRY_ENABLED,
  ROUTER_HELIX_ASSISTED_ROUTING_GROUP_SELECTION_STRATEGY, ROUTER_LATENCY_BASED_ROUTING_ENABLED,
  ROUTER_CLIENT_DECOMPRESSION_ENABLED, ROUTER_HTTP2_INBOUND_ENABLED, ROUTER_HTTPASYNCCLIENT_CONNECTION_WARMING_ENABLED,
  and all controller keys).
- Implemented `FeatureMatrixClusterSetup.java` — Creates multi-region cluster (2 regions, 1 cluster, 2 servers, 1
  router) via `ServiceFactory.getVeniceTwoLayerMultiRegionMultiClusterWrapper` using
  VeniceMultiRegionClusterCreateOptions builder.
- Implemented `StoreConfigurator.java` — Creates/configures stores via `UpdateStoreQueryParams` from W-dimensions
  (topology/hybrid config, NR, AA, WC, chunking, RMD chunking, incremental push, compression, separate RT topic, read
  compute, partition count).
- Implemented `ClientFactory.java` — Creates Thin/Fast/DaVinci clients from R-dimensions using
  `ClientConfig.defaultGenericClientConfig()` with retry support. Fixed generic type bounds
  (`ClientConfig<T extends SpecificRecord>` requires raw type for generic clients).

**Files created**:

- `tests/venice-feature-matrix-test/src/test/java/com/linkedin/venice/featurematrix/setup/ClusterConfigBuilder.java`
- `tests/venice-feature-matrix-test/src/test/java/com/linkedin/venice/featurematrix/FeatureMatrixClusterSetup.java`
- `tests/venice-feature-matrix-test/src/test/java/com/linkedin/venice/featurematrix/setup/StoreConfigurator.java`
- `tests/venice-feature-matrix-test/src/test/java/com/linkedin/venice/featurematrix/setup/ClientFactory.java`

---

## Phase 5: Write & Read Executors

**Status**: COMPLETED

**Summary**:

- Implemented `BatchPushExecutor.java` — Executes batch push with support for push engine type (MapReduce/Spark),
  deferred version swap (W9), target region push (W10), and TTL repush (W12). Builds VPJ properties and delegates to
  IntegrationTestPushUtils at runtime.
- Implemented `StreamingWriteExecutor.java` — Executes streaming writes to RT topics for hybrid/nearline stores, with
  support for write compute partial updates (W4) and AA records (W3).
- Implemented `IncrementalPushExecutor.java` — Executes incremental push for W7=on stores, producing records to the RT
  topic with incremental push metadata.

**Files created**:

- `tests/venice-feature-matrix-test/src/test/java/com/linkedin/venice/featurematrix/write/BatchPushExecutor.java`
- `tests/venice-feature-matrix-test/src/test/java/com/linkedin/venice/featurematrix/write/StreamingWriteExecutor.java`
- `tests/venice-feature-matrix-test/src/test/java/com/linkedin/venice/featurematrix/write/IncrementalPushExecutor.java`

---

## Phase 6: Reporting

**Status**: COMPLETED

**Summary**:

- Implemented `FailureReport.java` — Per-failure data class capturing testCaseId, all 42 dimensions, validation step,
  likely component (classified from 12 failure categories), error message, stack trace, duration.
- Implemented `FeatureMatrixReportAggregator.java` — Aggregates failures and generates reports grouped by dimension (top
  20), component, dimension-pair (2-way combinations, top 20). Produces HTML report (`feature-matrix-report.html`) with
  styled tables and JSON report (`feature-matrix-results.json`) with stable schema for regression tooling.
- Implemented `FeatureMatrixReportListener.java` — TestNG `ITestListener` that captures 42-dimensional context per
  failure, infers validation step from exception messages, classifies component, and triggers report generation on suite
  finish. Fixed for TestNG 6.x compatibility (`onTestFailedButWithinSuccessPercentage`).

**Files created**:

- `tests/venice-feature-matrix-test/src/test/java/com/linkedin/venice/featurematrix/reporting/FailureReport.java`
- `tests/venice-feature-matrix-test/src/test/java/com/linkedin/venice/featurematrix/reporting/FeatureMatrixReportAggregator.java`
- `tests/venice-feature-matrix-test/src/test/java/com/linkedin/venice/featurematrix/reporting/FeatureMatrixReportListener.java`

---

## Phase 7: Main Test Class

**Status**: COMPLETED

**Summary**:

- Implemented `FeatureMatrixIntegrationTest.java` — Main test class using TestNG
  `@Factory(dataProvider="clusterConfigs")` pattern to create one test instance per unique (S,RT,C) cluster
  configuration. Each instance:
  - `@BeforeClass`: Creates multi-region cluster via FeatureMatrixClusterSetup
  - `@DataProvider("storeAndClientMatrix")`: Returns all (W,R) test cases for this cluster
  - `@Test`: For each combination — creates store, writes data (batch/streaming/incremental per topology), validates
    reads (single get, batch get, read compute, write compute, chunking), cleans up store
  - `@AfterClass`: Tears down cluster
- Test names encode all 42 dimensions for immediate failure identification.

**Files created**:

- `tests/venice-feature-matrix-test/src/test/java/com/linkedin/venice/featurematrix/FeatureMatrixIntegrationTest.java`

---

## Build Verification

**Status**: PASSED

```
JAVA_HOME=/Library/Java/JavaVirtualMachines/jdk17.0.5-msft.jdk/Contents/Home \
  ./gradlew :tests:venice-feature-matrix-test:compileTestJava
BUILD SUCCESSFUL in 3s
```

All 16 Java source files compile clean against Venice's actual APIs.

---

## File Inventory

| #   | File                                            | Lines     | Status             |
| --- | ----------------------------------------------- | --------- | ------------------ |
| 1   | `docs/dev-guide/feature-flag-catalog.md`        | 989       | Created            |
| 2   | `tests/venice-feature-matrix-test/DESIGN.md`    | 170       | Created            |
| 3   | `tests/venice-feature-matrix-test/README.md`    | 117       | Created            |
| 4   | `tests/venice-feature-matrix-test/build.gradle` | 49        | Created            |
| 5   | `settings.gradle`                               | 78        | Modified (+1 line) |
| 6   | `src/test/resources/feature-matrix-model.pict`  | 109       | Created            |
| 7   | `model/FeatureDimensions.java`                  | ~100      | Created            |
| 8   | `model/TestCaseConfig.java`                     | ~330      | Created            |
| 9   | `model/PictModelParser.java`                    | ~110      | Created            |
| 10  | `setup/ClusterConfigBuilder.java`               | ~200      | Created            |
| 11  | `setup/StoreConfigurator.java`                  | ~130      | Created            |
| 12  | `setup/ClientFactory.java`                      | ~130      | Created            |
| 13  | `FeatureMatrixClusterSetup.java`                | ~100      | Created            |
| 14  | `FeatureMatrixIntegrationTest.java`             | ~210      | Created            |
| 15  | `write/BatchPushExecutor.java`                  | ~100      | Created            |
| 16  | `write/StreamingWriteExecutor.java`             | ~60       | Created            |
| 17  | `write/IncrementalPushExecutor.java`            | ~70       | Created            |
| 18  | `validation/DataIntegrityValidator.java`        | ~130      | Created            |
| 19  | `validation/ReadComputeValidator.java`          | ~50       | Created            |
| 20  | `validation/WriteComputeValidator.java`         | ~50       | Created            |
| 21  | `reporting/FailureReport.java`                  | ~100      | Created            |
| 22  | `reporting/FeatureMatrixReportAggregator.java`  | ~200      | Created            |
| 23  | `reporting/FeatureMatrixReportListener.java`    | ~150      | Created            |
| 24  | `PROGRESS.md`                                   | this file | Created            |

---

## Next Steps (to make tests runnable)

1. Install PICT: `brew install pict`
2. Generate test cases:
   `pict src/test/resources/feature-matrix-model.pict /o:3 > src/test/resources/generated-test-cases.tsv`
3. Run: `./gradlew :tests:venice-feature-matrix-test:featureMatrixTest`
