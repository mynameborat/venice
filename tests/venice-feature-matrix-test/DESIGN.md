# Venice Feature Matrix Integration Test - Design Document

## Overview

This module implements a 3-way combinatorial integration test covering 42 feature dimensions across Venice's write path,
read path, server, controller, and router components.

## Problem Statement

Venice has 600+ configuration keys across routers, servers, controllers, push jobs, and clients. These are tested in
isolation across ~130 E2E tests. There is no systematic way to test cross-layer interactions between feature flags,
leading to coverage gaps for multi-flag interaction bugs.

## Solution

Use PICT (Pairwise Independent Combinatorial Testing) to generate a covering array of test cases that exercises all
3-way interactions across 42 feature dimensions, producing approximately 300-500 test cases.

## 42-Dimension Feature Matrix

### Write Path (W1-W13)

| ID  | Dimension             | Values                              | Constraint                         |
| --- | --------------------- | ----------------------------------- | ---------------------------------- |
| W1  | Data Flow Topology    | Batch-only / Hybrid / Nearline-only | --                                 |
| W2  | Native Replication    | on / off                            | --                                 |
| W3  | Active-Active         | on / off                            | W2=on                              |
| W4  | Write Compute         | on / off                            | W3=on                              |
| W5  | Chunking              | on / off                            | Auto when W4=on                    |
| W6  | RMD Chunking          | on / off                            | W5=on AND W3=on                    |
| W7  | Incremental Push      | on / off                            | W3=on                              |
| W8  | Compression           | NO_OP / GZIP / ZSTD_WITH_DICT       | ZSTD invalid when W1=Nearline-only |
| W9  | Deferred Version Swap | on / off                            | C5=on                              |
| W10 | Target Region Push    | on / off                            | Multi-region only                  |
| W11 | Push Engine           | MapReduce / Spark native            | --                                 |
| W12 | TTL Repush            | on / off                            | Kafka source repush                |
| W13 | Separate RT Topic     | on / off                            | W7=on                              |

### Read Path (R1-R6)

| ID  | Dimension                  | Values                               | Constraint               |
| --- | -------------------------- | ------------------------------------ | ------------------------ |
| R1  | Client Type                | Thin / Fast / DaVinci                | --                       |
| R2  | Read Compute               | on / off                             | Store readComputeEnabled |
| R3  | DaVinci Storage Class      | MEMORY_BACKED_BY_DISK / MEMORY / N/A | R1=DaVinci               |
| R4  | Fast Client Routing        | LEAST_LOADED / HELIX_ASSISTED / N/A  | R1=Fast                  |
| R5  | Long-tail Retry (client)   | on / off                             | R1=Thin or Fast          |
| R6  | DaVinci Record Transformer | on / off / N/A                       | R1=DaVinci               |

### Server (S1-S6)

| ID  | Dimension                 | Values   | Constraint      |
| --- | ------------------------- | -------- | --------------- |
| S1  | Parallel Batch Get        | on / off | --              |
| S2  | Fast Avro                 | on / off | --              |
| S3  | AA/WC Parallel Processing | on / off | W3=on AND W4=on |
| S4  | Blob Transfer             | on / off | --              |
| S5  | Quota Enforcement         | on / off | --              |
| S6  | Adaptive Throttler        | on / off | --              |

### Controller (C1-C9)

| ID  | Dimension                         | Values   | Constraint |
| --- | --------------------------------- | -------- | ---------- |
| C1  | AA Default for Hybrid             | on / off | --         |
| C2  | WC Auto for Hybrid+AA             | on / off | --         |
| C3  | Inc Push Auto for Hybrid+AA       | on / off | --         |
| C4  | Separate RT Auto for Inc Push     | on / off | --         |
| C5  | Deferred Version Swap Service     | on / off | --         |
| C6  | Schema Validation                 | on / off | --         |
| C7  | Backup Version Cleanup            | on / off | --         |
| C8  | System Store Auto-Materialization | on / off | --         |
| C9  | Superset Schema Generation        | on / off | --         |

### Router (RT1-RT8)

| ID  | Dimension             | Values                        | Constraint |
| --- | --------------------- | ----------------------------- | ---------- |
| RT1 | Read Throttling       | on / off                      | --         |
| RT2 | Early Throttle        | on / off                      | RT1=on     |
| RT3 | Smart Long-tail Retry | on / off                      | --         |
| RT4 | Routing Strategy      | LEAST_LOADED / HELIX_ASSISTED | --         |
| RT5 | Latency-based Routing | on / off                      | --         |
| RT6 | Client Decompression  | on / off                      | --         |
| RT7 | HTTP/2 Inbound        | on / off                      | --         |
| RT8 | Connection Warming    | on / off                      | --         |

## Architecture

### Cluster Grouping Strategy

Server, router, and controller configs are set at startup and cannot change per-store. Test cases are grouped by unique
(S1-S6, RT1-RT8, C1-C9) tuples. One cluster instance is created per unique tuple. The 3-way algorithm naturally limits
this to ~10-20 clusters.

### Test Flow

```
FeatureMatrixIntegrationTest
|-- @Factory(dataProvider = "clusterConfigs")
|   Creates one test instance per unique (S,RT,C) cluster config
|
|-- @BeforeClass
|   FeatureMatrixClusterSetup.create()
|   Creates multi-region cluster via ServiceFactory
|
|-- @DataProvider("storeAndClientMatrix")
|   Returns all (W,R) test cases for this cluster's (S,RT,C) config
|
|-- @Test(dataProvider = "storeAndClientMatrix")
|   For each (W,R) combination:
|   1. StoreConfigurator.create() - store with W flags
|   2. Write data - BatchPush/Streaming/IncrementalPush
|   3. Validate reads - DataIntegrity/ReadCompute/WriteCompute
|   4. Cleanup store
|
|-- @AfterClass
    Tear down cluster
```

### Dependency Chains

**Chain 1: Replication Hierarchy**

```
Native Replication (NR)
  -- Active-Active (AA) [requires NR]
       |-- Incremental Push [requires AA]
       |-- Write Compute [auto for hybrid+AA]
       |    |-- Chunking [auto when WC]
       |    -- RMD Chunking [auto when WC+AA]
       -- Separate RT Topic [optional, for inc push]
```

**Chain 2: Hybrid Store** - HybridStoreConfig -> RT Topic, Separate RT, Disk Quota

**Chain 3: Compression** - ZSTD_WITH_DICT -> Dict training -> Router dict retrieval -> Client decompression

**Chain 4: Blob Transfer** - Store flag -> Version flag -> Server flag (cross-layer)

**Chain 5: Chunking** - chunkingEnabled -> rmdChunkingEnabled (requires chunking + AA)

### Failure Classification

| Validation Step Fails At   | Likely Component         |
| -------------------------- | ------------------------ |
| Store creation             | Controller               |
| Batch push                 | PushJob / Server         |
| RT write / ingestion       | Server (ingestion)       |
| Incremental push           | Server / Controller      |
| Single get returns null    | Server / Router          |
| Single get wrong value     | Server (data corruption) |
| Batch get missing keys     | Router (scatter-gather)  |
| Read compute wrong result  | Server (compute engine)  |
| Write compute wrong result | Server (AA/WC merge)     |
| Decompression failure      | Router / Client          |
| Chunk reassembly failure   | Client / Server          |

## Reporting

- **HTML Report**: `build/reports/feature-matrix-report.html`

  - Failures by component
  - Failures by dimension value (top 20)
  - Failures by dimension pair (top 20)
  - Individual failure details

- **JSON Report**: `build/reports/feature-matrix-results.json`
  - Structured data for future regression tooling
  - Deterministic test case IDs for run-to-run comparison
