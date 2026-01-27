# Debugging Leader Heartbeat Delays in Venice

## Overview

Leader heartbeat delay measures the replication lag for nearline/hybrid stores in Venice. High heartbeat delays indicate that data written to the real-time (RT) topic is not being consumed and applied to storage in a timely manner, which can lead to stale reads and data inconsistency across regions.

**Primary Metric**: `ingestion.replication.heartbeat.delay`
- **Type**: Histogram
- **Unit**: Milliseconds
- **Source**: `ServerMetricEntity` / `HeartbeatOtelStats`

---

## Quick Diagnosis Prompt

When investigating leader heartbeat delays, use this systematic approach:

```
I'm seeing high leader heartbeat delays for store [STORE_NAME] in cluster [CLUSTER_NAME].

Current symptoms:
- Heartbeat delay: [X] ms (normal: < 1000ms)
- Affected regions: [REGIONS]
- Affected partitions: [PARTITIONS or "all"]
- Started at: [TIMESTAMP]

Please help me:
1. Identify the root cause from the metrics below
2. Determine if this is a producer, consumer, or storage issue
3. Suggest remediation steps
```

---

## Metrics to Investigate

### 1. Primary Heartbeat Metrics

| Metric | Location | What to Look For |
|--------|----------|------------------|
| `ingestion.replication.heartbeat.delay` | Server | High values (> 1s warning, > 5s critical) |
| `{region}_rt_bytes_consumed` | Server | Should be non-zero and steady |
| `{region}_rt_records_consumed` | Server | Should be non-zero and steady |

**Dimensions to filter by**:
- `venice.store.name` - Specific store
- `venice.cluster.name` - Cluster
- `venice.region.name` - Source region of RT data
- `venice.version.role` - CURRENT, BACKUP, or FUTURE
- `venice.replica.type` - Leader vs follower
- `venice.replica.state` - Replica health state

### 2. Ingestion Pipeline Metrics

| Metric | Normal Range | Issue Indicated |
|--------|--------------|-----------------|
| `bytes_consumed` | Store-dependent | Zero = consumer stalled |
| `records_consumed` | Store-dependent | Zero = consumer stalled |
| `leader_bytes_consumed` | > 0 for leaders | Zero = leader not consuming |
| `leader_records_consumed` | > 0 for leaders | Zero = leader not consuming |
| `follower_bytes_consumed` | > 0 for followers | Zero = follower not consuming |
| `consumer_records_queue_put_latency` | < 100ms | High = backpressure |

### 3. Leader Production Metrics (for L/F model)

| Metric | Normal Range | Issue Indicated |
|--------|--------------|-----------------|
| `leader_bytes_produced` | > 0 | Zero = leader not producing to VT |
| `leader_records_produced` | > 0 | Zero = leader not producing to VT |
| `leader_produce_latency` | < 100ms | High = Kafka production slow |
| `leader_compress_latency` | < 50ms | High = compression bottleneck |
| `leader_producer_synchronize_latency` | < 100ms | High = producer contention |

### 4. Storage Engine Metrics

| Metric | Normal Range | Issue Indicated |
|--------|--------------|-----------------|
| `storage_engine_put_latency` | < 10ms | High = RocksDB slow |
| `storage_engine_delete_latency` | < 10ms | High = RocksDB slow |
| `disk_usage_in_bytes` | < 80% capacity | High = disk pressure |

### 5. Active-Active (DCR) Metrics

| Metric | Normal Range | Issue Indicated |
|--------|--------------|-----------------|
| `update_ignored_dcr` | Low rate | High = excessive conflicts |
| `tombstone_creation_dcr` | Low rate | High = excessive deletes |
| `timestamp_regression_dcr_error` | 0 | Non-zero = clock skew issues |
| `offset_regression_dcr_error` | 0 | Non-zero = offset vector issues |
| `leader_ingestion_active_active_put_latency` | < 50ms | High = A/A processing slow |
| `leader_ingestion_active_active_update_latency` | < 50ms | High = A/A processing slow |

### 6. Kafka Consumer Metrics

| Metric | Normal Range | Issue Indicated |
|--------|--------------|-----------------|
| `kafka_consumer_poll_latency` | < 500ms | High = Kafka broker issues |
| `records_per_poll` | > 0 | Zero = no data or consumer lag |
| `consumer_idle_time` | Low | High = consumer not processing |

### 7. Resource & Error Metrics

| Metric | Normal Range | Issue Indicated |
|--------|--------------|-----------------|
| `ingestion_failure` | 0 | Non-zero = ingestion errors |
| `checksum_verification_failure` | 0 | Non-zero = data corruption |
| `unexpected_message` | 0 | Non-zero = protocol issues |
| `storage_quota_used` | < 100% | 100% = quota exhausted |
| `ingestion_task_count` | Expected count | Mismatch = tasks missing |

---

## Common Root Causes & Remediation

### 1. Kafka Consumer Lag

**Symptoms**:
- `bytes_consumed` = 0 or very low
- `records_consumed` = 0 or very low
- Heartbeat delay increasing over time

**Diagnosis**:
```bash
# Check consumer group lag
kafka-consumer-groups --bootstrap-server <broker> --describe --group <venice-consumer-group>
```

**Remediation**:
- Check Kafka broker health
- Verify network connectivity to Kafka
- Check for consumer rebalancing
- Increase consumer throughput (more threads/partitions)

### 2. Storage Engine Bottleneck

**Symptoms**:
- `storage_engine_put_latency` > 50ms
- `disk_usage_in_bytes` near capacity
- `consumer_records_queue_put_latency` high

**Diagnosis**:
- Check RocksDB compaction stats
- Check disk I/O metrics
- Check memory pressure

**Remediation**:
- Trigger manual compaction if needed
- Add disk capacity
- Tune RocksDB settings (block cache, write buffer)

### 3. Leader Production Slowdown

**Symptoms**:
- `leader_produce_latency` high
- `leader_bytes_produced` low
- `leader_producer_synchronize_latency` high

**Diagnosis**:
- Check VT topic broker health
- Check producer batching settings
- Check for producer errors in logs

**Remediation**:
- Verify Kafka cluster health
- Tune producer batch size/linger
- Check for network issues to Kafka

### 4. Active-Active Conflict Storm

**Symptoms**:
- `update_ignored_dcr` rate spiking
- `leader_ingestion_active_active_*_latency` high
- Multiple regions showing delays

**Diagnosis**:
- Check if multiple regions writing same keys
- Verify timestamp/offset vector consistency

**Remediation**:
- Review application write patterns
- Check clock synchronization across regions
- Consider key partitioning strategy

### 5. Quota Exhaustion

**Symptoms**:
- `storage_quota_used` = 100%
- Ingestion metrics flat
- Errors in logs about quota

**Diagnosis**:
```bash
# Check store quota via admin tool
venice-admin-tool --get-store-quota --store <store-name>
```

**Remediation**:
- Increase store quota
- Clean up old versions
- Review data retention policy

### 6. Version/Partition Issues

**Symptoms**:
- Only specific partitions affected
- `ingestion_task_count` mismatch
- Heartbeat delay only for certain versions

**Diagnosis**:
- Check Helix partition assignment
- Verify all partitions are assigned and ONLINE

**Remediation**:
- Trigger partition rebalance
- Check for stuck state transitions
- Restart affected server nodes

---

## Debugging Checklist

```markdown
## Heartbeat Delay Investigation for [STORE_NAME]

### Initial Assessment
- [ ] Current delay value: _____ ms
- [ ] Normal baseline: _____ ms
- [ ] Affected regions: _____
- [ ] Started at: _____
- [ ] Any recent deployments/changes: _____

### Metrics Collected
- [ ] `ingestion.replication.heartbeat.delay` - Value: _____
- [ ] `bytes_consumed` rate - Value: _____
- [ ] `records_consumed` rate - Value: _____
- [ ] `leader_bytes_produced` rate - Value: _____
- [ ] `storage_engine_put_latency` - Value: _____
- [ ] `storage_quota_used` - Value: _____
- [ ] `ingestion_failure` count - Value: _____

### Infrastructure Checks
- [ ] Kafka consumer group lag
- [ ] Kafka broker health
- [ ] Disk I/O on storage nodes
- [ ] Network latency between components
- [ ] Memory/CPU utilization

### Root Cause
- [ ] Consumer lag
- [ ] Storage bottleneck
- [ ] Leader production issue
- [ ] DCR conflicts
- [ ] Quota exhaustion
- [ ] Partition/version issue
- [ ] Other: _____

### Resolution
- Action taken: _____
- Time to resolve: _____
- Follow-up items: _____
```

---

## Key Source Files for Deep Dive

| Component | File Path |
|-----------|-----------|
| Heartbeat Stats | `clients/da-vinci-client/src/main/java/com/linkedin/davinci/stats/ingestion/heartbeat/HeartbeatOtelStats.java` |
| Server Metric Entity | `clients/da-vinci-client/src/main/java/com/linkedin/davinci/stats/ServerMetricEntity.java` |
| Ingestion Stats | `clients/da-vinci-client/src/main/java/com/linkedin/davinci/stats/HostLevelIngestionStats.java` |
| Store Ingestion Task | `clients/da-vinci-client/src/main/java/com/linkedin/davinci/kafka/consumer/StoreIngestionTask.java` |
| Leader/Follower Logic | `clients/da-vinci-client/src/main/java/com/linkedin/davinci/kafka/consumer/LeaderFollowerStoreIngestionTask.java` |

---

## Alerting Thresholds (Recommended)

| Severity | Threshold | Action |
|----------|-----------|--------|
| Warning | > 1,000 ms (1s) | Investigate, monitor trend |
| Critical | > 5,000 ms (5s) | Page on-call, immediate investigation |
| Emergency | > 30,000 ms (30s) | All hands, potential data staleness |

---

## Related Documentation

- [Metrics Categorization](./metrics-categorization.md) - Full metrics reference
- Venice Architecture - Write path and ingestion flow
- Active-Active Replication - DCR and conflict resolution
