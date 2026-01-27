# Heartbeat Debug Agent Design

## Overview

This document defines the design for an AI agent that automates the debugging of Venice leader heartbeat delay alerts using the [debugging guide](./debugging-leader-heartbeat-delays.md).

---

## Agent Inputs

### Required Inputs

| Input | Type | Description | Example |
|-------|------|-------------|---------|
| `store_name` | string | Name of the affected store | `my-feature-store` |
| `cluster_name` | string | Venice cluster name | `venice-prod-ltx1` |
| `alert_timestamp` | datetime | When the alert fired | `2024-01-15T10:30:00Z` |
| `current_delay_ms` | number | Current heartbeat delay in ms | `15000` |

### Optional Inputs

| Input | Type | Description | Default |
|-------|------|-------------|---------|
| `region` | string | Specific region to investigate | All regions |
| `partition_id` | number | Specific partition if known | All partitions |
| `version_number` | number | Store version if known | Current version |
| `baseline_delay_ms` | number | Normal delay for this store | `500` |
| `recent_changes` | string | Any known recent deployments | None |
| `severity` | enum | Alert severity level | `warning` |
| `time_window_minutes` | number | How far back to look | `60` |

### Input Schema (JSON)

```json
{
  "store_name": "string (required)",
  "cluster_name": "string (required)",
  "alert_timestamp": "ISO8601 datetime (required)",
  "current_delay_ms": "number (required)",
  "region": "string (optional)",
  "partition_id": "number (optional)",
  "version_number": "number (optional)",
  "baseline_delay_ms": "number (optional, default: 500)",
  "recent_changes": "string (optional)",
  "severity": "enum: warning|critical|emergency (optional, default: warning)",
  "time_window_minutes": "number (optional, default: 60)"
}
```

---

## Required Skills/Tools

### 1. Metrics Query Skill

**Purpose**: Fetch metrics from the observability system (e.g., InGraphs, Prometheus, OpenTelemetry backend)

**Capabilities**:
```yaml
skill: metrics_query
operations:
  - query_metric:
      inputs:
        - metric_name: string
        - dimensions: map<string, string>
        - time_range: TimeRange
        - aggregation: enum[avg, max, min, sum, rate, p50, p99]
      outputs:
        - timeseries: List<DataPoint>
        - current_value: number
        - trend: enum[increasing, decreasing, stable]

  - query_multiple_metrics:
      inputs:
        - metrics: List<MetricQuery>
      outputs:
        - results: map<string, TimeSeries>
```

**Metrics to Query**:
```python
HEARTBEAT_METRICS = [
    "ingestion.replication.heartbeat.delay",
    "{region}_rt_bytes_consumed",
    "{region}_rt_records_consumed",
]

INGESTION_METRICS = [
    "bytes_consumed",
    "records_consumed",
    "leader_bytes_consumed",
    "leader_records_consumed",
    "follower_bytes_consumed",
    "consumer_records_queue_put_latency",
]

LEADER_PRODUCTION_METRICS = [
    "leader_bytes_produced",
    "leader_records_produced",
    "leader_produce_latency",
    "leader_compress_latency",
    "leader_producer_synchronize_latency",
]

STORAGE_METRICS = [
    "storage_engine_put_latency",
    "storage_engine_delete_latency",
    "disk_usage_in_bytes",
]

DCR_METRICS = [
    "update_ignored_dcr",
    "tombstone_creation_dcr",
    "timestamp_regression_dcr_error",
    "offset_regression_dcr_error",
    "leader_ingestion_active_active_put_latency",
]

ERROR_METRICS = [
    "ingestion_failure",
    "checksum_verification_failure",
    "unexpected_message",
    "storage_quota_used",
]
```

---

### 2. Kafka Operations Skill

**Purpose**: Query Kafka consumer group lag and topic information

**Capabilities**:
```yaml
skill: kafka_operations
operations:
  - get_consumer_group_lag:
      inputs:
        - consumer_group: string
        - topic: string (optional)
      outputs:
        - total_lag: number
        - partition_lags: map<partition_id, lag>
        - last_commit_time: datetime

  - get_topic_info:
      inputs:
        - topic_name: string
      outputs:
        - partition_count: number
        - replication_factor: number
        - retention_ms: number
        - partition_leaders: map<partition_id, broker_id>

  - get_broker_health:
      inputs:
        - broker_ids: List<number> (optional)
      outputs:
        - broker_status: map<broker_id, HealthStatus>
        - under_replicated_partitions: number
```

**Topic Naming Convention**:
```python
def get_rt_topic(store_name: str) -> str:
    return f"{store_name}_rt"

def get_vt_topic(store_name: str, version: int) -> str:
    return f"{store_name}_v{version}"
```

---

### 3. Venice Admin Skill

**Purpose**: Query Venice-specific metadata and configurations

**Capabilities**:
```yaml
skill: venice_admin
operations:
  - get_store_info:
      inputs:
        - store_name: string
        - cluster_name: string
      outputs:
        - current_version: number
        - partition_count: number
        - replication_factor: number
        - hybrid_config: HybridConfig
        - quota_bytes: number
        - active_active_enabled: boolean

  - get_store_version_info:
      inputs:
        - store_name: string
        - version: number
      outputs:
        - status: enum[STARTED, PUSHED, ONLINE, ERROR]
        - partition_states: map<partition_id, state>
        - push_start_time: datetime

  - get_replica_status:
      inputs:
        - store_name: string
        - cluster_name: string
        - version: number (optional)
      outputs:
        - replicas: List<ReplicaInfo>
        - offline_replicas: List<ReplicaInfo>
        - leader_partitions: map<partition_id, host>

  - get_ingestion_task_status:
      inputs:
        - store_name: string
        - host: string (optional)
      outputs:
        - task_count: number
        - task_states: map<partition_id, TaskState>
```

---

### 4. Host Health Skill

**Purpose**: Query host-level resource metrics

**Capabilities**:
```yaml
skill: host_health
operations:
  - get_host_metrics:
      inputs:
        - hostname: string
        - metrics: List<string>  # cpu, memory, disk_io, network
      outputs:
        - cpu_usage_percent: number
        - memory_usage_percent: number
        - disk_io_utilization: number
        - network_throughput: NetworkStats

  - get_disk_usage:
      inputs:
        - hostname: string
        - mount_point: string (optional)
      outputs:
        - total_bytes: number
        - used_bytes: number
        - available_bytes: number
        - usage_percent: number

  - get_rocksdb_stats:
      inputs:
        - hostname: string
        - store_name: string
      outputs:
        - compaction_pending: number
        - block_cache_hit_ratio: number
        - write_stall_duration: number
```

---

### 5. Log Search Skill

**Purpose**: Search and analyze application logs

**Capabilities**:
```yaml
skill: log_search
operations:
  - search_logs:
      inputs:
        - query: string
        - time_range: TimeRange
        - hosts: List<string> (optional)
        - log_level: enum[ERROR, WARN, INFO] (optional)
        - limit: number (default: 100)
      outputs:
        - logs: List<LogEntry>
        - error_count: number
        - unique_errors: List<ErrorSummary>

  - get_error_summary:
      inputs:
        - store_name: string
        - time_range: TimeRange
      outputs:
        - error_counts_by_type: map<string, number>
        - top_errors: List<ErrorWithCount>
```

**Common Log Queries**:
```python
LOG_QUERIES = {
    "ingestion_errors": 'store="{store}" AND level=ERROR AND message:*ingestion*',
    "kafka_errors": 'store="{store}" AND level=ERROR AND message:*kafka*',
    "storage_errors": 'store="{store}" AND level=ERROR AND message:*rocksdb*',
    "quota_errors": 'store="{store}" AND message:*quota*exceeded*',
    "dcr_errors": 'store="{store}" AND message:*DCR*error*',
}
```

---

### 6. Helix/Cluster State Skill

**Purpose**: Query Helix cluster state and partition assignments

**Capabilities**:
```yaml
skill: helix_state
operations:
  - get_partition_assignment:
      inputs:
        - cluster_name: string
        - resource: string  # store_name_v{version}
      outputs:
        - assignments: map<partition_id, List<host>>
        - leader_map: map<partition_id, host>

  - get_instance_state:
      inputs:
        - cluster_name: string
        - instance: string (optional)
      outputs:
        - instances: List<InstanceInfo>
        - offline_instances: List<string>

  - get_state_transitions:
      inputs:
        - cluster_name: string
        - resource: string
        - time_range: TimeRange
      outputs:
        - transitions: List<StateTransition>
        - stuck_transitions: List<StateTransition>
```

---

## Agent Workflow

```
┌─────────────────────────────────────────────────────────────────┐
│                     HEARTBEAT DEBUG AGENT                        │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐                                               │
│  │    INPUT     │                                               │
│  │  - store     │                                               │
│  │  - cluster   │                                               │
│  │  - delay_ms  │                                               │
│  │  - timestamp │                                               │
│  └──────┬───────┘                                               │
│         │                                                        │
│         ▼                                                        │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ PHASE 1: CONTEXT GATHERING                                │   │
│  │                                                           │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐       │   │
│  │  │ Venice Admin │  │   Helix     │  │   Metrics   │       │   │
│  │  │ get_store   │  │ get_replicas│  │ get_baseline│       │   │
│  │  └─────────────┘  └─────────────┘  └─────────────┘       │   │
│  └──────────────────────────┬───────────────────────────────┘   │
│                             │                                    │
│                             ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ PHASE 2: METRICS COLLECTION (Parallel)                    │   │
│  │                                                           │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐     │   │
│  │  │Heartbeat │ │Ingestion │ │ Storage  │ │   DCR    │     │   │
│  │  │ Metrics  │ │ Metrics  │ │ Metrics  │ │ Metrics  │     │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘     │   │
│  └──────────────────────────┬───────────────────────────────┘   │
│                             │                                    │
│                             ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ PHASE 3: INFRASTRUCTURE CHECK (Parallel)                  │   │
│  │                                                           │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐     │   │
│  │  │  Kafka   │ │   Host   │ │   Logs   │ │  Helix   │     │   │
│  │  │Consumer  │ │  Health  │ │  Errors  │ │  State   │     │   │
│  │  │   Lag    │ │          │ │          │ │          │     │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘     │   │
│  └──────────────────────────┬───────────────────────────────┘   │
│                             │                                    │
│                             ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ PHASE 4: ANALYSIS & DIAGNOSIS                             │   │
│  │                                                           │   │
│  │  Apply decision tree from debugging guide:                │   │
│  │  1. Check if consumer stalled (bytes_consumed = 0)        │   │
│  │  2. Check storage bottleneck (put_latency > 50ms)         │   │
│  │  3. Check leader production (leader_produced = 0)         │   │
│  │  4. Check DCR conflicts (update_ignored high)             │   │
│  │  5. Check quota (storage_quota_used = 100%)               │   │
│  │  6. Check partition issues (missing tasks)                │   │
│  └──────────────────────────┬───────────────────────────────┘   │
│                             │                                    │
│                             ▼                                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │ PHASE 5: OUTPUT                                           │   │
│  │                                                           │   │
│  │  - Root cause identification                              │   │
│  │  - Evidence (metrics, logs)                               │   │
│  │  - Remediation steps                                      │   │
│  │  - Confidence score                                       │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Agent Output Schema

```json
{
  "diagnosis": {
    "root_cause": "string - identified root cause",
    "root_cause_category": "enum: consumer_lag|storage_bottleneck|leader_production|dcr_conflicts|quota_exhaustion|partition_issues|unknown",
    "confidence": "number 0-100",
    "severity_assessment": "enum: resolved|warning|critical|emergency"
  },

  "evidence": {
    "primary_indicators": [
      {
        "metric": "string",
        "value": "number",
        "threshold": "number",
        "status": "enum: normal|warning|critical"
      }
    ],
    "supporting_indicators": [...],
    "log_evidence": [
      {
        "timestamp": "datetime",
        "message": "string",
        "relevance": "string"
      }
    ]
  },

  "affected_components": {
    "hosts": ["list of affected hosts"],
    "partitions": ["list of affected partitions"],
    "regions": ["list of affected regions"]
  },

  "remediation": {
    "immediate_actions": [
      {
        "action": "string",
        "command": "string (optional)",
        "risk": "enum: low|medium|high",
        "expected_impact": "string"
      }
    ],
    "follow_up_actions": [...],
    "escalation_needed": "boolean",
    "escalation_reason": "string (if needed)"
  },

  "investigation_summary": {
    "metrics_checked": ["list of metrics"],
    "time_range_analyzed": "TimeRange",
    "data_gaps": ["any missing data"],
    "assumptions": ["any assumptions made"]
  }
}
```

---

## Decision Tree Logic

```python
def diagnose_heartbeat_delay(metrics: Dict, kafka: Dict, host: Dict, logs: Dict) -> Diagnosis:
    """
    Decision tree for root cause identification.
    Order matters - check most common/impactful causes first.
    """

    # 1. Consumer completely stalled?
    if metrics['bytes_consumed'] == 0 and metrics['records_consumed'] == 0:
        if kafka['consumer_lag'] > 0:
            return Diagnosis(
                cause="consumer_lag",
                detail="Consumer stalled with Kafka lag present",
                remediation="Check Kafka connectivity, consumer group health"
            )
        else:
            return Diagnosis(
                cause="no_data",
                detail="No data in RT topic",
                remediation="Verify producers are writing to RT topic"
            )

    # 2. Storage bottleneck?
    if metrics['storage_engine_put_latency'] > 50:
        if host['disk_io_utilization'] > 80:
            return Diagnosis(
                cause="storage_bottleneck",
                detail="Disk I/O saturation causing slow writes",
                remediation="Check RocksDB compaction, consider disk upgrade"
            )
        if metrics['disk_usage_percent'] > 90:
            return Diagnosis(
                cause="storage_bottleneck",
                detail="Disk nearly full",
                remediation="Free disk space, increase quota, clean old versions"
            )

    # 3. Leader production issues?
    if metrics['leader_bytes_produced'] == 0:
        if metrics['leader_produce_latency'] > 100:
            return Diagnosis(
                cause="leader_production",
                detail="Leader Kafka production slow",
                remediation="Check VT topic broker health"
            )
        return Diagnosis(
            cause="leader_production",
            detail="Leader not producing to VT",
            remediation="Check leader election, ingestion task state"
        )

    # 4. DCR conflict storm?
    if metrics['update_ignored_dcr'] > threshold:
        return Diagnosis(
            cause="dcr_conflicts",
            detail="High rate of DCR conflicts",
            remediation="Review write patterns, check clock sync"
        )

    # 5. Quota exhaustion?
    if metrics['storage_quota_used'] >= 100:
        return Diagnosis(
            cause="quota_exhaustion",
            detail="Store quota exhausted",
            remediation="Increase quota or reduce data"
        )

    # 6. Check for errors in logs
    if logs['ingestion_failure_count'] > 0:
        return Diagnosis(
            cause="ingestion_errors",
            detail=f"Found {logs['ingestion_failure_count']} ingestion errors",
            remediation="Review error logs for specific failures"
        )

    # 7. Partial consumer lag
    if kafka['consumer_lag'] > threshold:
        return Diagnosis(
            cause="consumer_lag",
            detail="Consumer falling behind",
            remediation="Increase consumer throughput"
        )

    # Unable to determine
    return Diagnosis(
        cause="unknown",
        detail="No clear root cause identified",
        remediation="Manual investigation needed"
    )
```

---

## Example Agent Invocation

### Input
```json
{
  "store_name": "member-features-store",
  "cluster_name": "venice-prod-ltx1",
  "alert_timestamp": "2024-01-15T10:30:00Z",
  "current_delay_ms": 15000,
  "region": "ltx1",
  "baseline_delay_ms": 500,
  "severity": "critical"
}
```

### Output
```json
{
  "diagnosis": {
    "root_cause": "Storage engine write latency elevated due to RocksDB compaction backlog",
    "root_cause_category": "storage_bottleneck",
    "confidence": 85,
    "severity_assessment": "critical"
  },
  "evidence": {
    "primary_indicators": [
      {
        "metric": "storage_engine_put_latency",
        "value": 125,
        "threshold": 50,
        "status": "critical"
      },
      {
        "metric": "ingestion.replication.heartbeat.delay",
        "value": 15000,
        "threshold": 5000,
        "status": "critical"
      }
    ],
    "supporting_indicators": [
      {
        "metric": "disk_io_utilization",
        "value": 92,
        "threshold": 80,
        "status": "warning"
      }
    ],
    "log_evidence": [
      {
        "timestamp": "2024-01-15T10:28:45Z",
        "message": "RocksDB compaction taking longer than expected",
        "relevance": "Confirms storage bottleneck"
      }
    ]
  },
  "affected_components": {
    "hosts": ["venice-server-ltx1-001", "venice-server-ltx1-002"],
    "partitions": [0, 1, 2, 3, 4, 5],
    "regions": ["ltx1"]
  },
  "remediation": {
    "immediate_actions": [
      {
        "action": "Trigger manual compaction to clear backlog",
        "command": "venice-admin-tool --trigger-compaction --store member-features-store",
        "risk": "medium",
        "expected_impact": "Temporary increased I/O, then latency improvement"
      }
    ],
    "follow_up_actions": [
      {
        "action": "Review RocksDB tuning parameters",
        "risk": "low"
      },
      {
        "action": "Consider adding SSD capacity",
        "risk": "low"
      }
    ],
    "escalation_needed": false
  },
  "investigation_summary": {
    "metrics_checked": [
      "ingestion.replication.heartbeat.delay",
      "bytes_consumed",
      "storage_engine_put_latency",
      "disk_usage_in_bytes"
    ],
    "time_range_analyzed": "2024-01-15T09:30:00Z to 2024-01-15T10:35:00Z",
    "data_gaps": [],
    "assumptions": []
  }
}
```

---

## Implementation Notes

### Skill Integration Points

| Skill | Typical Integration | Authentication |
|-------|---------------------|----------------|
| `metrics_query` | InGraphs API, Prometheus, OTel Collector | Service account |
| `kafka_operations` | Kafka Admin Client, Cruise Control API | Kerberos/SSL |
| `venice_admin` | Venice Admin Tool CLI or REST API | Service account |
| `host_health` | Host metrics collector, Node exporter | Service account |
| `log_search` | Splunk, ELK, or internal log system | Service account |
| `helix_state` | Helix REST API, ZooKeeper | Service account |

### Error Handling

The agent should handle:
- Skill timeouts (default: 30s per skill call)
- Missing data (some metrics may not be available)
- Partial failures (continue with available data)
- Rate limiting on metrics/log APIs

### Caching

Consider caching:
- Store configuration (changes rarely)
- Host lists (changes occasionally)
- Baseline metrics (refresh daily)

---

## Future Enhancements

1. **Auto-remediation**: Execute safe remediation actions automatically
2. **Pattern learning**: Learn from past incidents to improve diagnosis
3. **Correlation**: Correlate with other alerts (Kafka, host, network)
4. **Prediction**: Predict heartbeat delays before they become critical
5. **Multi-store**: Handle alerts for multiple stores simultaneously
