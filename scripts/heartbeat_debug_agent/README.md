# Venice Heartbeat Debug Agent

An automated agent for debugging leader heartbeat delay alerts in Venice.

## Overview

This agent automates the investigation process described in the [debugging guide](../../docs/debugging-leader-heartbeat-delays.md). It collects metrics, checks infrastructure health, and uses a decision tree to identify the root cause of heartbeat delays.

## Features

- **Automated diagnosis** of 6+ root cause categories
- **Parallel data collection** for fast investigation
- **Confidence scoring** for diagnoses
- **Remediation recommendations** with commands
- **Extensible skill system** for different infrastructure

## Installation

```bash
# From the venice repository root
cd scripts/heartbeat_debug_agent

# Install dependencies (if any)
pip install -r requirements.txt  # if exists
```

## Quick Start

### Using Mock Data (Testing)

```python
from datetime import datetime
from heartbeat_debug_agent import HeartbeatDebugAgent, AgentInput
from heartbeat_debug_agent.mock_skills import create_mock_skill_registry

# Create registry with mock skills (simulates storage bottleneck)
registry = create_mock_skill_registry("storage_bottleneck")

# Create agent
agent = HeartbeatDebugAgent(registry)

# Create input
input_data = AgentInput(
    store_name="my-store",
    cluster_name="venice-prod",
    alert_timestamp=datetime.utcnow(),
    current_delay_ms=15000
)

# Run diagnosis
output = agent.run(input_data)

# View results
print(f"Root Cause: {output.diagnosis.root_cause}")
print(f"Confidence: {output.diagnosis.confidence}%")
```

### Using CLI

```bash
# With mock data
python -m heartbeat_debug_agent.cli \
    --store my-store \
    --cluster venice-prod \
    --delay 15000 \
    --mock storage_bottleneck

# Output as JSON
python -m heartbeat_debug_agent.cli \
    --store my-store \
    --cluster venice-prod \
    --delay 15000 \
    --mock consumer_stalled \
    --output json
```

### Running Example Scenarios

```bash
python -m heartbeat_debug_agent.example
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   HeartbeatDebugAgent                        │
├─────────────────────────────────────────────────────────────┤
│  Input: AgentInput (store, cluster, delay, timestamp, etc.) │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  Phase 1: Context Gathering                                  │
│    └── VeniceAdminSkill → Store info, replicas              │
│                                                              │
│  Phase 2: Metrics Collection (parallel)                      │
│    └── MetricsQuerySkill → Heartbeat, ingestion, storage    │
│                                                              │
│  Phase 3: Infrastructure Check (parallel)                    │
│    ├── KafkaOperationsSkill → Consumer lag                  │
│    ├── HostHealthSkill → CPU, disk, RocksDB                 │
│    ├── LogSearchSkill → Error logs                          │
│    └── HelixStateSkill → Partition assignments              │
│                                                              │
│  Phase 4: Analysis & Diagnosis                               │
│    └── Decision tree based on collected data                │
│                                                              │
│  Phase 5: Output Generation                                  │
│    └── AgentOutput (diagnosis, evidence, remediation)       │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

## Input Parameters

| Parameter | Required | Description |
|-----------|----------|-------------|
| `store_name` | Yes | Name of the affected store |
| `cluster_name` | Yes | Venice cluster name |
| `alert_timestamp` | Yes | When the alert fired |
| `current_delay_ms` | Yes | Current heartbeat delay in ms |
| `region` | No | Specific region to investigate |
| `partition_id` | No | Specific partition if known |
| `version_number` | No | Store version if known |
| `baseline_delay_ms` | No | Normal delay baseline (default: 500) |
| `severity` | No | Alert severity level |
| `time_window_minutes` | No | How far back to look (default: 60) |

## Output Structure

```json
{
  "diagnosis": {
    "root_cause": "Storage engine write latency elevated...",
    "root_cause_category": "storage_bottleneck",
    "confidence": 85,
    "severity_assessment": "critical"
  },
  "evidence": {
    "primary_indicators": [...],
    "supporting_indicators": [...],
    "log_evidence": [...]
  },
  "affected_components": {
    "hosts": [...],
    "partitions": [...],
    "regions": [...]
  },
  "remediation": {
    "immediate_actions": [...],
    "follow_up_actions": [...],
    "escalation_needed": false
  },
  "investigation_summary": {
    "metrics_checked": [...],
    "time_range_analyzed": "...",
    "data_gaps": [...]
  }
}
```

## Root Cause Categories

| Category | Description |
|----------|-------------|
| `consumer_lag` | Kafka consumer stalled or falling behind |
| `storage_bottleneck` | RocksDB write latency or disk issues |
| `leader_production` | Leader not producing to VT |
| `dcr_conflicts` | Active-active conflict storm |
| `quota_exhaustion` | Store quota full |
| `partition_issues` | Offline replicas or partition problems |
| `ingestion_errors` | Ingestion failures in logs |
| `no_data` | No data in RT topic |
| `unknown` | Unable to determine |

## Implementing Real Skills

To use in production, implement the skill interfaces:

```python
from heartbeat_debug_agent.skills import MetricsQuerySkill, TimeRange

class MyMetricsSkill(MetricsQuerySkill):
    def __init__(self, api_client):
        self.client = api_client

    def query_metric(self, metric_name, dimensions, time_range, aggregation="avg"):
        # Implement using your metrics system (InGraphs, Prometheus, etc.)
        response = self.client.query(
            metric=metric_name,
            filters=dimensions,
            start=time_range.start,
            end=time_range.end,
            agg=aggregation
        )
        return self._convert_to_timeseries(response)

# Register with skill registry
from heartbeat_debug_agent import SkillRegistry

registry = SkillRegistry()
registry.register("metrics", MyMetricsSkill(my_api_client))
registry.register("kafka", MyKafkaSkill(...))
# ... register other skills

agent = HeartbeatDebugAgent(registry)
```

## Customizing Thresholds

```python
from heartbeat_debug_agent import HeartbeatDebugAgent, Thresholds

# Create custom thresholds
thresholds = Thresholds()
thresholds.HEARTBEAT_WARNING_MS = 2000  # More lenient
thresholds.STORAGE_PUT_LATENCY_WARNING_MS = 100

# Create agent with custom thresholds
agent = HeartbeatDebugAgent(registry, thresholds=thresholds)
```

## Files

```
heartbeat_debug_agent/
├── __init__.py          # Package exports
├── models.py            # Data models (input/output)
├── skills.py            # Skill interfaces
├── agent.py             # Main agent implementation
├── mock_skills.py       # Mock implementations for testing
├── cli.py               # Command-line interface
├── example.py           # Example usage script
└── README.md            # This file
```

## Related Documentation

- [Debugging Leader Heartbeat Delays](../../docs/debugging-leader-heartbeat-delays.md)
- [Metrics Categorization](../../docs/metrics-categorization.md)
- [Agent Design Document](../../docs/heartbeat-debug-agent-design.md)
