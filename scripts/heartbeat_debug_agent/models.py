"""
Data models for the Heartbeat Debug Agent.
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Any


class Severity(Enum):
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class RootCauseCategory(Enum):
    CONSUMER_LAG = "consumer_lag"
    STORAGE_BOTTLENECK = "storage_bottleneck"
    LEADER_PRODUCTION = "leader_production"
    DCR_CONFLICTS = "dcr_conflicts"
    QUOTA_EXHAUSTION = "quota_exhaustion"
    PARTITION_ISSUES = "partition_issues"
    NO_DATA = "no_data"
    INGESTION_ERRORS = "ingestion_errors"
    UNKNOWN = "unknown"


class MetricStatus(Enum):
    NORMAL = "normal"
    WARNING = "warning"
    CRITICAL = "critical"


class SeverityAssessment(Enum):
    RESOLVED = "resolved"
    WARNING = "warning"
    CRITICAL = "critical"
    EMERGENCY = "emergency"


class Risk(Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


# Input Models

@dataclass
class AgentInput:
    """Input parameters for the heartbeat debug agent."""
    store_name: str
    cluster_name: str
    alert_timestamp: datetime
    current_delay_ms: float
    region: Optional[str] = None
    partition_id: Optional[int] = None
    version_number: Optional[int] = None
    baseline_delay_ms: float = 500.0
    recent_changes: Optional[str] = None
    severity: Severity = Severity.WARNING
    time_window_minutes: int = 60

    def to_dict(self) -> Dict[str, Any]:
        return {
            "store_name": self.store_name,
            "cluster_name": self.cluster_name,
            "alert_timestamp": self.alert_timestamp.isoformat(),
            "current_delay_ms": self.current_delay_ms,
            "region": self.region,
            "partition_id": self.partition_id,
            "version_number": self.version_number,
            "baseline_delay_ms": self.baseline_delay_ms,
            "recent_changes": self.recent_changes,
            "severity": self.severity.value,
            "time_window_minutes": self.time_window_minutes,
        }


# Metrics Models

@dataclass
class DataPoint:
    """A single data point in a time series."""
    timestamp: datetime
    value: float


@dataclass
class TimeSeries:
    """A time series of metric data."""
    metric_name: str
    data_points: List[DataPoint]

    @property
    def current_value(self) -> Optional[float]:
        if not self.data_points:
            return None
        return self.data_points[-1].value

    @property
    def average(self) -> Optional[float]:
        if not self.data_points:
            return None
        return sum(dp.value for dp in self.data_points) / len(self.data_points)

    @property
    def max_value(self) -> Optional[float]:
        if not self.data_points:
            return None
        return max(dp.value for dp in self.data_points)


@dataclass
class MetricIndicator:
    """A metric indicator with its status."""
    metric: str
    value: float
    threshold: float
    status: MetricStatus

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metric": self.metric,
            "value": self.value,
            "threshold": self.threshold,
            "status": self.status.value,
        }


# Kafka Models

@dataclass
class ConsumerLagInfo:
    """Kafka consumer group lag information."""
    consumer_group: str
    total_lag: int
    partition_lags: Dict[int, int]
    last_commit_time: Optional[datetime] = None


@dataclass
class BrokerHealth:
    """Kafka broker health status."""
    broker_id: int
    is_healthy: bool
    under_replicated_partitions: int = 0


# Venice Models

@dataclass
class StoreInfo:
    """Venice store information."""
    store_name: str
    current_version: int
    partition_count: int
    replication_factor: int
    hybrid_enabled: bool
    active_active_enabled: bool
    quota_bytes: int
    regions: List[str] = field(default_factory=list)


@dataclass
class ReplicaInfo:
    """Venice replica information."""
    partition_id: int
    host: str
    is_leader: bool
    state: str  # ONLINE, OFFLINE, BOOTSTRAP, etc.


@dataclass
class IngestionTaskInfo:
    """Ingestion task status."""
    partition_id: int
    state: str
    current_offset: int
    lag: int


# Host Models

@dataclass
class HostMetrics:
    """Host-level resource metrics."""
    hostname: str
    cpu_usage_percent: float
    memory_usage_percent: float
    disk_io_utilization: float
    disk_usage_percent: float


@dataclass
class RocksDBStats:
    """RocksDB statistics."""
    hostname: str
    compaction_pending: int
    block_cache_hit_ratio: float
    write_stall_duration_ms: float


# Log Models

@dataclass
class LogEntry:
    """A single log entry."""
    timestamp: datetime
    level: str
    message: str
    host: Optional[str] = None


@dataclass
class ErrorSummary:
    """Summary of errors."""
    error_type: str
    count: int
    sample_message: str


# Output Models

@dataclass
class Diagnosis:
    """Diagnosis result."""
    root_cause: str
    root_cause_category: RootCauseCategory
    confidence: int  # 0-100
    severity_assessment: SeverityAssessment

    def to_dict(self) -> Dict[str, Any]:
        return {
            "root_cause": self.root_cause,
            "root_cause_category": self.root_cause_category.value,
            "confidence": self.confidence,
            "severity_assessment": self.severity_assessment.value,
        }


@dataclass
class LogEvidence:
    """Log-based evidence."""
    timestamp: datetime
    message: str
    relevance: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "timestamp": self.timestamp.isoformat(),
            "message": self.message,
            "relevance": self.relevance,
        }


@dataclass
class Evidence:
    """Evidence supporting the diagnosis."""
    primary_indicators: List[MetricIndicator]
    supporting_indicators: List[MetricIndicator]
    log_evidence: List[LogEvidence]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "primary_indicators": [i.to_dict() for i in self.primary_indicators],
            "supporting_indicators": [i.to_dict() for i in self.supporting_indicators],
            "log_evidence": [e.to_dict() for e in self.log_evidence],
        }


@dataclass
class AffectedComponents:
    """Components affected by the issue."""
    hosts: List[str]
    partitions: List[int]
    regions: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "hosts": self.hosts,
            "partitions": self.partitions,
            "regions": self.regions,
        }


@dataclass
class RemediationAction:
    """A remediation action."""
    action: str
    command: Optional[str] = None
    risk: Risk = Risk.LOW
    expected_impact: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "action": self.action,
            "risk": self.risk.value,
        }
        if self.command:
            result["command"] = self.command
        if self.expected_impact:
            result["expected_impact"] = self.expected_impact
        return result


@dataclass
class Remediation:
    """Remediation plan."""
    immediate_actions: List[RemediationAction]
    follow_up_actions: List[RemediationAction]
    escalation_needed: bool
    escalation_reason: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result = {
            "immediate_actions": [a.to_dict() for a in self.immediate_actions],
            "follow_up_actions": [a.to_dict() for a in self.follow_up_actions],
            "escalation_needed": self.escalation_needed,
        }
        if self.escalation_reason:
            result["escalation_reason"] = self.escalation_reason
        return result


@dataclass
class InvestigationSummary:
    """Summary of the investigation."""
    metrics_checked: List[str]
    time_range_start: datetime
    time_range_end: datetime
    data_gaps: List[str]
    assumptions: List[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "metrics_checked": self.metrics_checked,
            "time_range_analyzed": f"{self.time_range_start.isoformat()} to {self.time_range_end.isoformat()}",
            "data_gaps": self.data_gaps,
            "assumptions": self.assumptions,
        }


@dataclass
class AgentOutput:
    """Complete output from the heartbeat debug agent."""
    diagnosis: Diagnosis
    evidence: Evidence
    affected_components: AffectedComponents
    remediation: Remediation
    investigation_summary: InvestigationSummary

    def to_dict(self) -> Dict[str, Any]:
        return {
            "diagnosis": self.diagnosis.to_dict(),
            "evidence": self.evidence.to_dict(),
            "affected_components": self.affected_components.to_dict(),
            "remediation": self.remediation.to_dict(),
            "investigation_summary": self.investigation_summary.to_dict(),
        }
