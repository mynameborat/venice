"""
Heartbeat Debug Agent - Main implementation.

This agent automates the debugging of Venice leader heartbeat delay alerts.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple

from .models import (
    AgentInput,
    AgentOutput,
    Diagnosis,
    Evidence,
    AffectedComponents,
    Remediation,
    RemediationAction,
    InvestigationSummary,
    MetricIndicator,
    LogEvidence,
    RootCauseCategory,
    MetricStatus,
    SeverityAssessment,
    Risk,
    StoreInfo,
    TimeSeries,
    HostMetrics,
    ConsumerLagInfo,
)
from .skills import (
    SkillRegistry,
    TimeRange,
    MetricsQuerySkill,
    KafkaOperationsSkill,
    VeniceAdminSkill,
    HostHealthSkill,
    LogSearchSkill,
    HelixStateSkill,
)

logger = logging.getLogger(__name__)


# Threshold constants
class Thresholds:
    """Configurable thresholds for diagnosis."""

    # Heartbeat delay thresholds (ms)
    HEARTBEAT_WARNING_MS = 1000
    HEARTBEAT_CRITICAL_MS = 5000
    HEARTBEAT_EMERGENCY_MS = 30000

    # Ingestion thresholds
    BYTES_CONSUMED_MIN = 0
    CONSUMER_LAG_WARNING = 10000
    CONSUMER_LAG_CRITICAL = 100000

    # Storage thresholds
    STORAGE_PUT_LATENCY_WARNING_MS = 50
    STORAGE_PUT_LATENCY_CRITICAL_MS = 100
    DISK_USAGE_WARNING_PERCENT = 80
    DISK_USAGE_CRITICAL_PERCENT = 90
    DISK_IO_WARNING_PERCENT = 80

    # Leader production thresholds
    LEADER_PRODUCE_LATENCY_WARNING_MS = 100
    LEADER_PRODUCE_LATENCY_CRITICAL_MS = 500

    # DCR thresholds
    DCR_UPDATE_IGNORED_RATE_WARNING = 100  # per second
    DCR_TIMESTAMP_REGRESSION_THRESHOLD = 0

    # Quota thresholds
    QUOTA_WARNING_PERCENT = 90
    QUOTA_CRITICAL_PERCENT = 100


class CollectedData:
    """Container for all collected data during investigation."""

    def __init__(self):
        # Metrics
        self.heartbeat_delay: Optional[float] = None
        self.bytes_consumed: Optional[float] = None
        self.records_consumed: Optional[float] = None
        self.leader_bytes_consumed: Optional[float] = None
        self.leader_records_consumed: Optional[float] = None
        self.leader_bytes_produced: Optional[float] = None
        self.leader_produce_latency: Optional[float] = None
        self.storage_put_latency: Optional[float] = None
        self.storage_delete_latency: Optional[float] = None
        self.disk_usage_bytes: Optional[float] = None
        self.storage_quota_used: Optional[float] = None
        self.update_ignored_dcr: Optional[float] = None
        self.tombstone_creation_dcr: Optional[float] = None
        self.timestamp_regression_dcr: Optional[float] = None
        self.ingestion_failure_count: Optional[float] = None
        self.checksum_failure_count: Optional[float] = None

        # Infrastructure
        self.consumer_lag: Optional[ConsumerLagInfo] = None
        self.host_metrics: Dict[str, HostMetrics] = {}
        self.store_info: Optional[StoreInfo] = None
        self.leader_hosts: List[str] = []
        self.offline_replicas: List[Dict] = []
        self.error_logs: List[Any] = []

        # Tracking
        self.metrics_checked: List[str] = []
        self.data_gaps: List[str] = []


class HeartbeatDebugAgent:
    """
    Agent for debugging Venice leader heartbeat delay alerts.

    This agent follows a systematic approach:
    1. Gather context (store info, current state)
    2. Collect metrics in parallel
    3. Check infrastructure health
    4. Analyze data using decision tree
    5. Generate diagnosis and remediation
    """

    def __init__(self, skill_registry: SkillRegistry, thresholds: Optional[Thresholds] = None):
        """
        Initialize the agent.

        Args:
            skill_registry: Registry containing skill implementations
            thresholds: Optional custom thresholds (uses defaults if not provided)
        """
        self.skills = skill_registry
        self.thresholds = thresholds or Thresholds()
        self.executor = ThreadPoolExecutor(max_workers=10)

    def run(self, input_data: AgentInput) -> AgentOutput:
        """
        Run the heartbeat debug agent.

        Args:
            input_data: Agent input parameters

        Returns:
            AgentOutput with diagnosis and remediation
        """
        logger.info(f"Starting heartbeat debug for store={input_data.store_name}, "
                    f"cluster={input_data.cluster_name}, delay={input_data.current_delay_ms}ms")

        # Initialize time range for queries
        time_range = TimeRange.around_timestamp(
            input_data.alert_timestamp,
            window_minutes=input_data.time_window_minutes
        )

        # Initialize data container
        data = CollectedData()
        data.heartbeat_delay = input_data.current_delay_ms

        # Phase 1: Context gathering
        logger.info("Phase 1: Gathering context...")
        self._gather_context(input_data, data)

        # Phase 2: Collect metrics in parallel
        logger.info("Phase 2: Collecting metrics...")
        self._collect_metrics(input_data, time_range, data)

        # Phase 3: Check infrastructure
        logger.info("Phase 3: Checking infrastructure...")
        self._check_infrastructure(input_data, time_range, data)

        # Phase 4: Analyze and diagnose
        logger.info("Phase 4: Analyzing data...")
        diagnosis = self._analyze_and_diagnose(input_data, data)

        # Phase 5: Generate output
        logger.info("Phase 5: Generating output...")
        output = self._generate_output(input_data, data, diagnosis, time_range)

        logger.info(f"Diagnosis complete: {diagnosis.root_cause_category.value} "
                    f"(confidence: {diagnosis.confidence}%)")

        return output

    def _gather_context(self, input_data: AgentInput, data: CollectedData) -> None:
        """Phase 1: Gather store context and current state."""
        venice_skill = self.skills.get_venice_admin_skill()
        if not venice_skill:
            data.data_gaps.append("Venice admin skill not available")
            return

        # Get store info
        try:
            store_info = venice_skill.get_store_info(
                input_data.store_name,
                input_data.cluster_name
            )
            data.store_info = store_info
        except Exception as e:
            logger.warning(f"Failed to get store info: {e}")
            data.data_gaps.append("Store info unavailable")

        # Get replica status
        try:
            replicas = venice_skill.get_replica_status(
                input_data.store_name,
                input_data.cluster_name,
                input_data.version_number
            )
            data.leader_hosts = [r.host for r in replicas if r.is_leader]
            data.offline_replicas = [
                {"partition": r.partition_id, "host": r.host}
                for r in replicas if r.state == "OFFLINE"
            ]
        except Exception as e:
            logger.warning(f"Failed to get replica status: {e}")
            data.data_gaps.append("Replica status unavailable")

    def _collect_metrics(
        self,
        input_data: AgentInput,
        time_range: TimeRange,
        data: CollectedData
    ) -> None:
        """Phase 2: Collect metrics in parallel."""
        metrics_skill = self.skills.get_metrics_skill()
        if not metrics_skill:
            data.data_gaps.append("Metrics skill not available")
            return

        # Define all metrics to collect
        base_dimensions = {
            "venice.store.name": input_data.store_name,
            "venice.cluster.name": input_data.cluster_name,
        }

        if input_data.region:
            base_dimensions["venice.region.name"] = input_data.region

        metrics_to_collect = [
            ("ingestion.replication.heartbeat.delay", "heartbeat_delay"),
            ("bytes_consumed", "bytes_consumed"),
            ("records_consumed", "records_consumed"),
            ("leader_bytes_consumed", "leader_bytes_consumed"),
            ("leader_records_consumed", "leader_records_consumed"),
            ("leader_bytes_produced", "leader_bytes_produced"),
            ("leader_produce_latency", "leader_produce_latency"),
            ("storage_engine_put_latency", "storage_put_latency"),
            ("storage_engine_delete_latency", "storage_delete_latency"),
            ("disk_usage_in_bytes", "disk_usage_bytes"),
            ("storage_quota_used", "storage_quota_used"),
            ("update_ignored_dcr", "update_ignored_dcr"),
            ("tombstone_creation_dcr", "tombstone_creation_dcr"),
            ("timestamp_regression_dcr_error", "timestamp_regression_dcr"),
            ("ingestion_failure", "ingestion_failure_count"),
            ("checksum_verification_failure", "checksum_failure_count"),
        ]

        # Collect metrics in parallel
        futures = {}
        for metric_name, attr_name in metrics_to_collect:
            future = self.executor.submit(
                self._safe_query_metric,
                metrics_skill,
                metric_name,
                base_dimensions,
                time_range
            )
            futures[future] = (metric_name, attr_name)

        # Gather results
        for future in as_completed(futures):
            metric_name, attr_name = futures[future]
            try:
                value = future.result()
                setattr(data, attr_name, value)
                data.metrics_checked.append(metric_name)
                if value is None:
                    data.data_gaps.append(f"Metric {metric_name} returned no data")
            except Exception as e:
                logger.warning(f"Failed to collect metric {metric_name}: {e}")
                data.data_gaps.append(f"Metric {metric_name} collection failed")

    def _safe_query_metric(
        self,
        skill: MetricsQuerySkill,
        metric_name: str,
        dimensions: Dict[str, str],
        time_range: TimeRange
    ) -> Optional[float]:
        """Safely query a metric, returning None on error."""
        try:
            ts = skill.query_metric(metric_name, dimensions, time_range, aggregation="avg")
            return ts.current_value if ts else None
        except Exception as e:
            logger.debug(f"Error querying {metric_name}: {e}")
            return None

    def _check_infrastructure(
        self,
        input_data: AgentInput,
        time_range: TimeRange,
        data: CollectedData
    ) -> None:
        """Phase 3: Check infrastructure health."""
        # Check Kafka consumer lag
        kafka_skill = self.skills.get_kafka_skill()
        if kafka_skill:
            try:
                rt_topic = kafka_skill.get_rt_topic_name(input_data.store_name)
                consumer_group = f"venice-server-{input_data.cluster_name}"
                data.consumer_lag = kafka_skill.get_consumer_group_lag(
                    consumer_group, rt_topic
                )
            except Exception as e:
                logger.warning(f"Failed to get consumer lag: {e}")
                data.data_gaps.append("Consumer lag unavailable")

        # Check host health for leader hosts
        host_skill = self.skills.get_host_health_skill()
        if host_skill and data.leader_hosts:
            for host in data.leader_hosts[:5]:  # Limit to first 5 hosts
                try:
                    metrics = host_skill.get_host_metrics(host)
                    if metrics:
                        data.host_metrics[host] = metrics
                except Exception as e:
                    logger.warning(f"Failed to get host metrics for {host}: {e}")

        # Search for error logs
        log_skill = self.skills.get_log_search_skill()
        if log_skill:
            try:
                errors = log_skill.get_error_summary(input_data.store_name, time_range)
                data.error_logs = errors
            except Exception as e:
                logger.warning(f"Failed to search logs: {e}")
                data.data_gaps.append("Log search unavailable")

    def _analyze_and_diagnose(
        self,
        input_data: AgentInput,
        data: CollectedData
    ) -> Diagnosis:
        """
        Phase 4: Analyze collected data and produce diagnosis.

        Uses a decision tree approach, checking causes in order of likelihood.
        """
        th = self.thresholds

        # 1. Check if consumer completely stalled
        if self._is_value_zero_or_none(data.bytes_consumed) and \
           self._is_value_zero_or_none(data.records_consumed):
            if data.consumer_lag and data.consumer_lag.total_lag > 0:
                return Diagnosis(
                    root_cause="Consumer stalled with Kafka lag present - consumer is not making progress",
                    root_cause_category=RootCauseCategory.CONSUMER_LAG,
                    confidence=90,
                    severity_assessment=self._assess_severity(input_data.current_delay_ms)
                )
            else:
                return Diagnosis(
                    root_cause="No data being consumed - RT topic may be empty or consumer not subscribed",
                    root_cause_category=RootCauseCategory.NO_DATA,
                    confidence=75,
                    severity_assessment=self._assess_severity(input_data.current_delay_ms)
                )

        # 2. Check storage bottleneck
        if data.storage_put_latency and data.storage_put_latency > th.STORAGE_PUT_LATENCY_WARNING_MS:
            # Check if disk I/O is the cause
            high_io_hosts = [
                host for host, metrics in data.host_metrics.items()
                if metrics.disk_io_utilization > th.DISK_IO_WARNING_PERCENT
            ]

            if high_io_hosts:
                return Diagnosis(
                    root_cause=f"Storage bottleneck due to disk I/O saturation on hosts: {', '.join(high_io_hosts)}",
                    root_cause_category=RootCauseCategory.STORAGE_BOTTLENECK,
                    confidence=85,
                    severity_assessment=self._assess_severity(input_data.current_delay_ms)
                )

            # Check disk usage
            high_disk_hosts = [
                host for host, metrics in data.host_metrics.items()
                if metrics.disk_usage_percent > th.DISK_USAGE_WARNING_PERCENT
            ]

            if high_disk_hosts:
                return Diagnosis(
                    root_cause=f"Storage bottleneck due to high disk usage on hosts: {', '.join(high_disk_hosts)}",
                    root_cause_category=RootCauseCategory.STORAGE_BOTTLENECK,
                    confidence=80,
                    severity_assessment=self._assess_severity(input_data.current_delay_ms)
                )

            return Diagnosis(
                root_cause=f"Storage engine write latency elevated ({data.storage_put_latency:.1f}ms) - possible RocksDB compaction backlog",
                root_cause_category=RootCauseCategory.STORAGE_BOTTLENECK,
                confidence=75,
                severity_assessment=self._assess_severity(input_data.current_delay_ms)
            )

        # 3. Check leader production issues
        if self._is_value_zero_or_none(data.leader_bytes_produced):
            if data.leader_produce_latency and data.leader_produce_latency > th.LEADER_PRODUCE_LATENCY_WARNING_MS:
                return Diagnosis(
                    root_cause=f"Leader Kafka production slow ({data.leader_produce_latency:.1f}ms) - VT topic broker may be unhealthy",
                    root_cause_category=RootCauseCategory.LEADER_PRODUCTION,
                    confidence=80,
                    severity_assessment=self._assess_severity(input_data.current_delay_ms)
                )
            return Diagnosis(
                root_cause="Leader not producing to VT - check leader election and ingestion task state",
                root_cause_category=RootCauseCategory.LEADER_PRODUCTION,
                confidence=70,
                severity_assessment=self._assess_severity(input_data.current_delay_ms)
            )

        # 4. Check DCR conflicts (for active-active stores)
        if data.store_info and data.store_info.active_active_enabled:
            if data.update_ignored_dcr and data.update_ignored_dcr > th.DCR_UPDATE_IGNORED_RATE_WARNING:
                return Diagnosis(
                    root_cause=f"High rate of DCR conflicts ({data.update_ignored_dcr:.1f}/s) - possible write contention across regions",
                    root_cause_category=RootCauseCategory.DCR_CONFLICTS,
                    confidence=75,
                    severity_assessment=self._assess_severity(input_data.current_delay_ms)
                )

            if data.timestamp_regression_dcr and data.timestamp_regression_dcr > th.DCR_TIMESTAMP_REGRESSION_THRESHOLD:
                return Diagnosis(
                    root_cause="Timestamp regression errors detected - clock skew between regions",
                    root_cause_category=RootCauseCategory.DCR_CONFLICTS,
                    confidence=80,
                    severity_assessment=self._assess_severity(input_data.current_delay_ms)
                )

        # 5. Check quota exhaustion
        if data.storage_quota_used and data.storage_quota_used >= th.QUOTA_CRITICAL_PERCENT:
            return Diagnosis(
                root_cause="Store quota exhausted - ingestion blocked until quota is increased",
                root_cause_category=RootCauseCategory.QUOTA_EXHAUSTION,
                confidence=95,
                severity_assessment=self._assess_severity(input_data.current_delay_ms)
            )

        # 6. Check for ingestion errors
        if data.ingestion_failure_count and data.ingestion_failure_count > 0:
            return Diagnosis(
                root_cause=f"Ingestion failures detected ({int(data.ingestion_failure_count)} errors) - review logs for specific failures",
                root_cause_category=RootCauseCategory.INGESTION_ERRORS,
                confidence=70,
                severity_assessment=self._assess_severity(input_data.current_delay_ms)
            )

        # 7. Check partition/replica issues
        if data.offline_replicas:
            return Diagnosis(
                root_cause=f"Offline replicas detected: {len(data.offline_replicas)} partitions affected",
                root_cause_category=RootCauseCategory.PARTITION_ISSUES,
                confidence=75,
                severity_assessment=self._assess_severity(input_data.current_delay_ms)
            )

        # 8. Check consumer lag (partial)
        if data.consumer_lag and data.consumer_lag.total_lag > th.CONSUMER_LAG_WARNING:
            return Diagnosis(
                root_cause=f"Consumer falling behind - lag of {data.consumer_lag.total_lag} messages",
                root_cause_category=RootCauseCategory.CONSUMER_LAG,
                confidence=65,
                severity_assessment=self._assess_severity(input_data.current_delay_ms)
            )

        # Unable to determine root cause
        return Diagnosis(
            root_cause="Unable to determine specific root cause - manual investigation required",
            root_cause_category=RootCauseCategory.UNKNOWN,
            confidence=30,
            severity_assessment=self._assess_severity(input_data.current_delay_ms)
        )

    def _is_value_zero_or_none(self, value: Optional[float]) -> bool:
        """Check if a value is zero or None."""
        return value is None or value == 0

    def _assess_severity(self, delay_ms: float) -> SeverityAssessment:
        """Assess severity based on delay value."""
        th = self.thresholds
        if delay_ms >= th.HEARTBEAT_EMERGENCY_MS:
            return SeverityAssessment.EMERGENCY
        elif delay_ms >= th.HEARTBEAT_CRITICAL_MS:
            return SeverityAssessment.CRITICAL
        elif delay_ms >= th.HEARTBEAT_WARNING_MS:
            return SeverityAssessment.WARNING
        else:
            return SeverityAssessment.RESOLVED

    def _generate_output(
        self,
        input_data: AgentInput,
        data: CollectedData,
        diagnosis: Diagnosis,
        time_range: TimeRange
    ) -> AgentOutput:
        """Phase 5: Generate the complete agent output."""
        # Build evidence
        evidence = self._build_evidence(input_data, data)

        # Build affected components
        affected = self._build_affected_components(input_data, data)

        # Build remediation plan
        remediation = self._build_remediation(diagnosis, data)

        # Build investigation summary
        summary = InvestigationSummary(
            metrics_checked=data.metrics_checked,
            time_range_start=time_range.start,
            time_range_end=time_range.end,
            data_gaps=data.data_gaps,
            assumptions=[]
        )

        return AgentOutput(
            diagnosis=diagnosis,
            evidence=evidence,
            affected_components=affected,
            remediation=remediation,
            investigation_summary=summary
        )

    def _build_evidence(self, input_data: AgentInput, data: CollectedData) -> Evidence:
        """Build evidence from collected data."""
        th = self.thresholds
        primary = []
        supporting = []

        # Heartbeat delay is always primary
        if data.heartbeat_delay is not None:
            primary.append(MetricIndicator(
                metric="ingestion.replication.heartbeat.delay",
                value=data.heartbeat_delay,
                threshold=th.HEARTBEAT_CRITICAL_MS,
                status=self._get_metric_status(
                    data.heartbeat_delay,
                    th.HEARTBEAT_WARNING_MS,
                    th.HEARTBEAT_CRITICAL_MS
                )
            ))

        # Add other metrics as supporting evidence
        if data.bytes_consumed is not None:
            supporting.append(MetricIndicator(
                metric="bytes_consumed",
                value=data.bytes_consumed,
                threshold=th.BYTES_CONSUMED_MIN,
                status=MetricStatus.CRITICAL if data.bytes_consumed == 0 else MetricStatus.NORMAL
            ))

        if data.storage_put_latency is not None:
            status = self._get_metric_status(
                data.storage_put_latency,
                th.STORAGE_PUT_LATENCY_WARNING_MS,
                th.STORAGE_PUT_LATENCY_CRITICAL_MS
            )
            indicator = MetricIndicator(
                metric="storage_engine_put_latency",
                value=data.storage_put_latency,
                threshold=th.STORAGE_PUT_LATENCY_WARNING_MS,
                status=status
            )
            if status != MetricStatus.NORMAL:
                primary.append(indicator)
            else:
                supporting.append(indicator)

        if data.storage_quota_used is not None:
            status = self._get_metric_status(
                data.storage_quota_used,
                th.QUOTA_WARNING_PERCENT,
                th.QUOTA_CRITICAL_PERCENT
            )
            indicator = MetricIndicator(
                metric="storage_quota_used",
                value=data.storage_quota_used,
                threshold=th.QUOTA_CRITICAL_PERCENT,
                status=status
            )
            if status == MetricStatus.CRITICAL:
                primary.append(indicator)
            else:
                supporting.append(indicator)

        if data.consumer_lag:
            status = self._get_metric_status(
                data.consumer_lag.total_lag,
                th.CONSUMER_LAG_WARNING,
                th.CONSUMER_LAG_CRITICAL
            )
            indicator = MetricIndicator(
                metric="kafka_consumer_lag",
                value=float(data.consumer_lag.total_lag),
                threshold=float(th.CONSUMER_LAG_WARNING),
                status=status
            )
            if status != MetricStatus.NORMAL:
                primary.append(indicator)
            else:
                supporting.append(indicator)

        # Log evidence
        log_evidence = []
        for error in data.error_logs[:5]:  # Limit to 5 entries
            if hasattr(error, 'sample_message'):
                log_evidence.append(LogEvidence(
                    timestamp=datetime.utcnow(),
                    message=error.sample_message,
                    relevance=f"Error type: {error.error_type}, count: {error.count}"
                ))

        return Evidence(
            primary_indicators=primary,
            supporting_indicators=supporting,
            log_evidence=log_evidence
        )

    def _get_metric_status(
        self,
        value: float,
        warning_threshold: float,
        critical_threshold: float
    ) -> MetricStatus:
        """Get metric status based on thresholds."""
        if value >= critical_threshold:
            return MetricStatus.CRITICAL
        elif value >= warning_threshold:
            return MetricStatus.WARNING
        else:
            return MetricStatus.NORMAL

    def _build_affected_components(
        self,
        input_data: AgentInput,
        data: CollectedData
    ) -> AffectedComponents:
        """Build list of affected components."""
        hosts = list(data.leader_hosts)

        partitions = []
        if data.offline_replicas:
            partitions = [r["partition"] for r in data.offline_replicas]
        elif data.consumer_lag and data.consumer_lag.partition_lags:
            # Include partitions with high lag
            partitions = [
                p for p, lag in data.consumer_lag.partition_lags.items()
                if lag > self.thresholds.CONSUMER_LAG_WARNING
            ]

        regions = []
        if input_data.region:
            regions = [input_data.region]
        elif data.store_info:
            regions = data.store_info.regions

        return AffectedComponents(
            hosts=hosts,
            partitions=partitions,
            regions=regions
        )

    def _build_remediation(
        self,
        diagnosis: Diagnosis,
        data: CollectedData
    ) -> Remediation:
        """Build remediation plan based on diagnosis."""
        immediate = []
        follow_up = []
        escalation_needed = False
        escalation_reason = None

        category = diagnosis.root_cause_category

        if category == RootCauseCategory.CONSUMER_LAG:
            immediate.append(RemediationAction(
                action="Check Kafka consumer group health and connectivity",
                command="kafka-consumer-groups --bootstrap-server <broker> --describe --group <group>",
                risk=Risk.LOW,
                expected_impact="Identify consumer state and lag details"
            ))
            immediate.append(RemediationAction(
                action="Verify Kafka broker health",
                risk=Risk.LOW,
                expected_impact="Identify any broker issues"
            ))
            follow_up.append(RemediationAction(
                action="Consider increasing consumer thread count if lag persists",
                risk=Risk.MEDIUM
            ))

        elif category == RootCauseCategory.STORAGE_BOTTLENECK:
            immediate.append(RemediationAction(
                action="Check RocksDB compaction status and trigger manual compaction if needed",
                command="venice-admin-tool --trigger-compaction --store <store>",
                risk=Risk.MEDIUM,
                expected_impact="Clear compaction backlog, temporary increased I/O"
            ))
            follow_up.append(RemediationAction(
                action="Review RocksDB tuning parameters",
                risk=Risk.LOW
            ))
            follow_up.append(RemediationAction(
                action="Consider adding SSD capacity if disk is near full",
                risk=Risk.LOW
            ))

        elif category == RootCauseCategory.LEADER_PRODUCTION:
            immediate.append(RemediationAction(
                action="Check VT topic broker health",
                risk=Risk.LOW,
                expected_impact="Identify Kafka production issues"
            ))
            immediate.append(RemediationAction(
                action="Verify leader election state for affected partitions",
                command="venice-admin-tool --get-replica-status --store <store>",
                risk=Risk.LOW
            ))
            follow_up.append(RemediationAction(
                action="Consider restarting affected server nodes if leader is stuck",
                risk=Risk.HIGH
            ))

        elif category == RootCauseCategory.DCR_CONFLICTS:
            immediate.append(RemediationAction(
                action="Check clock synchronization across regions",
                risk=Risk.LOW,
                expected_impact="Identify clock skew issues"
            ))
            immediate.append(RemediationAction(
                action="Review write patterns for hot keys",
                risk=Risk.LOW
            ))
            follow_up.append(RemediationAction(
                action="Consider key partitioning strategy to reduce conflicts",
                risk=Risk.MEDIUM
            ))

        elif category == RootCauseCategory.QUOTA_EXHAUSTION:
            immediate.append(RemediationAction(
                action="Increase store quota",
                command="venice-admin-tool --update-store --store <store> --quota <new_quota>",
                risk=Risk.LOW,
                expected_impact="Allow ingestion to resume"
            ))
            follow_up.append(RemediationAction(
                action="Review data retention and cleanup old versions",
                risk=Risk.LOW
            ))
            follow_up.append(RemediationAction(
                action="Investigate unexpected data growth",
                risk=Risk.LOW
            ))

        elif category == RootCauseCategory.PARTITION_ISSUES:
            immediate.append(RemediationAction(
                action="Check Helix partition assignment and state transitions",
                risk=Risk.LOW
            ))
            immediate.append(RemediationAction(
                action="Verify affected hosts are healthy",
                risk=Risk.LOW
            ))
            follow_up.append(RemediationAction(
                action="Consider triggering partition rebalance",
                risk=Risk.MEDIUM
            ))

        elif category == RootCauseCategory.INGESTION_ERRORS:
            immediate.append(RemediationAction(
                action="Review error logs for specific failure patterns",
                risk=Risk.LOW,
                expected_impact="Identify root cause of ingestion failures"
            ))
            follow_up.append(RemediationAction(
                action="Address specific errors found in logs",
                risk=Risk.MEDIUM
            ))

        else:  # UNKNOWN or NO_DATA
            immediate.append(RemediationAction(
                action="Manual investigation required - review all metrics and logs",
                risk=Risk.LOW
            ))
            escalation_needed = True
            escalation_reason = "Unable to determine root cause automatically"

        # Check if escalation needed based on severity
        if diagnosis.severity_assessment == SeverityAssessment.EMERGENCY:
            escalation_needed = True
            escalation_reason = escalation_reason or "Emergency severity - immediate attention required"

        return Remediation(
            immediate_actions=immediate,
            follow_up_actions=follow_up,
            escalation_needed=escalation_needed,
            escalation_reason=escalation_reason
        )
