"""
Mock skill implementations for testing the Heartbeat Debug Agent.

These implementations return simulated data for testing purposes.
Replace with real implementations for production use.
"""

from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import random

from .models import (
    TimeSeries,
    DataPoint,
    ConsumerLagInfo,
    BrokerHealth,
    StoreInfo,
    ReplicaInfo,
    IngestionTaskInfo,
    HostMetrics,
    RocksDBStats,
    LogEntry,
    ErrorSummary,
)
from .skills import (
    TimeRange,
    MetricsQuerySkill,
    KafkaOperationsSkill,
    VeniceAdminSkill,
    HostHealthSkill,
    LogSearchSkill,
    HelixStateSkill,
)


class MockMetricsQuerySkill(MetricsQuerySkill):
    """Mock implementation of MetricsQuerySkill for testing."""

    def __init__(self, scenario: str = "healthy"):
        """
        Initialize with a scenario.

        Scenarios:
        - "healthy": All metrics normal
        - "consumer_stalled": Consumer not making progress
        - "storage_bottleneck": High storage latency
        - "leader_stuck": Leader not producing
        - "dcr_conflicts": High DCR conflict rate
        - "quota_exhausted": Store quota full
        """
        self.scenario = scenario
        self._setup_scenario_data()

    def _setup_scenario_data(self):
        """Setup mock data based on scenario."""
        self.mock_data = {
            "ingestion.replication.heartbeat.delay": 500,
            "bytes_consumed": 1000000,
            "records_consumed": 1000,
            "leader_bytes_consumed": 500000,
            "leader_records_consumed": 500,
            "leader_bytes_produced": 500000,
            "leader_produce_latency": 20,
            "storage_engine_put_latency": 5,
            "storage_engine_delete_latency": 3,
            "disk_usage_in_bytes": 50000000000,
            "storage_quota_used": 50,
            "update_ignored_dcr": 0,
            "tombstone_creation_dcr": 0,
            "timestamp_regression_dcr_error": 0,
            "ingestion_failure": 0,
            "checksum_verification_failure": 0,
        }

        if self.scenario == "consumer_stalled":
            self.mock_data["ingestion.replication.heartbeat.delay"] = 15000
            self.mock_data["bytes_consumed"] = 0
            self.mock_data["records_consumed"] = 0

        elif self.scenario == "storage_bottleneck":
            self.mock_data["ingestion.replication.heartbeat.delay"] = 8000
            self.mock_data["storage_engine_put_latency"] = 125
            self.mock_data["storage_engine_delete_latency"] = 80

        elif self.scenario == "leader_stuck":
            self.mock_data["ingestion.replication.heartbeat.delay"] = 20000
            self.mock_data["leader_bytes_produced"] = 0
            self.mock_data["leader_produce_latency"] = 500

        elif self.scenario == "dcr_conflicts":
            self.mock_data["ingestion.replication.heartbeat.delay"] = 5000
            self.mock_data["update_ignored_dcr"] = 500
            self.mock_data["timestamp_regression_dcr_error"] = 10

        elif self.scenario == "quota_exhausted":
            self.mock_data["ingestion.replication.heartbeat.delay"] = 30000
            self.mock_data["storage_quota_used"] = 100
            self.mock_data["bytes_consumed"] = 0

    def query_metric(
        self,
        metric_name: str,
        dimensions: Dict[str, str],
        time_range: TimeRange,
        aggregation: str = "avg",
    ) -> Optional[TimeSeries]:
        if metric_name not in self.mock_data:
            return None

        value = self.mock_data[metric_name]

        # Generate time series with some variance
        data_points = []
        current = time_range.start
        while current <= time_range.end:
            variance = random.uniform(0.9, 1.1)
            data_points.append(DataPoint(timestamp=current, value=value * variance))
            current += timedelta(minutes=1)

        return TimeSeries(metric_name=metric_name, data_points=data_points)

    def query_multiple_metrics(
        self,
        queries: List[Dict[str, Any]],
        time_range: TimeRange,
    ) -> Dict[str, Optional[TimeSeries]]:
        results = {}
        for query in queries:
            metric_name = query["metric_name"]
            dimensions = query.get("dimensions", {})
            aggregation = query.get("aggregation", "avg")
            results[metric_name] = self.query_metric(
                metric_name, dimensions, time_range, aggregation
            )
        return results


class MockKafkaOperationsSkill(KafkaOperationsSkill):
    """Mock implementation of KafkaOperationsSkill for testing."""

    def __init__(self, scenario: str = "healthy"):
        self.scenario = scenario

    def get_consumer_group_lag(
        self,
        consumer_group: str,
        topic: Optional[str] = None,
    ) -> Optional[ConsumerLagInfo]:
        if self.scenario == "consumer_stalled":
            return ConsumerLagInfo(
                consumer_group=consumer_group,
                total_lag=500000,
                partition_lags={0: 100000, 1: 150000, 2: 100000, 3: 150000},
                last_commit_time=datetime.utcnow() - timedelta(minutes=10)
            )

        return ConsumerLagInfo(
            consumer_group=consumer_group,
            total_lag=100,
            partition_lags={0: 25, 1: 25, 2: 25, 3: 25},
            last_commit_time=datetime.utcnow() - timedelta(seconds=5)
        )

    def get_topic_partition_count(self, topic_name: str) -> Optional[int]:
        return 16

    def get_broker_health(
        self,
        broker_ids: Optional[List[int]] = None,
    ) -> List[BrokerHealth]:
        brokers = broker_ids or [1, 2, 3]
        return [
            BrokerHealth(
                broker_id=bid,
                is_healthy=True,
                under_replicated_partitions=0
            )
            for bid in brokers
        ]


class MockVeniceAdminSkill(VeniceAdminSkill):
    """Mock implementation of VeniceAdminSkill for testing."""

    def __init__(self, scenario: str = "healthy"):
        self.scenario = scenario

    def get_store_info(
        self,
        store_name: str,
        cluster_name: str,
    ) -> Optional[StoreInfo]:
        return StoreInfo(
            store_name=store_name,
            current_version=5,
            partition_count=16,
            replication_factor=3,
            hybrid_enabled=True,
            active_active_enabled=self.scenario == "dcr_conflicts",
            quota_bytes=100000000000,
            regions=["ltx1", "lor1", "lsg1"]
        )

    def get_current_version(
        self,
        store_name: str,
        cluster_name: str,
    ) -> Optional[int]:
        return 5

    def get_replica_status(
        self,
        store_name: str,
        cluster_name: str,
        version: Optional[int] = None,
    ) -> List[ReplicaInfo]:
        replicas = []
        hosts = [f"venice-server-{i:03d}" for i in range(1, 7)]

        for partition in range(16):
            # Each partition has 3 replicas, first is leader
            for idx, host in enumerate(hosts[partition % 3: (partition % 3) + 3]):
                replicas.append(ReplicaInfo(
                    partition_id=partition,
                    host=host,
                    is_leader=(idx == 0),
                    state="ONLINE"
                ))

        return replicas

    def get_ingestion_task_status(
        self,
        store_name: str,
        cluster_name: str,
        host: Optional[str] = None,
    ) -> List[IngestionTaskInfo]:
        tasks = []
        for partition in range(16):
            tasks.append(IngestionTaskInfo(
                partition_id=partition,
                state="CONSUMING",
                current_offset=1000000,
                lag=100 if self.scenario == "healthy" else 50000
            ))
        return tasks


class MockHostHealthSkill(HostHealthSkill):
    """Mock implementation of HostHealthSkill for testing."""

    def __init__(self, scenario: str = "healthy"):
        self.scenario = scenario

    def get_host_metrics(
        self,
        hostname: str,
    ) -> Optional[HostMetrics]:
        if self.scenario == "storage_bottleneck":
            return HostMetrics(
                hostname=hostname,
                cpu_usage_percent=75,
                memory_usage_percent=80,
                disk_io_utilization=92,
                disk_usage_percent=85
            )

        return HostMetrics(
            hostname=hostname,
            cpu_usage_percent=40,
            memory_usage_percent=60,
            disk_io_utilization=30,
            disk_usage_percent=50
        )

    def get_rocksdb_stats(
        self,
        hostname: str,
        store_name: str,
    ) -> Optional[RocksDBStats]:
        if self.scenario == "storage_bottleneck":
            return RocksDBStats(
                hostname=hostname,
                compaction_pending=50,
                block_cache_hit_ratio=0.75,
                write_stall_duration_ms=100
            )

        return RocksDBStats(
            hostname=hostname,
            compaction_pending=0,
            block_cache_hit_ratio=0.95,
            write_stall_duration_ms=0
        )


class MockLogSearchSkill(LogSearchSkill):
    """Mock implementation of LogSearchSkill for testing."""

    def __init__(self, scenario: str = "healthy"):
        self.scenario = scenario

    def search_logs(
        self,
        query: str,
        time_range: TimeRange,
        hosts: Optional[List[str]] = None,
        log_level: Optional[str] = None,
        limit: int = 100,
    ) -> List[LogEntry]:
        if self.scenario == "healthy":
            return []

        logs = []
        if self.scenario == "storage_bottleneck":
            logs.append(LogEntry(
                timestamp=datetime.utcnow() - timedelta(minutes=5),
                level="WARN",
                message="RocksDB compaction taking longer than expected",
                host="venice-server-001"
            ))

        elif self.scenario == "consumer_stalled":
            logs.append(LogEntry(
                timestamp=datetime.utcnow() - timedelta(minutes=2),
                level="ERROR",
                message="Kafka consumer poll timeout - broker may be unavailable",
                host="venice-server-001"
            ))

        return logs[:limit]

    def get_error_summary(
        self,
        store_name: str,
        time_range: TimeRange,
    ) -> List[ErrorSummary]:
        if self.scenario == "healthy":
            return []

        if self.scenario == "storage_bottleneck":
            return [
                ErrorSummary(
                    error_type="RocksDB Write Stall",
                    count=15,
                    sample_message="Write stall detected - compaction backlog"
                )
            ]

        if self.scenario == "consumer_stalled":
            return [
                ErrorSummary(
                    error_type="Kafka Poll Timeout",
                    count=30,
                    sample_message="Consumer poll timeout after 30000ms"
                )
            ]

        return []


class MockHelixStateSkill(HelixStateSkill):
    """Mock implementation of HelixStateSkill for testing."""

    def __init__(self, scenario: str = "healthy"):
        self.scenario = scenario

    def get_partition_assignment(
        self,
        cluster_name: str,
        resource: str,
    ) -> Dict[int, List[str]]:
        hosts = [f"venice-server-{i:03d}" for i in range(1, 7)]
        assignments = {}
        for partition in range(16):
            assignments[partition] = hosts[partition % 3: (partition % 3) + 3]
        return assignments

    def get_offline_instances(
        self,
        cluster_name: str,
    ) -> List[str]:
        return []

    def get_stuck_state_transitions(
        self,
        cluster_name: str,
        resource: str,
        time_range: TimeRange,
    ) -> List[Dict[str, Any]]:
        return []


def create_mock_skill_registry(scenario: str = "healthy"):
    """
    Create a skill registry with mock implementations.

    Args:
        scenario: The scenario to simulate

    Returns:
        SkillRegistry with mock skills
    """
    from .skills import SkillRegistry

    registry = SkillRegistry()
    registry.register("metrics", MockMetricsQuerySkill(scenario))
    registry.register("kafka", MockKafkaOperationsSkill(scenario))
    registry.register("venice_admin", MockVeniceAdminSkill(scenario))
    registry.register("host_health", MockHostHealthSkill(scenario))
    registry.register("log_search", MockLogSearchSkill(scenario))
    registry.register("helix_state", MockHelixStateSkill(scenario))

    return registry
