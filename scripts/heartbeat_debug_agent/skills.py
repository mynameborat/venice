"""
Skill interfaces for the Heartbeat Debug Agent.

These are abstract base classes that define the interface for each skill.
Concrete implementations should be provided for your specific infrastructure.
"""

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

from .models import (
    TimeSeries,
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


class TimeRange:
    """Represents a time range for queries."""

    def __init__(self, start: datetime, end: datetime):
        self.start = start
        self.end = end

    @classmethod
    def last_minutes(cls, minutes: int) -> "TimeRange":
        end = datetime.utcnow()
        start = end - timedelta(minutes=minutes)
        return cls(start, end)

    @classmethod
    def around_timestamp(cls, timestamp: datetime, window_minutes: int = 30) -> "TimeRange":
        delta = timedelta(minutes=window_minutes)
        return cls(timestamp - delta, timestamp + timedelta(minutes=5))


class MetricsQuerySkill(ABC):
    """
    Skill for querying metrics from observability systems.

    Implementations might use:
    - InGraphs API
    - Prometheus
    - OpenTelemetry Collector
    - Datadog
    - etc.
    """

    @abstractmethod
    def query_metric(
        self,
        metric_name: str,
        dimensions: Dict[str, str],
        time_range: TimeRange,
        aggregation: str = "avg",
    ) -> Optional[TimeSeries]:
        """
        Query a single metric.

        Args:
            metric_name: Name of the metric
            dimensions: Dimension filters (e.g., {"store_name": "my-store"})
            time_range: Time range to query
            aggregation: Aggregation function (avg, max, min, sum, rate, p50, p99)

        Returns:
            TimeSeries with the metric data, or None if not found
        """
        pass

    @abstractmethod
    def query_multiple_metrics(
        self,
        queries: List[Dict[str, Any]],
        time_range: TimeRange,
    ) -> Dict[str, Optional[TimeSeries]]:
        """
        Query multiple metrics in parallel.

        Args:
            queries: List of query specifications, each containing:
                - metric_name: str
                - dimensions: Dict[str, str]
                - aggregation: str (optional)
            time_range: Time range to query

        Returns:
            Dict mapping metric names to their TimeSeries data
        """
        pass

    def get_current_value(
        self,
        metric_name: str,
        dimensions: Dict[str, str],
    ) -> Optional[float]:
        """Get the most recent value of a metric."""
        time_range = TimeRange.last_minutes(5)
        ts = self.query_metric(metric_name, dimensions, time_range)
        return ts.current_value if ts else None


class KafkaOperationsSkill(ABC):
    """
    Skill for Kafka operations.

    Implementations might use:
    - Kafka Admin Client
    - Cruise Control API
    - Burrow
    - etc.
    """

    @abstractmethod
    def get_consumer_group_lag(
        self,
        consumer_group: str,
        topic: Optional[str] = None,
    ) -> Optional[ConsumerLagInfo]:
        """
        Get consumer group lag information.

        Args:
            consumer_group: The consumer group name
            topic: Optional topic to filter by

        Returns:
            ConsumerLagInfo with lag details
        """
        pass

    @abstractmethod
    def get_topic_partition_count(self, topic_name: str) -> Optional[int]:
        """Get the number of partitions for a topic."""
        pass

    @abstractmethod
    def get_broker_health(
        self,
        broker_ids: Optional[List[int]] = None,
    ) -> List[BrokerHealth]:
        """
        Get health status of Kafka brokers.

        Args:
            broker_ids: Optional list of specific broker IDs to check

        Returns:
            List of BrokerHealth for each broker
        """
        pass

    def get_rt_topic_name(self, store_name: str) -> str:
        """Get the real-time topic name for a store."""
        return f"{store_name}_rt"

    def get_vt_topic_name(self, store_name: str, version: int) -> str:
        """Get the version topic name for a store version."""
        return f"{store_name}_v{version}"


class VeniceAdminSkill(ABC):
    """
    Skill for Venice admin operations.

    Implementations might use:
    - Venice Admin Tool CLI
    - Venice Controller REST API
    - etc.
    """

    @abstractmethod
    def get_store_info(
        self,
        store_name: str,
        cluster_name: str,
    ) -> Optional[StoreInfo]:
        """
        Get store information.

        Args:
            store_name: Name of the store
            cluster_name: Name of the cluster

        Returns:
            StoreInfo with store details
        """
        pass

    @abstractmethod
    def get_current_version(
        self,
        store_name: str,
        cluster_name: str,
    ) -> Optional[int]:
        """Get the current serving version of a store."""
        pass

    @abstractmethod
    def get_replica_status(
        self,
        store_name: str,
        cluster_name: str,
        version: Optional[int] = None,
    ) -> List[ReplicaInfo]:
        """
        Get replica status for a store version.

        Args:
            store_name: Name of the store
            cluster_name: Name of the cluster
            version: Optional version number (defaults to current)

        Returns:
            List of ReplicaInfo for all replicas
        """
        pass

    @abstractmethod
    def get_ingestion_task_status(
        self,
        store_name: str,
        cluster_name: str,
        host: Optional[str] = None,
    ) -> List[IngestionTaskInfo]:
        """
        Get ingestion task status.

        Args:
            store_name: Name of the store
            cluster_name: Name of the cluster
            host: Optional host to filter by

        Returns:
            List of IngestionTaskInfo
        """
        pass


class HostHealthSkill(ABC):
    """
    Skill for host-level health checks.

    Implementations might use:
    - Node exporter / Prometheus
    - Custom host metrics collector
    - Cloud provider APIs
    - etc.
    """

    @abstractmethod
    def get_host_metrics(
        self,
        hostname: str,
    ) -> Optional[HostMetrics]:
        """
        Get host-level metrics.

        Args:
            hostname: The host to query

        Returns:
            HostMetrics with resource utilization
        """
        pass

    @abstractmethod
    def get_rocksdb_stats(
        self,
        hostname: str,
        store_name: str,
    ) -> Optional[RocksDBStats]:
        """
        Get RocksDB statistics for a store on a host.

        Args:
            hostname: The host to query
            store_name: The store name

        Returns:
            RocksDBStats with RocksDB metrics
        """
        pass

    def get_multiple_host_metrics(
        self,
        hostnames: List[str],
    ) -> Dict[str, Optional[HostMetrics]]:
        """Get metrics for multiple hosts."""
        return {host: self.get_host_metrics(host) for host in hostnames}


class LogSearchSkill(ABC):
    """
    Skill for searching and analyzing logs.

    Implementations might use:
    - Splunk
    - ELK Stack
    - Loki
    - etc.
    """

    @abstractmethod
    def search_logs(
        self,
        query: str,
        time_range: TimeRange,
        hosts: Optional[List[str]] = None,
        log_level: Optional[str] = None,
        limit: int = 100,
    ) -> List[LogEntry]:
        """
        Search logs.

        Args:
            query: Search query string
            time_range: Time range to search
            hosts: Optional list of hosts to filter by
            log_level: Optional log level filter (ERROR, WARN, INFO)
            limit: Maximum number of results

        Returns:
            List of matching LogEntry
        """
        pass

    @abstractmethod
    def get_error_summary(
        self,
        store_name: str,
        time_range: TimeRange,
    ) -> List[ErrorSummary]:
        """
        Get a summary of errors for a store.

        Args:
            store_name: Store name to search for
            time_range: Time range to analyze

        Returns:
            List of ErrorSummary grouped by error type
        """
        pass

    def search_ingestion_errors(
        self,
        store_name: str,
        time_range: TimeRange,
        limit: int = 50,
    ) -> List[LogEntry]:
        """Search for ingestion-related errors."""
        query = f'store="{store_name}" AND level=ERROR AND (message:*ingestion* OR message:*consume*)'
        return self.search_logs(query, time_range, log_level="ERROR", limit=limit)

    def search_storage_errors(
        self,
        store_name: str,
        time_range: TimeRange,
        limit: int = 50,
    ) -> List[LogEntry]:
        """Search for storage-related errors."""
        query = f'store="{store_name}" AND level=ERROR AND (message:*rocksdb* OR message:*storage*)'
        return self.search_logs(query, time_range, log_level="ERROR", limit=limit)


class HelixStateSkill(ABC):
    """
    Skill for querying Helix cluster state.

    Implementations might use:
    - Helix REST API
    - ZooKeeper client
    - etc.
    """

    @abstractmethod
    def get_partition_assignment(
        self,
        cluster_name: str,
        resource: str,
    ) -> Dict[int, List[str]]:
        """
        Get partition to host assignments.

        Args:
            cluster_name: Helix cluster name
            resource: Resource name (store_name_v{version})

        Returns:
            Dict mapping partition ID to list of assigned hosts
        """
        pass

    @abstractmethod
    def get_offline_instances(
        self,
        cluster_name: str,
    ) -> List[str]:
        """
        Get list of offline instances in a cluster.

        Args:
            cluster_name: Helix cluster name

        Returns:
            List of offline instance names
        """
        pass

    @abstractmethod
    def get_stuck_state_transitions(
        self,
        cluster_name: str,
        resource: str,
        time_range: TimeRange,
    ) -> List[Dict[str, Any]]:
        """
        Get state transitions that appear stuck.

        Args:
            cluster_name: Helix cluster name
            resource: Resource name
            time_range: Time range to check

        Returns:
            List of stuck transition details
        """
        pass


# Skill Registry for dependency injection

class SkillRegistry:
    """Registry for skill implementations."""

    def __init__(self):
        self._skills: Dict[str, Any] = {}

    def register(self, skill_type: str, skill_instance: Any) -> None:
        """Register a skill implementation."""
        self._skills[skill_type] = skill_instance

    def get_metrics_skill(self) -> MetricsQuerySkill:
        return self._skills.get("metrics")

    def get_kafka_skill(self) -> KafkaOperationsSkill:
        return self._skills.get("kafka")

    def get_venice_admin_skill(self) -> VeniceAdminSkill:
        return self._skills.get("venice_admin")

    def get_host_health_skill(self) -> HostHealthSkill:
        return self._skills.get("host_health")

    def get_log_search_skill(self) -> LogSearchSkill:
        return self._skills.get("log_search")

    def get_helix_state_skill(self) -> HelixStateSkill:
        return self._skills.get("helix_state")
