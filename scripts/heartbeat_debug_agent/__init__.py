"""
Venice Heartbeat Debug Agent

An automated agent for debugging leader heartbeat delay alerts in Venice.

Usage:
    from heartbeat_debug_agent import HeartbeatDebugAgent, AgentInput, SkillRegistry
    from heartbeat_debug_agent.mock_skills import create_mock_skill_registry

    # Create skill registry (use mock for testing, real implementations for production)
    registry = create_mock_skill_registry(scenario="storage_bottleneck")

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
    print(output.to_dict())
"""

from .models import (
    AgentInput,
    AgentOutput,
    Severity,
    RootCauseCategory,
    MetricStatus,
    SeverityAssessment,
    Risk,
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
from .agent import HeartbeatDebugAgent, Thresholds

__all__ = [
    # Main classes
    "HeartbeatDebugAgent",
    "AgentInput",
    "AgentOutput",
    "Thresholds",
    "SkillRegistry",
    "TimeRange",
    # Enums
    "Severity",
    "RootCauseCategory",
    "MetricStatus",
    "SeverityAssessment",
    "Risk",
    # Skill interfaces
    "MetricsQuerySkill",
    "KafkaOperationsSkill",
    "VeniceAdminSkill",
    "HostHealthSkill",
    "LogSearchSkill",
    "HelixStateSkill",
]
