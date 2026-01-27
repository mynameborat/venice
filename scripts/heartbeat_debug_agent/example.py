#!/usr/bin/env python3
"""
Example usage of the Heartbeat Debug Agent.

This script demonstrates how to use the agent with different scenarios.
"""

import json
from datetime import datetime

from heartbeat_debug_agent import (
    HeartbeatDebugAgent,
    AgentInput,
    Severity,
)
from heartbeat_debug_agent.mock_skills import create_mock_skill_registry


def run_scenario(scenario_name: str, delay_ms: float):
    """Run the agent with a specific scenario."""
    print(f"\n{'='*80}")
    print(f"SCENARIO: {scenario_name.upper()}")
    print(f"{'='*80}\n")

    # Create skill registry with mock implementations
    registry = create_mock_skill_registry(scenario_name)

    # Create agent
    agent = HeartbeatDebugAgent(registry)

    # Create input
    input_data = AgentInput(
        store_name="member-features-store",
        cluster_name="venice-prod-ltx1",
        alert_timestamp=datetime.utcnow(),
        current_delay_ms=delay_ms,
        region="ltx1",
        baseline_delay_ms=500,
        severity=Severity.CRITICAL if delay_ms > 5000 else Severity.WARNING,
    )

    # Run diagnosis
    output = agent.run(input_data)

    # Print results
    print(f"Root Cause: {output.diagnosis.root_cause}")
    print(f"Category: {output.diagnosis.root_cause_category.value}")
    print(f"Confidence: {output.diagnosis.confidence}%")
    print(f"Severity: {output.diagnosis.severity_assessment.value}")
    print()

    print("Primary Indicators:")
    for ind in output.evidence.primary_indicators:
        print(f"  - {ind.metric}: {ind.value:.2f} ({ind.status.value})")
    print()

    print("Immediate Actions:")
    for action in output.remediation.immediate_actions:
        print(f"  - {action.action}")
        if action.command:
            print(f"    Command: {action.command}")
    print()

    if output.remediation.escalation_needed:
        print(f"⚠️  ESCALATION NEEDED: {output.remediation.escalation_reason}")

    return output


def main():
    """Run all scenarios."""
    scenarios = [
        ("healthy", 500),
        ("consumer_stalled", 15000),
        ("storage_bottleneck", 8000),
        ("leader_stuck", 20000),
        ("dcr_conflicts", 5000),
        ("quota_exhausted", 30000),
    ]

    print("=" * 80)
    print("HEARTBEAT DEBUG AGENT - EXAMPLE SCENARIOS")
    print("=" * 80)

    results = {}
    for scenario, delay in scenarios:
        output = run_scenario(scenario, delay)
        results[scenario] = output.diagnosis.root_cause_category.value

    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print("\nScenario -> Detected Root Cause:")
    for scenario, cause in results.items():
        print(f"  {scenario:20} -> {cause}")


if __name__ == "__main__":
    main()
