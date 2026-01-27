#!/usr/bin/env python3
"""
CLI for the Heartbeat Debug Agent.

Usage:
    # Run with mock data (for testing)
    python -m heartbeat_debug_agent.cli --store my-store --cluster venice-prod --delay 15000 --mock storage_bottleneck

    # Run with real skills (requires skill implementations)
    python -m heartbeat_debug_agent.cli --store my-store --cluster venice-prod --delay 15000
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from typing import Optional

from .models import AgentInput, Severity
from .agent import HeartbeatDebugAgent
from .skills import SkillRegistry


def setup_logging(verbose: bool = False):
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Venice Heartbeat Debug Agent - Automated diagnosis of heartbeat delay alerts"
    )

    # Required arguments
    parser.add_argument(
        "--store", "-s",
        required=True,
        help="Store name"
    )
    parser.add_argument(
        "--cluster", "-c",
        required=True,
        help="Cluster name"
    )
    parser.add_argument(
        "--delay", "-d",
        type=float,
        required=True,
        help="Current heartbeat delay in milliseconds"
    )

    # Optional arguments
    parser.add_argument(
        "--region", "-r",
        help="Specific region to investigate"
    )
    parser.add_argument(
        "--partition", "-p",
        type=int,
        help="Specific partition to investigate"
    )
    parser.add_argument(
        "--version", "-V",
        type=int,
        help="Store version number"
    )
    parser.add_argument(
        "--baseline",
        type=float,
        default=500,
        help="Baseline delay in ms (default: 500)"
    )
    parser.add_argument(
        "--severity",
        choices=["warning", "critical", "emergency"],
        default="warning",
        help="Alert severity (default: warning)"
    )
    parser.add_argument(
        "--time-window",
        type=int,
        default=60,
        help="Time window in minutes to analyze (default: 60)"
    )
    parser.add_argument(
        "--timestamp",
        help="Alert timestamp (ISO format, default: now)"
    )

    # Mock mode for testing
    parser.add_argument(
        "--mock",
        choices=["healthy", "consumer_stalled", "storage_bottleneck", "leader_stuck", "dcr_conflicts", "quota_exhausted"],
        help="Use mock skills with specified scenario (for testing)"
    )

    # Output options
    parser.add_argument(
        "--output", "-o",
        choices=["json", "text"],
        default="text",
        help="Output format (default: text)"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )

    return parser.parse_args()


def create_skill_registry(mock_scenario: Optional[str] = None) -> SkillRegistry:
    """
    Create skill registry.

    If mock_scenario is provided, uses mock skills.
    Otherwise, attempts to create real skill implementations.
    """
    if mock_scenario:
        from .mock_skills import create_mock_skill_registry
        return create_mock_skill_registry(mock_scenario)

    # For production, you would implement real skills here
    # For now, raise an error if no mock is specified
    raise NotImplementedError(
        "Real skill implementations not available. "
        "Use --mock flag for testing, or implement real skills."
    )


def format_output_text(output) -> str:
    """Format output as human-readable text."""
    lines = []

    # Header
    lines.append("=" * 80)
    lines.append("HEARTBEAT DEBUG AGENT - DIAGNOSIS REPORT")
    lines.append("=" * 80)
    lines.append("")

    # Diagnosis
    diag = output.diagnosis
    lines.append("DIAGNOSIS")
    lines.append("-" * 40)
    lines.append(f"Root Cause: {diag.root_cause}")
    lines.append(f"Category: {diag.root_cause_category.value}")
    lines.append(f"Confidence: {diag.confidence}%")
    lines.append(f"Severity: {diag.severity_assessment.value.upper()}")
    lines.append("")

    # Evidence
    lines.append("EVIDENCE")
    lines.append("-" * 40)
    lines.append("Primary Indicators:")
    for ind in output.evidence.primary_indicators:
        status_icon = "❌" if ind.status.value == "critical" else "⚠️" if ind.status.value == "warning" else "✅"
        lines.append(f"  {status_icon} {ind.metric}: {ind.value:.2f} (threshold: {ind.threshold})")

    if output.evidence.supporting_indicators:
        lines.append("\nSupporting Indicators:")
        for ind in output.evidence.supporting_indicators:
            lines.append(f"  • {ind.metric}: {ind.value:.2f}")

    if output.evidence.log_evidence:
        lines.append("\nLog Evidence:")
        for log in output.evidence.log_evidence:
            lines.append(f"  • {log.message}")
            lines.append(f"    Relevance: {log.relevance}")
    lines.append("")

    # Affected Components
    affected = output.affected_components
    lines.append("AFFECTED COMPONENTS")
    lines.append("-" * 40)
    if affected.hosts:
        lines.append(f"Hosts: {', '.join(affected.hosts[:5])}" +
                     (f" (+{len(affected.hosts)-5} more)" if len(affected.hosts) > 5 else ""))
    if affected.partitions:
        lines.append(f"Partitions: {affected.partitions[:10]}" +
                     (f" (+{len(affected.partitions)-10} more)" if len(affected.partitions) > 10 else ""))
    if affected.regions:
        lines.append(f"Regions: {', '.join(affected.regions)}")
    lines.append("")

    # Remediation
    lines.append("REMEDIATION")
    lines.append("-" * 40)
    lines.append("Immediate Actions:")
    for i, action in enumerate(output.remediation.immediate_actions, 1):
        lines.append(f"  {i}. {action.action}")
        if action.command:
            lines.append(f"     Command: {action.command}")
        lines.append(f"     Risk: {action.risk.value}")

    if output.remediation.follow_up_actions:
        lines.append("\nFollow-up Actions:")
        for i, action in enumerate(output.remediation.follow_up_actions, 1):
            lines.append(f"  {i}. {action.action} (Risk: {action.risk.value})")

    if output.remediation.escalation_needed:
        lines.append(f"\n⚠️  ESCALATION NEEDED: {output.remediation.escalation_reason}")
    lines.append("")

    # Investigation Summary
    lines.append("INVESTIGATION SUMMARY")
    lines.append("-" * 40)
    lines.append(f"Metrics Checked: {len(output.investigation_summary.metrics_checked)}")
    lines.append(f"Time Range: {output.investigation_summary.time_range_start} to {output.investigation_summary.time_range_end}")
    if output.investigation_summary.data_gaps:
        lines.append(f"Data Gaps: {len(output.investigation_summary.data_gaps)}")
        for gap in output.investigation_summary.data_gaps[:3]:
            lines.append(f"  - {gap}")

    lines.append("")
    lines.append("=" * 80)

    return "\n".join(lines)


def main():
    """Main entry point."""
    args = parse_args()
    setup_logging(args.verbose)

    logger = logging.getLogger(__name__)

    # Parse timestamp
    if args.timestamp:
        alert_timestamp = datetime.fromisoformat(args.timestamp)
    else:
        alert_timestamp = datetime.utcnow()

    # Create input
    input_data = AgentInput(
        store_name=args.store,
        cluster_name=args.cluster,
        alert_timestamp=alert_timestamp,
        current_delay_ms=args.delay,
        region=args.region,
        partition_id=args.partition,
        version_number=args.version,
        baseline_delay_ms=args.baseline,
        severity=Severity(args.severity),
        time_window_minutes=args.time_window
    )

    # Create skill registry
    try:
        registry = create_skill_registry(args.mock)
    except NotImplementedError as e:
        logger.error(str(e))
        sys.exit(1)

    # Create and run agent
    agent = HeartbeatDebugAgent(registry)

    try:
        output = agent.run(input_data)
    except Exception as e:
        logger.error(f"Agent execution failed: {e}")
        if args.verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

    # Format and print output
    if args.output == "json":
        print(json.dumps(output.to_dict(), indent=2, default=str))
    else:
        print(format_output_text(output))


if __name__ == "__main__":
    main()
