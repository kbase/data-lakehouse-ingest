"""
File name: src/data_lakehouse_ingest/utils/report_utils.py

Generates structured ingestion reports summarizing pipeline execution.
Computes duration, status, and per-table results with optional error and extra context.
"""

from datetime import datetime, timezone
from typing import Any

def generate_report(
    success: bool = True,
    started_at: str | None = None,
    tables: list[dict[str, Any]] | None = None,
    errors: list[dict[str, Any]] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Generate a consistent ingestion report structure.

    Args:
        success (bool): Whether the operation succeeded overall.
        started_at (str): ISO timestamp when the operation started.
        tables (list): List of table-level report dictionaries.
        errors (list, optional): List of error entries with 'phase', 'error', etc.
        extra (dict, optional): Additional key-value pairs to merge (e.g., for partial errors).

    Returns:
        dict: Structured ingestion report.
    """
    started_at = started_at or datetime.now(timezone.utc).isoformat()
    ended_at = datetime.now(timezone.utc).isoformat()
    duration_sec = (
        datetime.fromisoformat(ended_at) -
        datetime.fromisoformat(started_at)
    ).total_seconds()

    report = {
        "success": success,
        "started_at": started_at,
        "ended_at": ended_at,
        "duration_sec": duration_sec,
        "tables": tables,
        "errors": errors or [],
    }

    if extra:
        report.update(extra)

    return report
