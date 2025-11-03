"""
File name: src/data_lakehouse_ingest/utils/report_utils.py

Generates structured ingestion reports summarizing pipeline execution.
Computes duration, status, and per-table results with optional error and extra context.
"""

from datetime import datetime, timezone
from typing import Any
from typing import Any, TypedDict

class TableReport(TypedDict, total=False):
    name: str
    rows: int
    status: str
    duration_sec: float
    message: str

class ErrorReport(TypedDict, total=False):
    phase: str
    error: str
    table: str | None
    stacktrace: str | None

def _normalize_to_utc(ts: str) -> str:
    """Convert an ISO 8601 timestamp string to UTC ISO format."""
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        # Assume naive datetimes are UTC (could log a warning if desired)
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        # Convert any non-UTC timezone to UTC
        dt = dt.astimezone(timezone.utc)
    return dt.isoformat()


def generate_report(
    success: bool = True,
    started_at: str | None = None,
    tables: list[TableReport] | None = None,
    errors: list[ErrorReport] | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Generate a consistent ingestion report structure.

    Args:
        success (bool, defaults to True): Whether the operation succeeded overall.
        started_at (str, optional): ISO timestamp when the operation started.
        tables (list, optional): List of table-level report dictionaries.
        errors (list, optional): List of error entries with 'phase', 'error', etc.
        extra (dict, optional): Additional key-value pairs to merge (e.g., for partial errors).

    Returns:
        dict: Structured ingestion report.
    """
    if started_at is None:
        started_at = datetime.now(timezone.utc).isoformat()
    else:
        started_at = _normalize_to_utc(started_at)

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
