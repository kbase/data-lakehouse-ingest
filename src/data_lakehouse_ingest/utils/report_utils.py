"""
File name: src/data_lakehouse_ingest/utils/report_utils.py

Generates structured ingestion reports summarizing pipeline execution.
Computes duration, status, and per-table results with optional error and extra context.
"""

from datetime import datetime, timezone
from typing import Any
from typing import Any, TypedDict

class TableReport(TypedDict, total=False):
    """
    Represents a structured entry for a single table in the ingestion report.
    Attributes:
        name (str): Name of the table being processed.
        rows (int): Number of rows successfully loaded or processed.
        status (str): Outcome of the ingestion for this table (e.g., "success", "failed").
        duration_sec (float): Time taken (in seconds) to process this table.
        message (str): Optional descriptive message or additional context.
    """
    name: str
    rows: int
    status: str
    duration_sec: float
    message: str

class ErrorReport(TypedDict, total=False):
    """
    Represents a structured error entry within an ingestion report.
    Attributes:
        phase (str): The pipeline phase during which the error occurred
            (e.g., "config_validation", "data_load", "write_to_delta").
        error (str): A brief description of the error or exception message.
        table (str | None): Name of the table associated with the error, if applicable.
        stacktrace (str | None): Optional detailed stack trace or debugging information.
    """
    phase: str
    error: str
    table: str | None
    stacktrace: str | None

def _normalize_to_utc(ts: str) -> str:
    """
    Convert an ISO 8601 timestamp string to UTC ISO format.
    This helper ensures consistent UTC-based time representation
    across ingestion reports. If the timestamp lacks timezone info,
    it is assumed to be UTC. Non-UTC timezones are converted to UTC.
    Args:
        ts (str): ISO 8601 timestamp string (may be naive or timezone-aware).
    Returns:
        str: ISO 8601 timestamp string normalized to UTC.
    """
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
