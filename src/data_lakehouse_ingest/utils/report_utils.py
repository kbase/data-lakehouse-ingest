"""
Pydantic-based ingestion reporting utilities.

Provides validated, structured report models used to summarize data ingestion
pipeline execution. Ensures strong typing, automatic datetime handling, and
consistent JSON-serializable report output.
"""

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field, validator


class TableReport(BaseModel):
    """
    Represents the outcome of ingesting a single table.

    Attributes:
        name (str): Name of the table being processed.
        rows (int | None): Number of rows successfully ingested or processed.
        status (str): Outcome of the ingestion (e.g., "success", "failed").
        duration_sec (float | None): Time taken for table processing in seconds.
        message (str | None): Additional context or status message.
    """
    name: str = Field(..., description="Name of the table")
    rows: int | None = Field(None, description="Row count for the table")
    status: str = Field(..., description="Status of ingestion (success/failed)")
    duration_sec: float | None = Field(None, description="Processing duration")
    message: str | None = Field(None, description="Additional context or notes")


class ErrorReport(BaseModel):
    """
    Represents a structured error entry within an ingestion report.

    Attributes:
        phase (str): Pipeline phase where the error occurred.
        error (str): Brief error or exception message.
        table (str | None): Name of the table associated with the error, if applicable.
        stacktrace (str | None): Optional detailed stack trace.
    """
    phase: str = Field(..., description="Pipeline phase where error occurred")
    error: str = Field(..., description="Error message")
    table: str | None = Field(None, description="Table name related to the error")
    stacktrace: str | None = Field(None, description="Stack trace for debugging")


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
        # Assume naive datetimes are UTC
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
        success (bool): Whether the operation succeeded overall.
        started_at (str | None): ISO timestamp when ingestion started.
        tables (list[TableReport] | None): Per-table ingestion results.
        errors (list[ErrorReport] | None): Error entries for the run.
        extra (dict[str, Any] | None): Additional metadata fields.

    Returns:
        dict[str, Any]: Structured ingestion report.
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
