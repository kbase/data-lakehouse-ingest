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


def _normalize_to_utc(ts: str | None) -> datetime:
    """
    Normalize a timestamp to a timezone-aware UTC datetime object.
    
    This helper accepts an ISO 8601 timestamp string or None.  
    - If `ts` is None, the current UTC time is returned.  
    - If `ts` is a naive datetime string (no timezone), it is assumed to already
      represent UTC and is converted to a UTC-aware datetime.
    - If `ts` includes a timezone offset, it is converted to UTC.

    Args:
        ts (str | None): ISO 8601 timestamp string (naive or timezone-aware) or None.

    Returns:
        datetime: A timezone-aware UTC datetime object.
    """
    if ts is None:
        return datetime.now(timezone.utc)

    dt = datetime.fromisoformat(ts)

    if dt.tzinfo is None:
        # Assume naive datetime is UTC
        return dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


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
    started_at_dt = _normalize_to_utc(started_at)
    ended_at_dt = datetime.now(timezone.utc)
    duration_sec = (ended_at_dt - started_at_dt).total_seconds()

    report = {
        "success": success,
        "started_at": started_at_dt.isoformat(),
        "ended_at": ended_at_dt.isoformat(),
        "duration_sec": duration_sec,
        "tables": tables,
        "errors": errors or [],
    }

    if extra:
        report.update(extra)

    return report
