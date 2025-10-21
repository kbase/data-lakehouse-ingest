# src/data_lakehouse_ingest/utils/report_utils.py

from datetime import datetime
from typing import Dict, Any, List

def generate_report(
    success: bool,
    started_at: str,
    tables: List[Dict[str, Any]],
    errors: List[Dict[str, Any]] = None,
    extra: Dict[str, Any] = None
) -> Dict[str, Any]:
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
    ended_at = datetime.utcnow().isoformat() + "Z"
    duration_sec = (
        datetime.fromisoformat(ended_at.replace("Z", "")) -
        datetime.fromisoformat(started_at.replace("Z", ""))
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
