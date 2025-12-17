from datetime import datetime, timezone
from data_lakehouse_ingest.utils.report_utils import generate_report


def test_generate_report_basic_success():
    """Test default success case with minimal inputs."""
    report = generate_report()
    assert isinstance(report, dict)
    assert report["success"] is True
    assert "started_at" in report
    assert "ended_at" in report
    assert report["duration_sec"] >= 0
    assert report["errors"] == []


def test_generate_report_with_custom_inputs():
    """Test generate_report with custom values for all optional args."""
    start_time = datetime(2025, 1, 1, tzinfo=timezone.utc).isoformat()
    tables = [{"name": "prefix", "rows": 5}]
    errors = [{"phase": "load", "error": "File not found"}]
    extra = {"run_id": "test_run_001"}

    report = generate_report(
        success=False,
        started_at=start_time,
        tables=tables,
        errors=errors,
        extra=extra,
    )

    # --- Validate core fields ---
    assert report["success"] is False
    assert report["started_at"] == start_time
    assert "ended_at" in report
    assert report["duration_sec"] >= 0

    # --- Validate data content ---
    assert report["tables"] == tables
    assert report["errors"] == errors
    assert report["run_id"] == "test_run_001"


def test_generate_report_merges_extra_fields():
    """Test that extra fields are merged into report correctly."""
    extra = {"pipeline": "ontology_load", "status": "partial"}
    report = generate_report(extra=extra)
    assert report["pipeline"] == "ontology_load"
    assert report["status"] == "partial"
