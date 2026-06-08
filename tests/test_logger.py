import json
import logging
import os
import pytest
import importlib
import data_lakehouse_ingest.logger as logger_module


@pytest.fixture(autouse=True)
def reload_logger_module():
    importlib.reload(logger_module)
    yield


def test_setup_logger_creates_file_and_logs_context(tmp_path):
    """Verify setup_logger() creates a log file and writes contextual telemetry fields."""
    logger = logger_module.setup_logger(
        log_dir=str(tmp_path),
        logger_name="test_logger",
        pipeline_name="pangenome_pipeline",
        schema="pangenome_schema",
    )

    assert isinstance(logger, logging.Logger)
    assert hasattr(logger, "log_file_path")
    assert os.path.exists(logger.log_file_path)

    logger.info("This is a test message")

    for handler in logger.handlers:
        handler.flush()

    with open(logger.log_file_path, "r") as f:
        log_line = f.readline().strip()

    log_entry = json.loads(log_line)

    assert log_entry["pipeline_name"] == "pangenome_pipeline"
    assert log_entry["target_schema"] == "pangenome_schema"
    assert log_entry["table"] == "pending_config_load"
    assert log_entry["target_table"] == "pending_config_load"
    assert "This is a test message" in log_entry["message"]
    assert log_entry["level"] == "INFO"
    assert log_entry["status"] == "SUCCESS"
    assert log_entry["source_system"] == "ingest_lib"
    assert log_entry["pipeline_run_id"]


def test_logger_returns_same_instance(tmp_path):
    """Verify setup_logger() returns the existing singleton logger instance."""
    logger1 = logger_module.setup_logger(log_dir=str(tmp_path), logger_name="same_logger")
    logger2 = logger_module.setup_logger(log_dir=str(tmp_path), logger_name="same_logger")

    # Both should be identical (singleton behavior)
    assert logger1 is logger2


def test_pipeline_context_filter_injects_fields():
    """Verify PipelineContextFilter injects pipeline context and table metadata into log records."""
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="testing filter",
        args=(),
        exc_info=None,
    )

    # Before applying the filter, the fields should not exist
    assert not hasattr(record, "pipeline_name")
    assert not hasattr(record, "pipeline_run_id")
    assert not hasattr(record, "target_schema")
    assert not hasattr(record, "target_table")

    f = logger_module.PipelineContextFilter(
        "pipelineA",
        "schemaZ",
        pipeline_run_id="run-123",
    )

    f.set_table("tableX")
    f.filter(record)

    # After applying the filter, the fields should exist and match expected values
    assert record.pipeline_name == "pipelineA"
    assert record.pipeline_run_id == "run-123"
    assert record.target_schema == "schemaZ"
    assert record.table == "tableX"
    assert record.target_table == "my.schemaZ.tableX"


def test_setup_logger_uses_pipeline_run_id_from_env(tmp_path, monkeypatch):
    """Verify setup_logger() uses PIPELINE_RUN_ID from the environment when provided."""
    monkeypatch.setenv("PIPELINE_RUN_ID", "env-run-123")

    logger = logger_module.setup_logger(
        log_dir=str(tmp_path),
        logger_name="env_logger",
    )

    logger.info("test env run id")

    for handler in logger.handlers:
        handler.flush()

    with open(logger.log_file_path, "r") as f:
        log_entry = json.loads(f.readline())

    assert log_entry["pipeline_run_id"] == "env-run-123"


def test_pipeline_context_filter_uses_tenant_as_catalog():
    """Verify tenant names are used as the catalog when building fully qualified target tables."""
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="testing tenant catalog",
        args=(),
        exc_info=None,
    )

    f = logger_module.PipelineContextFilter(
        "pipelineA",
        "schemaZ",
        pipeline_run_id="run-123",
        tenant="berdl",
    )

    f.set_table("tableX")
    f.filter(record)

    assert record.tenant == "berdl"
    assert record.target_table == "berdl.schemaZ.tableX"


@pytest.mark.parametrize(
    "level,expected_status",
    [
        (logging.INFO, "SUCCESS"),
        (logging.WARNING, "WARNING"),
        (logging.ERROR, "FAILED"),
    ],
)
def test_json_formatter_sets_status(level, expected_status):
    """Verify JsonLineFormatter maps log severity levels to the expected status values."""
    record = logging.LogRecord(
        name="test",
        level=level,
        pathname=__file__,
        lineno=10,
        msg="status test",
        args=(),
        exc_info=None,
    )

    f = logger_module.PipelineContextFilter(
        "pipelineA",
        "schemaZ",
        pipeline_run_id="run-123",
    )
    f.filter(record)

    formatter = logger_module.JsonLineFormatter()
    payload = json.loads(formatter.format(record))

    assert payload["status"] == expected_status


def test_safe_log_json_writes_summary_event(tmp_path):
    """Verify safe_log_json() emits a structured pipeline summary telemetry event."""
    logger = logger_module.setup_logger(
        log_dir=str(tmp_path),
        logger_name="summary_logger",
        pipeline_name="pipelineA",
        schema="schemaZ",
    )

    summary = {
        "success": True,
        "duration_sec": 10,
        "tables": ["table1"],
        "errors": [],
    }

    logger_module.safe_log_json(logger, summary)

    for handler in logger.handlers:
        handler.flush()

    with open(logger.log_file_path, "r") as f:
        log_entry = json.loads(f.readline())

    assert log_entry["event_type"] == "ingest_run_summary"
    assert log_entry["event_category"] == "pipeline_summary"
    assert log_entry["table"] == "pipeline_stage"
    assert log_entry["target_table"] == "pipeline_stage"
    assert log_entry["summary"] == summary
