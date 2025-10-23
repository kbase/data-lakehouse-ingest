import json
import logging
import os
import pytest
from data_lakehouse_ingest.logger import setup_logger, PipelineContextFilter

@pytest.fixture
def temp_log_dir(tmp_path):
    """Fixture to provide a temporary log directory."""
    return tmp_path

def test_setup_logger_creates_file_and_logs_context(temp_log_dir):
    logger = setup_logger(
        log_dir=str(temp_log_dir),
        logger_name="test_logger",
        pipeline_name="pangenome_pipeline",
        target_table="genome",
        schema="pangenome_schema"
    )

    # Verify logger type
    assert isinstance(logger, logging.Logger)

    # Verify file exists
    assert hasattr(logger, "log_file_path")
    assert os.path.exists(logger.log_file_path)

    # Log a sample message
    logger.info("This is a test message")

    # Read the log file
    with open(logger.log_file_path, "r") as f:
        log_line = f.readline().strip()

    # Parse JSON log entry
    log_entry = json.loads(log_line)

    # Verify contextual fields and message content
    assert log_entry["pipeline"] == "pangenome_pipeline"
    assert log_entry["schema"] == "pangenome_schema"
    assert log_entry["table"] == "genome"
    assert "This is a test message" in log_entry["msg"]
    assert log_entry["level"] == "INFO"

def test_logger_returns_same_instance(temp_log_dir):
    logger1 = setup_logger(log_dir=str(temp_log_dir), logger_name="same_logger")
    logger2 = setup_logger(log_dir=str(temp_log_dir), logger_name="same_logger")

    # Both should be identical (singleton behavior)
    assert logger1 is logger2

def test_pipeline_context_filter_injects_fields():
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="testing filter",
        args=(),
        exc_info=None,
    )

    f = PipelineContextFilter("pipelineA", "tableX", "schemaZ")
    f.filter(record)

    assert record.pipeline_name == "pipelineA"
    assert record.target_table == "tableX"
    assert record.schema == "schemaZ"
