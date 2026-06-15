import json
import logging
import os
import sys
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


def test_compress_log_file_creates_zstd_file(tmp_path):
    """Verify compress_log_file() creates a compressed .zstd file."""
    log_file = tmp_path / "test.jsonl"
    log_file.write_text('{"message": "hello"}\n')

    compressed_file = logger_module.compress_log_file(log_file)

    assert compressed_file.exists()
    assert compressed_file.name.endswith(".jsonl.zstd")
    assert compressed_file.stat().st_size > 0


def test_build_ingest_telemetry_key(monkeypatch, tmp_path):
    """Verify build_ingest_telemetry_key() creates the expected partitioned object key."""
    monkeypatch.setenv("INGEST_TELEMETRY_PREFIX", "ingest-job-logs")

    compressed_file = tmp_path / "pipeline_run_20260615T120000Z.jsonl.zstd"

    object_key = logger_module.build_ingest_telemetry_key(
        compressed_file_path=compressed_file,
        user="amkhan",
        pipeline_name="test_pipeline",
    )

    assert object_key.startswith("ingest-job-logs/amkhan/date=")
    assert object_key.endswith("test_pipeline/pipeline_run_20260615T120000Z.jsonl.zstd")


def test_upload_log_file_to_minio_skips_when_env_missing(tmp_path, caplog, monkeypatch):
    """Verify upload_log_file_to_minio() skips upload when required S3 env vars are missing."""
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    monkeypatch.delenv("S3_ACCESS_KEY", raising=False)
    monkeypatch.delenv("S3_SECRET_KEY", raising=False)

    logger = logging.getLogger("test_minio_skip")
    caplog.set_level(logging.WARNING)

    result = logger_module.upload_log_file_to_minio(
        logger=logger,
        compressed_file_path=tmp_path / "missing.zstd",
        object_key="ingest-job-logs/test.zstd",
    )

    assert result is False
    assert "MinIO upload skipped" in caplog.text


def test_upload_log_file_to_telemetry_uploader_skips_when_url_missing(
    tmp_path, caplog, monkeypatch
):
    """Verify telemetry uploader upload is skipped when INGEST_TELEMETRY_UPLOAD_URL is missing."""
    monkeypatch.delenv("INGEST_TELEMETRY_UPLOAD_URL", raising=False)

    logger = logging.getLogger("test_uploader_skip")
    caplog.set_level(logging.WARNING)

    result = logger_module.upload_log_file_to_telemetry_uploader(
        logger=logger,
        compressed_file_path=tmp_path / "missing.zstd",
        object_key="ingest-job-logs/test.zstd",
    )

    assert result is False
    assert "INGEST_TELEMETRY_UPLOAD_URL is missing" in caplog.text


def test_finalize_logger_flushes_handlers_when_upload_temporarily_disabled(tmp_path):
    """Verify finalize_logger() flushes handlers without attempting telemetry upload."""
    logger = logger_module.setup_logger(
        log_dir=str(tmp_path),
        logger_name="finalize_logger",
    )

    logger.info("before finalize")

    logger_module.finalize_logger(logger)

    assert os.path.exists(logger.log_file_path)


def test_notebook_formatter_formats_summary_record():
    """Verify NotebookFormatter renders summary events as pretty JSON."""
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="summary message",
        args=(),
        exc_info=None,
    )

    f = logger_module.PipelineContextFilter(
        "pipelineA",
        "schemaZ",
        pipeline_run_id="run-123",
    )
    f.filter(record)
    record.summary = {"success": True}

    output = logger_module.NotebookFormatter().format(record)

    assert "summary message" in output
    assert '"success": true' in output


def test_notebook_formatter_formats_table_context():
    """Verify NotebookFormatter includes target table when table context is available."""
    record = logging.LogRecord(
        name="test",
        level=logging.INFO,
        pathname=__file__,
        lineno=10,
        msg="table message",
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

    output = logger_module.NotebookFormatter().format(record)

    assert "berdl.schemaZ.tableX" in output
    assert "table message" in output


def test_upload_log_file_to_minio_logs_failure_when_upload_fails(tmp_path, caplog, monkeypatch):
    """Verify upload_log_file_to_minio() logs and returns False when boto3 upload fails."""
    compressed_file = tmp_path / "test.zstd"
    compressed_file.write_bytes(b"test")

    monkeypatch.setenv("INGEST_TELEMETRY_BUCKET", "test-bucket")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_ACCESS_KEY", "test-access")
    monkeypatch.setenv("S3_SECRET_KEY", "test-secret")

    class FakeS3Client:
        def upload_file(self, Filename, Bucket, Key):
            raise RuntimeError("upload failed")

    monkeypatch.setattr(
        logger_module.boto3,
        "client",
        lambda *args, **kwargs: FakeS3Client(),
    )

    logger = logging.getLogger("test_minio_failure")
    caplog.set_level(logging.ERROR)

    result = logger_module.upload_log_file_to_minio(
        logger=logger,
        compressed_file_path=compressed_file,
        object_key="ingest-job-logs/test.zstd",
    )

    assert result is False
    assert "Failed to upload ingest telemetry log to MinIO" in caplog.text


def test_upload_log_file_to_telemetry_uploader_logs_failure(tmp_path, caplog, monkeypatch):
    """Verify telemetry uploader upload returns False when HTTP upload fails."""
    compressed_file = tmp_path / "test.zstd"
    compressed_file.write_bytes(b"test")

    monkeypatch.setenv("INGEST_TELEMETRY_UPLOAD_URL", "http://telemetry-uploader:8080/upload")
    monkeypatch.setenv("TELEMETRY_TOKEN", "test-token")

    def fake_post(*args, **kwargs):
        raise RuntimeError("http failed")

    monkeypatch.setattr(logger_module.requests, "post", fake_post)

    logger = logging.getLogger("test_uploader_failure")
    caplog.set_level(logging.ERROR)

    result = logger_module.upload_log_file_to_telemetry_uploader(
        logger=logger,
        compressed_file_path=compressed_file,
        object_key="ingest-job-logs/test.zstd",
    )

    assert result is False
    assert "Failed to upload ingest telemetry log via telemetry uploader" in caplog.text


def test_upload_log_file_to_victorialogs_success(tmp_path, monkeypatch, caplog):
    """Verify VictoriaLogs upload returns True when the HTTP request succeeds."""

    log_file = tmp_path / "test.jsonl"
    log_file.write_text('{"message":"test"}\n')

    class FakeResponse:
        def raise_for_status(self):
            pass

    def fake_post(*args, **kwargs):
        return FakeResponse()

    monkeypatch.setattr(logger_module.requests, "post", fake_post)

    logger = logging.getLogger("victorialogs_success")

    result = logger_module.upload_log_file_to_victorialogs(
        logger=logger,
        log_file_path=log_file,
    )

    assert result is True


def test_upload_log_file_to_victorialogs_failure(tmp_path, monkeypatch, caplog):
    """Verify VictoriaLogs upload returns False when the HTTP request fails."""

    log_file = tmp_path / "test.jsonl"
    log_file.write_text('{"message":"test"}\n')

    def fake_post(*args, **kwargs):
        raise RuntimeError("VictoriaLogs unavailable")

    monkeypatch.setattr(logger_module.requests, "post", fake_post)

    logger = logging.getLogger("victorialogs_failure")
    caplog.set_level(logging.ERROR)

    result = logger_module.upload_log_file_to_victorialogs(
        logger=logger,
        log_file_path=log_file,
    )

    assert result is False
    assert "Failed to upload ingest telemetry log to VictoriaLogs" in caplog.text


def test_json_formatter_includes_exception_fields():
    """Verify JsonLineFormatter includes exception details when exc_info is present."""
    try:
        raise ValueError("bad value")
    except ValueError:
        exc_info = sys.exc_info()

    record = logging.LogRecord(
        name="test",
        level=logging.ERROR,
        pathname=__file__,
        lineno=10,
        msg="exception test",
        args=(),
        exc_info=exc_info,
    )

    f = logger_module.PipelineContextFilter(
        "pipelineA",
        "schemaZ",
        pipeline_run_id="run-123",
    )
    f.filter(record)

    payload = json.loads(logger_module.JsonLineFormatter().format(record))

    assert payload["exception_type"] == "ValueError"
    assert "ValueError: bad value" in payload["exception"]
    assert payload["status"] == "FAILED"


def test_setup_logger_removes_existing_handlers(tmp_path):
    """Verify setup_logger() closes and removes pre-existing logger handlers."""
    test_logger = logging.getLogger("handler_cleanup_logger")

    existing_handler = logging.StreamHandler()
    test_logger.addHandler(existing_handler)

    logger = logger_module.setup_logger(
        log_dir=str(tmp_path),
        logger_name="handler_cleanup_logger",
    )

    assert existing_handler not in logger.handlers
    assert len(logger.handlers) == 2


def test_upload_log_file_to_minio_success(tmp_path, monkeypatch, caplog):
    """Verify upload_log_file_to_minio() returns True after successful S3 upload."""
    compressed_file = tmp_path / "test.zstd"
    compressed_file.write_bytes(b"test")

    monkeypatch.setenv("INGEST_TELEMETRY_BUCKET", "test-bucket")
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.setenv("S3_ACCESS_KEY", "test-access")
    monkeypatch.setenv("S3_SECRET_KEY", "test-secret")

    class FakeS3Client:
        def upload_file(self, Filename, Bucket, Key):
            assert Filename == str(compressed_file)
            assert Bucket == "test-bucket"
            assert Key == "ingest-job-logs/test.zstd"

    monkeypatch.setattr(
        logger_module.boto3,
        "client",
        lambda *args, **kwargs: FakeS3Client(),
    )

    logger = logging.getLogger("test_minio_success")
    caplog.set_level(logging.INFO)

    result = logger_module.upload_log_file_to_minio(
        logger=logger,
        compressed_file_path=compressed_file,
        object_key="ingest-job-logs/test.zstd",
    )

    assert result is True
    assert (
        "Uploaded ingest telemetry log to s3://test-bucket/ingest-job-logs/test.zstd" in caplog.text
    )


def test_upload_log_file_to_telemetry_uploader_success(tmp_path, monkeypatch, caplog):
    """Verify telemetry uploader upload returns True when HTTP upload succeeds."""
    compressed_file = tmp_path / "test.zstd"
    compressed_file.write_bytes(b"test")

    monkeypatch.setenv("INGEST_TELEMETRY_UPLOAD_URL", "http://telemetry-uploader:8080/upload")
    monkeypatch.setenv("TELEMETRY_TOKEN", "test-token")

    class FakeResponse:
        def raise_for_status(self):
            pass

    def fake_post(url, headers, data, files, timeout):
        assert url == "http://telemetry-uploader:8080/upload"
        assert headers["X-Telemetry-Token"] == "test-token"
        assert data["object_key"] == "ingest-job-logs/test.zstd"
        assert timeout == 60
        return FakeResponse()

    monkeypatch.setattr(logger_module.requests, "post", fake_post)

    logger = logging.getLogger("test_uploader_success")
    caplog.set_level(logging.INFO)

    result = logger_module.upload_log_file_to_telemetry_uploader(
        logger=logger,
        compressed_file_path=compressed_file,
        object_key="ingest-job-logs/test.zstd",
    )

    assert result is True
    assert "Uploaded ingest telemetry log via telemetry uploader" in caplog.text


def test_finalize_logger_logs_when_telemetry_disabled(tmp_path, monkeypatch):
    """Verify finalize_logger() logs and returns when telemetry is disabled."""
    monkeypatch.setenv("INGEST_TELEMETRY_ENABLED", "false")

    logger = logger_module.setup_logger(
        log_dir=str(tmp_path),
        logger_name="finalize_disabled_logger",
    )

    logger_module.finalize_logger(logger)

    for handler in logger.handlers:
        handler.flush()

    with open(logger.log_file_path, "r") as f:
        logs = [json.loads(line) for line in f if line.strip()]

    assert logs[-1]["message"] == "Ingest telemetry upload is disabled"


def test_safe_log_json_falls_back_to_string_when_summary_logging_fails(caplog):
    """Verify safe_log_json() logs string data when structured summary logging fails."""

    class FakeLogger:
        def __init__(self):
            self.calls = 0

        def info(self, message, *args, **kwargs):
            self.calls += 1
            if self.calls == 1:
                raise RuntimeError("structured logging failed")
            logging.getLogger("safe_log_fallback").info(message)

    caplog.set_level(logging.INFO)

    data = {"success": False, "errors": ["bad record"]}

    logger_module.safe_log_json(FakeLogger(), data)

    assert str(data) in caplog.text
