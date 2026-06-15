"""
Structured logging for Data Lakehouse Ingest pipelines.

Writes JSONL logs locally and optionally uploads compressed telemetry logs
to MinIO after ingest processing completes.
"""

import getpass
import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
import requests
import uuid

import boto3
import zstandard as zstd

from .utils.json_encoder import PipelineJSONEncoder


_logger_instance = None


class PipelineContextFilter(logging.Filter):
    """
    Injects ingestion pipeline context into every log record.

    This filter enriches log events with standardized telemetry fields
    used throughout the ingest framework, including pipeline metadata,
    user identity, schema, table context, timestamps, and event
    categorization.

    The filter maintains the current table context via `set_table()`
    so that table-level operations automatically inherit the correct
    target table information.

    Attributes:
        pipeline_name (str):
            Logical pipeline name associated with the ingestion run.
        schema (str):
            Target schema or dataset name.
        target_table (str):
            Current table being processed.
        user (str):
            User executing the ingestion run.
        tenant (str):
            Tenant or namespace associated with the run.
        catalog (str):
            Catalog used when constructing fully-qualified table names.
        pipeline_run_id (str):
            Unique identifier for the ingestion run.
    """

    NON_TABLE_CONTEXTS = {
        "pending_config_load",
        "pipeline_stage",
    }

    def __init__(
        self,
        pipeline_name: str,
        schema: str,
        pipeline_run_id: str,
        user: str | None = None,
        tenant: str | None = None,
    ):
        super().__init__()
        self.pipeline_name = pipeline_name
        self.schema = schema
        self.target_table = "pending_config_load"
        self.user = user or os.getenv("USER") or getpass.getuser()
        self.tenant = tenant or os.getenv("TENANT", "pending_config_load")
        self.catalog = tenant if tenant else "my"
        self.pipeline_run_id = pipeline_run_id

    def set_table(self, table: str):
        """
        Update the current table context.

        Sets the active table name that will be attached to all
        subsequently generated log records until another table
        is assigned.

        Args:
            table:
                Target table currently being processed.
        """
        self.target_table = table

    def filter(self, record):
        """
        Enrich a log record with pipeline telemetry metadata.

        Adds standardized ingestion telemetry fields including
        timestamps, pipeline identifiers, user information,
        table context, and event categorization before the
        record is processed by formatters.

        Args:
            record:
                LogRecord instance being processed.

        Returns:
            bool:
                Always True to allow the record to be emitted.
        """
        now = datetime.now(timezone.utc)

        record.source_system = "ingest_lib"
        record.event_type = getattr(record, "event_type", "ingest_run_event")

        record.event_category = getattr(
            record,
            "event_category",
            "table" if self.target_table != "pending_config_load" else "pipeline",
        )
        record.event_date = now.date().isoformat()
        record.event_timestamp = now.isoformat().replace("+00:00", "Z")
        record.processing_timestamp = now.isoformat().replace("+00:00", "Z")

        record.user = self.user
        record.tenant = self.tenant
        record.pipeline_name = self.pipeline_name
        record.pipeline_run_id = getattr(record, "pipeline_run_id", self.pipeline_run_id)
        record.target_schema = self.schema
        table_name = getattr(record, "table", self.target_table)

        record.table = table_name

        record.target_table = getattr(
            record,
            "target_table",
            (
                table_name
                if table_name in self.NON_TABLE_CONTEXTS
                else f"{self.catalog}.{self.schema}.{table_name}"
            ),
        )

        return True


class JsonLineFormatter(logging.Formatter):
    """
    Formats log records as structured JSONL events.

    Produces one JSON document per log entry suitable for ingestion
    into VictoriaLogs and other observability platforms. The formatter
    serializes all standard pipeline telemetry fields, supports
    exception capture, and optionally embeds structured run summaries.

    Status values are derived from log severity:

    - SUCCESS for INFO and DEBUG
    - WARNING for WARNING
    - FAILED for ERROR, CRITICAL, and exception events
    """

    def format(self, record):
        """
        Convert a log record into a structured JSON document.

        Args:
            record:
                LogRecord instance to serialize.

        Returns:
            str:
                JSON representation of the log event suitable
                for JSONL storage and VictoriaLogs ingestion.
        """
        payload = {
            "source_system": record.source_system,
            "event_type": getattr(record, "event_type", "ingest_run_event"),
            "event_category": record.event_category,
            "event_date": record.event_date,
            "event_timestamp": record.event_timestamp,
            "processing_timestamp": record.processing_timestamp,
            "user": record.user,
            "tenant": record.tenant,
            "pipeline_name": record.pipeline_name,
            "pipeline_run_id": record.pipeline_run_id,
            "target_schema": record.target_schema,
            "table": record.table,
            "target_table": record.target_table,
            "level": record.levelname,
            "module": record.module,
            "message": record.getMessage(),
        }

        if hasattr(record, "summary"):
            payload["summary"] = record.summary

        if record.exc_info:
            payload["exception_type"] = record.exc_info[0].__name__
            payload["exception"] = self.formatException(record.exc_info)
            payload["status"] = "FAILED"
        elif record.levelname in {"ERROR", "CRITICAL"}:
            payload["status"] = "FAILED"
        elif record.levelname == "WARNING":
            payload["status"] = "WARNING"
        else:
            payload["status"] = "SUCCESS"

        return json.dumps(payload, cls=PipelineJSONEncoder, ensure_ascii=False)


class NotebookFormatter(logging.Formatter):
    """
    Formats log records for human-readable notebook output.

    Unlike JsonLineFormatter, this formatter prioritizes readability
    for interactive users by displaying concise log messages with
    timestamps, severity levels, and target table information.

    Pipeline summary events are rendered as pretty-printed JSON reports
    to provide a detailed end-of-run summary while keeping routine
    events compact.
    """

    def format(self, record):
        """
        Convert a log record into a human-readable notebook message.

        Args:
            record:
                LogRecord instance to render.

        Returns:
            str:
                Formatted text optimized for interactive notebook
                output.
        """
        event_time = getattr(record, "event_timestamp", "")[:19].replace("T", " ")
        target_table = getattr(record, "target_table", "")
        message = record.getMessage()

        if hasattr(record, "summary"):
            pretty_summary = json.dumps(
                record.summary,
                indent=2,
                cls=PipelineJSONEncoder,
                ensure_ascii=False,
            )

            return f"{event_time} | {record.levelname} | {message}\n{pretty_summary}"

        if target_table and target_table not in {
            "pending_config_load",
            "pipeline_stage",
        }:
            return f"{event_time} | {record.levelname} | {target_table} | {record.getMessage()}"

        return f"{event_time} | {record.levelname} | {record.getMessage()}"


def setup_logger(
    log_dir: str | Path = Path("local_logs"),
    logger_name: str = "pipeline_logger",
    pipeline_name: str = "pending_config_load",
    schema: str = "pending_config_load",
    log_level: str | None = None,
    user: str | None = None,
    tenant: str | None = None,
) -> logging.Logger:
    """
    Set up and return a structured logger for ingestion telemetry.

    Creates a singleton logger configured with both file and console
    handlers. The file handler writes structured JSONL telemetry events
    suitable for VictoriaLogs ingestion, while the console handler emits
    concise human-readable output optimized for notebooks and interactive
    development.

    Args:
        log_dir (str | Path):
            Directory where local JSONL telemetry logs are written.
        logger_name (str):
            Name of the logger instance.
        pipeline_name (str):
            Logical pipeline name associated with the ingestion run.
        schema (str):
            Target schema or dataset name.
        log_level (str | None):
            Optional logging level ("DEBUG", "INFO", "WARNING",
            "ERROR", "CRITICAL"). If not provided, the
            PIPELINE_LOG_LEVEL environment variable is used.
            Defaults to "DEBUG".
        user (str | None):
            User executing the ingestion pipeline. If omitted,
            the current operating-system user is used.
        tenant (str | None):
            Tenant or namespace associated with the ingestion run.

    Returns:
        logging.Logger:
            Configured singleton logger instance containing:

            - Structured JSONL file handler
            - Human-readable notebook console handler
            - Pipeline context filter
            - Pipeline run identifier
            - Log file path metadata

    Notes:
        - If the logger has already been initialized, the existing
          instance is returned.
        - Each ingestion run receives a unique pipeline_run_id,
          either from PIPELINE_RUN_ID or an auto-generated UUID.
        - File output is formatted using JsonLineFormatter and is
          intended for telemetry ingestion and audit purposes.
        - Console output is formatted using NotebookFormatter and
          is intended for interactive users.
        - All log records are automatically enriched with:
            - source_system
            - event_type
            - event_category
            - tenant
            - user
            - pipeline_name
            - pipeline_run_id
            - target_schema
            - table
            - target_table
            - timestamps
        - The generated JSONL file can be uploaded directly to
          VictoriaLogs via upload_log_file_to_victorialogs().
    """
    global _logger_instance

    if _logger_instance:
        return _logger_instance

    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_file = log_dir / f"pipeline_run_{timestamp}.jsonl"

    logger = logging.getLogger(logger_name)

    effective_log_level = (log_level or os.getenv("PIPELINE_LOG_LEVEL", "DEBUG")).upper()
    logger.setLevel(getattr(logging, effective_log_level, logging.DEBUG))
    logger.propagate = False

    if logger.hasHandlers():
        for handler in logger.handlers[:]:
            handler.close()
            logger.removeHandler(handler)

    json_formatter = JsonLineFormatter()
    notebook_formatter = NotebookFormatter()

    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setFormatter(json_formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(notebook_formatter)
    logger.addHandler(console_handler)

    pipeline_run_id = os.getenv("PIPELINE_RUN_ID") or str(uuid.uuid4())

    context_filter = PipelineContextFilter(
        pipeline_name=pipeline_name,
        schema=schema,
        pipeline_run_id=pipeline_run_id,
        user=user,
        tenant=tenant,
    )
    logger.addFilter(context_filter)

    logger.context_filter = context_filter
    logger.log_file_path = str(log_file)

    _logger_instance = logger
    return logger


def compress_log_file(log_file_path: str | Path) -> Path:
    """
    Compress a telemetry log file using Zstandard.

    Args:
        log_file_path:
            Path to the JSONL log file.

    Returns:
        Path:
            Path to the compressed .zstd file.

    Notes:
        Compression is performed using Zstandard level 3, which
        provides a good balance between compression ratio and speed
        for telemetry workloads.
    """
    log_file_path = Path(log_file_path)
    compressed_path = log_file_path.with_suffix(log_file_path.suffix + ".zstd")

    compressor = zstd.ZstdCompressor(level=3)

    with open(log_file_path, "rb") as source, open(compressed_path, "wb") as target:
        compressor.copy_stream(source, target)

    return compressed_path


def build_ingest_telemetry_key(
    compressed_file_path: str | Path,
    user: str,
    pipeline_name: str,
    prefix: str | None = None,
) -> str:
    """
    Build the MinIO object key for an ingest telemetry log.

    Generates a partitioned object path organized by user, date,
    and pipeline name to simplify telemetry discovery and replay.

    Returns:
        str:
            Fully qualified object key suitable for MinIO upload.

    Example:
        ingest-job-logs/amkhan/date=2026-06-05/
        personal_ontology_source_66_ingest/
        pipeline_run_20260605T153000Z.jsonl.zstd
    """
    prefix = (prefix or os.getenv("INGEST_TELEMETRY_PREFIX", "ingest-job-logs")).strip("/")
    event_date = datetime.now(timezone.utc).date().isoformat()
    filename = Path(compressed_file_path).name

    return f"{prefix}/{user}/date={event_date}/{pipeline_name}/{filename}"


def upload_log_file_to_minio(
    logger: logging.Logger,
    compressed_file_path: str | Path,
    object_key: str,
) -> bool:
    """
    Upload a compressed telemetry log file to MinIO.

    Args:
        logger:
            Structured logger instance.
        compressed_file_path:
            Path to the compressed log file.
        object_key:
            Destination object key within the target bucket.

    Returns:
        bool:
            True if upload succeeds, otherwise False.

    Notes:
        Upload failures are logged but do not interrupt pipeline
        execution.
    """
    bucket = os.getenv("INGEST_TELEMETRY_BUCKET", "cdm-ingest-job-logs")
    endpoint_url = os.getenv("S3_ENDPOINT_URL")
    access_key = os.getenv("S3_ACCESS_KEY")
    secret_key = os.getenv("S3_SECRET_KEY")

    if not endpoint_url or not access_key or not secret_key:
        logger.warning(
            "MinIO upload skipped because S3_ENDPOINT_URL, S3_ACCESS_KEY, or S3_SECRET_KEY is missing"
        )
        return False

    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )

        s3_client.upload_file(
            Filename=str(compressed_file_path),
            Bucket=bucket,
            Key=object_key,
        )

        logger.info(f"Uploaded ingest telemetry log to s3://{bucket}/{object_key}")
        return True

    except Exception:
        logger.exception("Failed to upload ingest telemetry log to MinIO")
        return False


def upload_log_file_to_victorialogs(
    logger: logging.Logger,
    log_file_path: str | Path,
) -> bool:
    """
    Upload structured telemetry events directly to VictoriaLogs.

    Reads the generated JSONL log file and sends all events to the
    VictoriaLogs JSON ingestion endpoint.

    Args:
        logger:
            Structured logger instance.
        log_file_path:
            Path to the JSONL telemetry file.

    Returns:
        bool:
            True if ingestion succeeds, otherwise False.

    Notes:
        VictoriaLogs uses event_timestamp as the event time field and
        message as the display message field.
    """
    victorialogs_url = os.getenv(
        "VICTORIALOGS_URL",
        "http://victorialogs:9428",
    ).rstrip("/")

    ingest_url = (
        f"{victorialogs_url}/insert/jsonline"
        "?_time_field=event_timestamp"
        "&_msg_field=message"
        "&_stream_fields=source_system,tenant,user,pipeline_name,pipeline_run_id,target_schema,table,level,status"
    )

    try:
        with open(log_file_path, "rb") as f:
            response = requests.post(
                ingest_url,
                data=f,
                headers={"Content-Type": "application/stream+json"},
                timeout=30,
            )

        response.raise_for_status()
        logger.info(
            f"Uploaded ingest telemetry log to VictoriaLogs: {victorialogs_url}",
            extra={
                "event_category": "pipeline",
                "table": "pipeline_stage",
                "target_table": "pipeline_stage",
            },
        )
        return True

    except Exception:
        logger.exception(
            "Failed to upload ingest telemetry log to VictoriaLogs",
            extra={
                "event_category": "pipeline",
                "table": "pipeline_stage",
                "target_table": "pipeline_stage",
            },
        )
        return False


def upload_log_file_to_telemetry_uploader(
    logger: logging.Logger,
    compressed_file_path: str | Path,
    object_key: str,
) -> bool:
    """
    Upload a compressed telemetry log file through the telemetry uploader service.
    """
    upload_url = os.getenv("INGEST_TELEMETRY_UPLOAD_URL")
    telemetry_token = os.getenv("TELEMETRY_TOKEN")

    if not upload_url:
        logger.warning(
            "Telemetry upload skipped because INGEST_TELEMETRY_UPLOAD_URL is missing"
        )
        return False

    try:
        with open(compressed_file_path, "rb") as f:
            response = requests.post(
                upload_url,
                headers={
                    "X-Telemetry-Token": telemetry_token,
                },
                data={
                    "object_key": object_key,
                },
                files={
                    "file": (
                        Path(compressed_file_path).name,
                        f,
                        "application/zstd",
                    )
                },
                timeout=60,
            )

        response.raise_for_status()

        logger.info(
            f"Uploaded ingest telemetry log via telemetry uploader: {object_key}"
        )
        return True

    except Exception:
        logger.exception(
            "Failed to upload ingest telemetry log via telemetry uploader"
        )
        return False


def finalize_logger(logger: logging.Logger) -> None:
    """
    Finalize ingestion telemetry and upload logs to VictoriaLogs.

    Flushes all logger handlers to ensure that the JSONL telemetry file
    contains the complete set of events generated during the ingestion
    run, including the final pipeline summary event. If telemetry is
    enabled, the completed JSONL file is uploaded directly to the
    configured VictoriaLogs instance.

    Args:
        logger:
            Structured logger instance created by setup_logger().

    Notes:
        - All handlers are flushed before upload to ensure no log
          records remain buffered.
        - Upload is skipped when INGEST_TELEMETRY_ENABLED is set
          to false.
        - Telemetry upload failures are logged but do not interrupt
          the ingestion workflow.
        - This function should typically be called once at the end
          of an ingestion run from a finally block.
    """
    telemetry_enabled = os.getenv("INGEST_TELEMETRY_ENABLED", "true").lower() == "true"

    for handler in logger.handlers:
        handler.flush()

    if not telemetry_enabled:
        logger.info("Ingest telemetry upload is disabled")
        return

    try:
        log_file_path = Path(logger.log_file_path)

        compressed_file_path = compress_log_file(log_file_path)

        object_key = build_ingest_telemetry_key(
            compressed_file_path=compressed_file_path,
            user=logger.context_filter.user,
            pipeline_name=logger.context_filter.pipeline_name,
        )

        # Temporarily disable telemetry uploads while
        # telemetry-uploader integration is being finalized.
        #
        # upload_log_file_to_telemetry_uploader(
        #     logger=logger,
        #     compressed_file_path=compressed_file_path,
        #     object_key=object_key,
        # )

    except Exception:
        logger.exception("Failed during ingest telemetry finalization")


def safe_log_json(logger: logging.Logger, data: dict) -> None:
    """
    Emit a structured pipeline run summary event.

    Creates a dedicated telemetry event containing overall pipeline
    execution metrics and the full ingestion report. The summary is
    stored as structured JSON for downstream querying and analytics
    while also producing a human-readable summary message.

    Args:
        logger:
            Structured logger instance.
        data:
            Ingestion report generated by generate_report().

    Notes:
        The emitted event uses:
        - event_type = ingest_run_summary
        - event_category = pipeline_summary

        The full report is attached to the log record under the
        'summary' field and is available in VictoriaLogs as
        structured telemetry.
    """
    try:
        logger.info(
            f"Pipeline run summary: success={data.get('success')}, "
            f"duration_sec={data.get('duration_sec')}, "
            f"tables={len(data.get('tables', []))}, "
            f"errors={len(data.get('errors', []))}",
            extra={
                "event_type": "ingest_run_summary",
                "event_category": "pipeline_summary",
                "table": "pipeline_stage",
                "target_table": "pipeline_stage",
                "summary": data,
            },
        )
    except Exception:
        logger.info(str(data))
