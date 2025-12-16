"""
Core orchestration module for the Data Lakehouse Ingest framework.
Executes config-driven ingestion from Bronze (raw) to Silver (curated) Delta tables.
Handles schema enforcement (SQL/LinkML), multi-format loading, and report generation.
"""

import logging
from typing import Any
from datetime import datetime, timezone
from minio import Minio
from pyspark.sql import SparkSession

from .utils.report_utils import generate_report
from .logger import safe_log_json
from .config_loader import ConfigLoader

# New modular helpers
from .orchestrator.init_utils import init_logger, init_run_context
from .orchestrator.table_batch_processor import process_tables

from berdl_notebook_utils.setup_spark_session import get_spark_session
from berdl_notebook_utils.clients import get_minio_client


def ingest(
    config: str | dict[str, Any],
    spark: SparkSession | None = None,
    logger: logging.Logger | None = None,
    minio_client: Minio | None = None,
) -> dict[str, Any]:
    """
    Orchestrates the end-to-end data ingestion process defined by a configuration.

    Loads raw (Bronze) data from local or S3/MinIO sources, applies schema enforcement
    (SQL or LinkML-based), and writes curated (Silver) Delta tables. Generates a
    structured report summarizing table-level outcomes and errors.

    Args:
        config (str | dict[str, Any]): Path to the config file (local or s3a://) or an inline config dictionary.
        spark (SparkSession, optional): Active Spark session used for reading and writing data.
        logger (logging.Logger, optional): Logger instance for structured logging.
        minio_client (Minio, optional): MinIO client used to read configuration or data from S3-compatible sources.

    Returns:
        dict[str, Any]: A structured ingestion report containing status, errors, and table-level metrics.

    Notes:
        - SparkSession may be provided by the caller or auto-initialized.
        - A valid MinIO client is REQUIRED for ingestion.
          If `minio_client` is not provided, `get_minio_client()` is attempted.
          If MinIO cannot be initialized, the pipeline fails immediately.
        - Supports multiple file formats (CSV, TSV, JSON, XML).
        - Each table in the configuration is processed independently.
    """
    started_at = datetime.now(timezone.utc).isoformat()

    # --- Logger ---
    """
    Initialize or reuse a structured logger to ensure consistent JSON-formatted logs
    across the entire ingestion run.
    """
    logger = init_logger(logger)

    # ----------------------------------------------------------------------
    # Spark Session Initialization
    # ----------------------------------------------------------------------
    """
    Initialize a SparkSession if not provided by the caller.

    Since `berdl_notebook_utils` is an explicit project dependency, this function
    uses `get_spark_session()` from `berdl_notebook_utils.setup_spark_session`
    to construct a properly configured Spark session. If Spark initialization fails,
    the error is logged and a structured failure report is returned.
    """
    if spark is None:
        logger.info("No SparkSession provided — initializing via get_spark_session()")
        try:
            spark = get_spark_session()
        except Exception as e:
            error_msg = f"Failed to initialize Spark session via get_spark_session(): {e}"
            return log_error(
                logger=logger,
                error_msg=error_msg,
                phase="spark_initialization",
                started_at=started_at,
                exc=e,
            )

    # ----------------------------------------------------------------------
    # MinIO Client Initialization
    # ----------------------------------------------------------------------
    if minio_client is None:
        logger.info("No MinIO client provided — attempting auto-initialization via get_minio_client()")
        try:
            minio_client = get_minio_client()
            logger.info("MinIO client successfully initialized via get_minio_client()")
        except Exception as e:
            error_msg = (
                "MinIO client is required for ingestion but could not be initialized. "
                "Call get_minio_client() and pass it explicitly into ingest(...)."
            )
            return log_error(
                logger=logger,
                error_msg=error_msg,
                phase="minio_initialization",
                started_at=started_at,
                exc=e,
            )

    # Defensive check in case get_minio_client() returned None without raising
    if minio_client is None:
        error_msg = (
            "MinIO client is required for ingestion but was not provided or initialized."
        )
        return log_error(
            logger=logger,
            error_msg=error_msg,
            phase="minio_initialization",
            started_at=started_at,
            exc=None,
        )

    # --- Config Loader ---
    try:
        loader = ConfigLoader(config, logger=logger, minio_client=minio_client)
    except Exception as e:
        error_msg = f"Failed to load or validate configuration: {e}"
        logger.info("Ingestion terminated during config validation")
        return log_error(
            logger=logger,
            error_msg=error_msg,
            phase="config_validation",
            started_at=started_at,
            exc=e,
        )

    # --- Init run context (tenant, defaults, tables, DB) ---
    ctx = init_run_context(spark, logger, loader)

    # --- Table-level processing ---
    table_reports, error_list = process_tables(
        spark=spark,
        logger=logger,
        loader=loader,
        ctx=ctx,
        started_at=started_at,
        minio_client=minio_client,
    )

    # --- Final report ---
    report = generate_report(
        success=all(t.get("status") == "success" for t in table_reports),
        started_at=started_at,
        tables=table_reports,
        errors=error_list,
    )

    logger.info("Ingestion complete")
    safe_log_json(logger, report)
    return report

def log_error(
    logger: logging.Logger,
    error_msg: str,
    phase: str,
    started_at: str,
    exc: Exception | None = None
) -> dict[str, Any]:
    """
    Log an ingestion error and generate a standardized failure report.

    This helper ensures that all fatal or early-exit failures during an
    ingestion run are logged in a consistent JSON-structured format. It logs
    the provided error message, optionally includes full traceback details
    when an exception is passed, and returns a minimal ingestion report
    describing the failure.

    Args:
        logger (logging.Logger):
            The structured logger used for emitting JSON-formatted log entries.
        error_msg (str):
            Human-readable description of the failure condition.
        phase (str):
            The ingestion phase where the failure occurred
            (e.g., "spark_initialization", "config_validation", "table_processing").
        started_at (str):
            ISO-8601 timestamp marking when the ingestion run began.
        exc (Exception | None, optional):
            The underlying exception that triggered the failure. When provided,
            the full traceback is logged via `exc_info=True`. Defaults to None.

    Returns:
        dict[str, Any]:
            A standardized ingestion failure report containing:
            - `success=False`
            - `started_at` timestamp
            - empty `tables` list
            - a single entry in `errors` describing the failed phase and message

    Notes:
        - This function is used for early termination paths (e.g., Spark setup,
          config validation) where table-level work has not begun.
        - The returned report is always safe to serialize and is also logged via
          `safe_log_json()` for downstream pipeline auditing.
    """
    if exc is not None:
        logger.error(error_msg, exc_info=True)
    else:
        logger.error(error_msg)

    report = generate_report(
        success=False,
        started_at=started_at,
        tables=[],
        errors=[{"phase": phase, "error": error_msg}],
    )

    safe_log_json(logger, report)
    return report

