"""
Core orchestration module for the Data Lakehouse Ingest framework.
Executes config-driven ingestion from Bronze (raw) to Silver (curated) Delta tables.
Handles schema enforcement (SQL string or structured list-of-maps), multi-format loading,
optional DataFrame overrides, and report generation.
"""

import logging
from typing import Any
from datetime import datetime, timezone
from minio import Minio
from pyspark.sql import SparkSession, DataFrame

from .utils.report_utils import generate_report
from .logger import safe_log_json
from .config_loader import ConfigLoader
from .orchestrator.models import ProcessStatus

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
    dataframes: dict[str, DataFrame] | None = None,
) -> dict[str, Any]:
    """
    Orchestrates the end-to-end data ingestion process defined by a configuration.

    Loads raw (Bronze) data from local or S3/MinIO sources, applies schema enforcement
    (using SQL strings or structured list-of-maps schemas), and writes curated (Silver) Delta tables.
    Generates a structured report summarizing table-level outcomes and errors.

    Args:
        config (str | dict[str, Any]): Path to the config file (local or s3a://) or an inline config dictionary.
        spark (SparkSession, optional): Active Spark session used for reading and writing data.
        logger (logging.Logger, optional): Logger instance for structured logging.
        minio_client (Minio, optional): MinIO client used to read configuration or data from S3-compatible sources.
        dataframes (dict[str, DataFrame], optional): Optional in-memory DataFrame overrides keyed by table name.
            When provided, keys must match table names defined in the configuration.

    Returns:
        dict[str, Any]:
            A structured ingestion report containing overall success status,
            table-level metrics, and captured errors. Enum values are serialized
            when the report is logged or exported using the pipeline JSON encoder.

    Notes:
        - SparkSession may be provided by the caller or auto-initialized.
        - A valid MinIO client is REQUIRED for ingestion.
          If `minio_client` is not provided, `get_minio_client()` is attempted.
          If MinIO cannot be initialized, the pipeline fails immediately.
        - Supports multiple file formats (CSV, TSV, JSON, XML, Parquet).
        - Schema enforcement supports SQL-style schemas (`schema_sql`) and structured schemas (`schema` list-of-maps).
        - Each table in the configuration is processed independently.
        - Table processing results include enum-based status and input metadata.
        - If `dataframes` is provided, it is validated as dict[str, DataFrame] and keys must match configured tables.
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
        logger.info(
            "No MinIO client provided — attempting auto-initialization via get_minio_client()"
        )
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
        error_msg = "MinIO client is required for ingestion but was not provided or initialized."
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

    # ----------------------------------------------------------------------
    # Validate DataFrame overrides (if provided)
    # ----------------------------------------------------------------------
    """
    Validate optional DataFrame overrides.

    Ensures `dataframes` is a dict[str, DataFrame] and that all provided
    table keys exist in the ingestion configuration before processing begins.
    """
    if dataframes is not None:
        if not isinstance(dataframes, dict):
            return log_error(
                logger=logger,
                error_msg=(
                    "Invalid 'dataframes' argument. "
                    "Expected dict[str, pyspark.sql.DataFrame], "
                    f"got {type(dataframes).__name__}."
                ),
                phase="dataframe_validation",
                started_at=started_at,
            )

        # Validate keys and values (accumulate errors)
        df_errors: list[str] = []

        for key, value in dataframes.items():
            if not isinstance(key, str):
                df_errors.append(
                    f"Invalid key in 'dataframes': expected str table name, got {type(key).__name__} ({key!r})."
                )
                # If key isn't str, avoid using it in other messages safely
                continue

            if not isinstance(value, DataFrame):
                df_errors.append(
                    f"Invalid value for table '{key}' in 'dataframes': expected pyspark.sql.DataFrame, got {type(value).__name__}."
                )

        if df_errors:
            return log_error(
                logger=logger,
                error_msg="DataFrame override validation failed:\n- " + "\n- ".join(df_errors),
                phase="dataframe_validation",
                started_at=started_at,
            )

        # validate that provided table names exist in config
        config_table_names = {t["name"] for t in ctx["tables"]}
        invalid_keys = {k for k in dataframes.keys() if isinstance(k, str)} - config_table_names

        if invalid_keys:
            return log_error(
                logger=logger,
                error_msg=(
                    "DataFrame override provided for unknown table(s): "
                    f"{sorted(invalid_keys)}. "
                    f"Valid tables: {sorted(config_table_names)}."
                ),
                phase="dataframe_validation",
                started_at=started_at,
            )

    # --- Table-level processing ---
    table_reports, error_list = process_tables(
        spark=spark,
        logger=logger,
        loader=loader,
        ctx=ctx,
        started_at=started_at,
        minio_client=minio_client,
        dataframes=dataframes,
    )

    # --- Final report ---
    report = generate_report(
        success=all(t.get("status") == ProcessStatus.SUCCESS for t in table_reports),
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
    exc: Exception | None = None,
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
            (e.g., "spark_initialization", "config_validation", "dataframe_validation", "table_processing").
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
            - a single entry in `errors` describing the failed phase and message.
              If an exception is provided, the traceback is included in the logs.

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
