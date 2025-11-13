"""
Core orchestration module for the Data Lakehouse Ingest framework.
Executes config-driven ingestion from Bronze (raw) to Silver (curated) Delta tables.
Handles schema enforcement (SQL/LinkML), multi-format loading, and report generation.
"""

import json
import logging
from typing import Any
from datetime import datetime, timezone
from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql.utils import AnalysisException

from .utils.report_utils import generate_report
from .logger import safe_log_json
from .config_loader import ConfigLoader

# New modular helpers
from .orchestrator.init_utils import init_logger, init_run_context
from .orchestrator.table_processor import process_table
from .orchestrator.error_utils import error_entry_for_exception

from berdl_notebook_utils.setup_spark_session import get_spark_session


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
        - SparkSession must be provided by the caller.
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

    Attempts to create one via `get_spark_session()` from
    `berdl_notebook_utils.setup_spark_session`. If the module is unavailable
    or session creation fails, logs the error and returns a structured
    failure report under the "spark_initialization" phase.
    """
    if spark is None:
        try:
            logger.info("No SparkSession provided — initializing via get_spark_session()")
            spark = get_spark_session()
        except (ImportError, ModuleNotFoundError):
            # berdl_notebook_utils not available — fallback to explicit requirement
            error_msg = (
                "SparkSession must be provided by the caller. "
                "berdl_notebook_utils.setup_spark_session not found in this environment."
            )
            logger.error(error_msg)
            report = generate_report(
                success=False,
                started_at=started_at,
                tables=[],
                errors=[{"phase": "spark_initialization", "error": error_msg}],
            )
            safe_log_json(logger, report)
            return report
        except Exception as e:
            # unexpected failure inside get_spark_session()
            error_msg = f"Failed to initialize Spark session via get_spark_session(): {e}"
            logger.error(error_msg, exc_info=True)
            report = generate_report(
                success=False,
                started_at=started_at,
                tables=[],
                errors=[{"phase": "spark_initialization", "error": str(e)}],
            )
            safe_log_json(logger, report)
            return report


    # --- Config Loader ---
    try:
        loader = ConfigLoader(config, logger=logger, minio_client=minio_client)
    except Exception as e:
        logger.error(f"Failed to load or validate configuration: {e}", exc_info=True)
        report = generate_report(
            success=False, started_at=started_at, tables=[],
            errors=[{"phase": "config_validation", "error": str(e)}]
        )
        logger.info("Ingestion terminated during config validation")
        safe_log_json(logger, report)
        return report

    # --- Init run context (tenant, defaults, tables, DB) ---
    ctx = init_run_context(spark, logger, loader)
    tables = ctx["tables"]

    table_reports: list[dict[str, Any]] = []
    error_list: list[dict[str, Any]] = []

    # --- Table-level processing ---
    for table in tables:
        table_name = table.get("name", "pipeline_stage")

        # set dynamic table context for logger
        if hasattr(logger, "context_filter"):
            logger.context_filter.set_table(table_name)

        logger.info(f"Processing table: {table_name}")
        try:
            report_row = process_table(
                spark=spark,
                logger=logger,
                loader=loader,
                ctx=ctx,
                table=table,
                run_started_at_iso=started_at,
                minio_client=minio_client,
            )
            table_reports.append(report_row)
        except Exception as e:
            entry = error_entry_for_exception(table, e)
            table_reports.append(entry)
            error_list.append(entry)

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
