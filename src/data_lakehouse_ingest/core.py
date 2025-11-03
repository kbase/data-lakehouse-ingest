"""
File name: src/data_lakehouse_ingest/core.py

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

    # --- Spark Session ---
    if spark is None:
        error_msg = (
            "❌ SparkSession must be provided by the caller. "
            "Please start a Spark session and pass it as `spark=` argument."
        )
        logger_error = logger or logging.getLogger("data_lakehouse_ingest")
        logger_error.error(error_msg)

        report = generate_report(
            success=False, started_at=started_at, tables=[],
            errors=[{"phase": "spark_initialization", "error": error_msg}]
        )
        logger_error.info("🏁 Ingestion terminated during Spark session check")
        logger_error.info(json.dumps(report, indent=2))
        return report

    # --- Logger ---
    logger = init_logger(logger)

    # --- Config Loader ---
    try:
        loader = ConfigLoader(config, logger=logger, minio_client=minio_client)
    except Exception as e:
        logger.error(f"❌ Failed to load or validate configuration: {e}", exc_info=True)
        report = generate_report(
            success=False, started_at=started_at, tables=[],
            errors=[{"phase": "config_validation", "error": str(e)}]
        )
        logger.info("🏁 Ingestion terminated during config validation")
        safe_log_json(logger, report)
        return report

    # --- Init run context (tenant, defaults, tables, DB) ---
    ctx = init_run_context(spark, logger, loader)
    tenant = ctx["tenant"]
    tables = ctx["tables"]

    table_reports: list[dict[str, Any]] = []
    error_list: list[dict[str, Any]] = []

    # --- Table-level processing ---
    for table in tables:
        try:
            report_row = process_table(
                spark=spark,
                logger=logger,
                loader=loader,
                tenant=tenant,
                table=table,
                run_started_at_iso=started_at,
                minio_client=minio_client,
            )
            table_reports.append(report_row)
        except AnalysisException as e:
            entry = error_entry_for_exception(table, e)
            table_reports.append(entry)
            error_list.append(entry)
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

    logger.info("🏁 Ingestion complete")
    safe_log_json(logger, report)
    return report
