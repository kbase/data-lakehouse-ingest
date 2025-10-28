"""
File name: src/data_lakehouse_ingest/orchestrator/init_utils.py

Initialization utilities for the Data Lakehouse Ingest framework.
Handles logger setup and Spark session context initialization,
including tenant creation, catalog switching, and configuration extraction.
"""
import logging
from typing import Any
from pyspark.sql import SparkSession


def init_logger(logger: logging.Logger | None) -> logging.Logger:
    """
    Initialize or return an existing logger for the ingestion framework.

    If an external logger is not provided, a default logger is configured
    with INFO-level verbosity and a simple timestamped format.

    Args:
        logger (logging.Logger | None): Optional external logger instance.
            If None, a new logger is created and configured.

    Returns:
        logging.Logger: The initialized or existing logger instance.

    Notes:
        - The default logger uses a simple, human-readable format.
        - All log messages will include timestamps and severity levels.
        - The logger name is fixed to "data_lakehouse_ingest" for consistency.
    """
    if logger is None:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
        logger = logging.getLogger("data_lakehouse_ingest")
        logger.info("No external logger provided; using internal basic logger.")
    return logger


def init_run_context(
    spark: SparkSession,
    logger: logging.Logger,
    loader: Any,
) -> dict[str, Any]:
    tenant = loader.get_tenant()
    tables = loader.get_tables()

    logger.info(f"🔧 Loaded configuration for tenant: {tenant}")
    logger.info(f"📋 Found {len(tables)} table(s) to process")

    spark.sql(f"CREATE DATABASE IF NOT EXISTS `{tenant}`")
    spark.catalog.setCurrentDatabase(tenant)

    # Prefer loader method if present; else an empty dict
    format_defaults = loader.get_all_defaults() if hasattr(loader, "get_all_defaults") else {}

    return {
        "tenant": tenant,
        "tables": tables,
        "format_defaults": format_defaults,
    }
