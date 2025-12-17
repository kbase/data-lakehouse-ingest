"""
Initialization utilities for the Data Lakehouse Ingest framework.
Handles logger setup and Spark session context initialization,
including tenant creation, catalog switching, and configuration extraction.
"""

import logging
from typing import Any
from pyspark.sql import SparkSession
from berdl_notebook_utils.spark.database import create_namespace_if_not_exists
from data_lakehouse_ingest.logger import setup_logger


def init_logger(logger: logging.Logger | None) -> logging.Logger:
    """
    Initialize or return a structured logger for the ingestion framework.

    If an external logger is provided, it is reused as-is. If no logger is
    supplied, this function creates and returns a fully configured structured
    logger using `setup_logger()`. The default structured logger emits JSON-
    formatted log entries to both the console and a timestamped log file,
    and automatically injects pipeline context (pipeline name, schema, table)
    into each record.

    Args:
        logger (logging.Logger | None):
            Optional externally provided logger instance. If None, a new
            structured pipeline logger is created using default values.

    Returns:
        logging.Logger:
            A structured logger instance configured with:
            - JSON-formatted output
            - Console and file handlers
            - Pipeline context filters (pipeline, schema, table)
            - Dynamic log level support

    Notes:
        - The default pipeline name is set to `"data_lakehouse_ingest"`.
        - The default schema is set to `"default"`.
        - The default log level is `"INFO"`, unless overridden by environment
          variables inside `setup_logger()`.
        - Repeated calls return the same underlying logger instance due to the
          singleton behavior in `setup_logger()`.
    """
    if logger is not None:
        return logger

    # Use structured logger
    return setup_logger(
        pipeline_name="data_lakehouse_ingest",
        schema="default",
        log_level="INFO",
    )


def init_run_context(
    spark: SparkSession,
    logger: logging.Logger,
    loader: Any,
) -> dict[str, Any]:
    """
    Initialize the ingestion run context based on config.

    Uses the JupyterHub helper `create_namespace_if_not_exists` instead of
    SQL CREATE DATABASE statements. The behavior depends on the 'is_tenant'
    flag in the config.

    Args:
        spark (SparkSession): Active Spark session.
        logger (logging.Logger): Logger instance for structured output.
        loader (Any): ConfigLoader or equivalent with get_tenant(), get_dataset(), get_tables().

    Returns:
        dict[str, Any]: Context dictionary with namespace, tables, and defaults.
    """
    # ----------------------------------------------------------------------
    # Extract configuration
    # ----------------------------------------------------------------------
    tenant = loader.config.get("tenant")
    dataset = loader.config.get("dataset")
    tables = loader.get_tables()

    if not dataset:
        raise ValueError("Config must include 'dataset' field.")

    logger.info(f"Loaded configuration: tenant={tenant}, dataset={dataset}")
    logger.info(f"Found {len(tables)} table(s) to process")

    # ----------------------------------------------------------------------
    # Create namespace using JupyterHub helper
    # ----------------------------------------------------------------------
    try:
        if tenant:
            # Multi-tenant governed environment
            namespace = create_namespace_if_not_exists(
                spark,
                namespace=dataset,
                tenant_name=tenant,
            )
            logger.info(f"Tenant namespace created/accessed: {namespace}")
        else:
            # Personal (user-level) environment
            namespace = create_namespace_if_not_exists(spark, dataset)
            logger.info(f"Personal namespace created/accessed: {namespace}")

        spark.catalog.setCurrentDatabase(namespace)

        # Extract physical namespace path
        try:
            ns_info = spark.sql(f"DESCRIBE NAMESPACE EXTENDED {namespace}").collect()
            base_path = [r.info_value for r in ns_info if r.info_name.lower() == "location"][0]
            logger.info(f"Namespace storage location: {base_path}")
        except Exception as e:
            logger.warning(f"Unable to determine namespace storage location for '{namespace}': {e}")
            base_path = None

    except Exception as e:
        logger.error(
            f"Failed to create or access namespace for dataset '{dataset}': {e}", exc_info=True
        )
        raise

    logger.info("Ingestion context initialized successfully.")

    return {
        "tenant": tenant,
        "dataset": dataset,
        "namespace": namespace,
        "namespace_base_path": base_path,
        "tables": tables,
    }
