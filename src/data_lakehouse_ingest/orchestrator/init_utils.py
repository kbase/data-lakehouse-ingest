"""
Initialization utilities for the Data Lakehouse Ingest framework.
Handles logger setup and Spark session context initialization,
including tenant creation, catalog switching, and configuration extraction.
"""
import logging
from typing import Any
from pyspark.sql import SparkSession
from berdl_notebook_utils.spark.database import create_namespace_if_not_exists


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
        logger.error(f"Failed to create or access namespace for dataset '{dataset}': {e}", exc_info=True)
        raise

    logger.info("Ingestion context initialized successfully.")

    return {
        "tenant": tenant,
        "dataset": dataset,
        "namespace": namespace,
        "namespace_base_path": base_path, 
        "tables": tables,
    }
