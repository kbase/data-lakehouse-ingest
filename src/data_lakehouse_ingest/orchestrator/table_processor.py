"""
Purpose:
    Provides a unified entry point for processing a single table ingestion within
    the Data Lakehouse Ingest framework.

    The `process_table()` function orchestrates:
      - Schema resolution (using LinkML or SQL schemas)
      - Data loading from the Bronze layer (via Spark)
      - Schema application and column alignment
      - Writing curated data to the Silver layer in Delta format
"""

from __future__ import annotations
import logging
from datetime import datetime, timezone
from typing import Any
from minio import Minio
from pyspark.sql import SparkSession

from .schema_utils import resolve_schema, apply_schema_columns
from .io_utils import detect_format, load_table_data, write_to_delta


def process_table(
    spark: SparkSession,
    logger: logging.Logger,
    loader: Any,
    ctx: dict[str, Any],
    table: dict,
    run_started_at_iso: str,
    minio_client: Minio | None = None,
) -> dict[str, Any]:
    """
    Ingest a single table from the Bronze layer into the Silver Delta layer.

    This function handles both standard and specialized ingestion workflows:
    - Determines file format (CSV, TSV, JSON, XML, etc.)
    - Resolves the applicable schema using LinkML or SQL sources
    - Loads data from the Bronze path via Spark
    - Applies schema alignment and column cleanup
    - Writes the processed DataFrame to the Silver Delta location
    - Returns a structured report entry summarizing ingestion results

    Args:
        spark (SparkSession):
            Active Spark session for reading and writing data.

        logger (logging.Logger):
            Structured logger instance with contextual metadata (pipeline, schema, table).

        loader (Any):
            Loader object that provides methods like `get_bronze_path()` and `get_silver_path()`.

        ctx (dict[str, Any]):
            Execution context containing keys such as:
                - "tenant": Tenant identifier
                - "namespace": Fully qualified namespace for the Delta tables
                - "namespace_base_path": Base path under which Silver tables will be written

        table (dict):
            Table configuration dictionary containing keys such as:
                - "name": Table name
                - "format": Input format override (optional)
                - "mode": Write mode (e.g., "overwrite", "append")
                - "partition_by": Optional partition columns
                - "drop_extra_columns": Whether to drop non-schema columns

        run_started_at_iso (str):
            ISO-8601 timestamp representing when the pipeline run began.

        minio_client (Minio | None, optional):
            Optional MinIO client for reading schema or metadata when required.


    Returns:
        dict[str, Any]:
            A structured dictionary summarizing the ingestion outcome, including:
                - "name", "tenant", "target_table"
                - "bronze_path", "silver_path"
                - "rows_in", "rows_written", "elapsed_sec"
                - "status": "success" or "failed"
                - Additional diagnostic fields for errors or special handlers.

    Raises:
        KeyError: If required keys (e.g., "name") are missing from the table config.
        Exception: Propagates errors from schema resolution or data loading phases
                   if not handled internally.

    Example:
        >>> result = process_table(
        ...     spark=spark,
        ...     logger=logger,
        ...     loader=loader,
        ...     tenant="tenant_alpha",
        ...     table={"name": "genome", "format": "csv"},
        ...     run_started_at_iso="2025-10-31T12:00:00Z"
        ... )
        >>> print(result["status"])
        success
    """
    # --- Dynamic table context and logging ---
    table_name = table.get("name", "pipeline_stage")

    if hasattr(logger, "context_filter"):
        logger.context_filter.set_table(table_name)

    logger.info(f"Processing table: {table_name}")

    tenant = ctx["tenant"]
    namespace = ctx["namespace"]
    namespace_base_path = ctx["namespace_base_path"]
    
    name = table["name"]
    bronze_path = loader.get_bronze_path(name)
    silver_path = namespace_base_path

    # Regular CSV/TSV/JSON/XML path
    start_table_time = datetime.now(timezone.utc)

    # --- Determine format ---
    fmt = detect_format(bronze_path, table.get("format"))

    # --- Resolve schema (LinkML takes precedence) ---
    schema_sql, schema_source = resolve_schema(
        spark=spark, table=table, logger=logger, minio_client=minio_client
    )

    # --- Load format defaults ---
    if hasattr(loader, "get_defaults_for"):
        opts = loader.get_defaults_for(fmt)
    else:
        # Keep your original fallback behavior
        delimiter = "\t" if fmt == "tsv" else ","
        opts = {"header": True, "delimiter": delimiter, "inferSchema": False}

    # Normalize bools to Spark-friendly strings
    opts = {k: (str(v).lower() if isinstance(v, bool) else v) for k, v in opts.items()}
    opts["recursiveFileLookup"] = "true"

    logger.info(f"   Bronze: {bronze_path}")
    logger.info(f"   Silver: {silver_path}")

    # --- Load data ---
    try:
        df, rows_in = load_table_data(spark, bronze_path, fmt, opts, logger)
    except Exception as e:
        logger.error(f"Failed to load data for table '{name}': {e}", exc_info=True)
        return {
            "name": name,
            "error": str(e),
            "phase": "data_loading",
            "bronze_path": bronze_path,
            "format": fmt,
            "status": "failed",
        }

    # --- Apply schema (rename; optionally drop extras if requested) ---
    df = apply_schema_columns(
        df=df,
        schema_sql=schema_sql,
        logger=logger,
        drop_extra_columns=bool(table.get("drop_extra_columns", False)),  # defaults to False to avoid behavior change
    )

    # --- Write to Delta ---
    partition_by = table.get("partition_by")
    mode = table.get("mode", "overwrite")
    rows_written = write_to_delta(
        df=df,
        spark=spark,
        namespace=namespace,
        namespace_base_path=namespace_base_path,
        name=name,
        silver_path=silver_path,
        partition_by=partition_by,
        mode=mode,
        logger=logger,
    )

    rows_rejected = 0
    partitions_written = None
    quarantine_path = f"{silver_path}/quarantine/{run_started_at_iso.replace(':', '-')}/"
    elapsed_sec = (datetime.now(timezone.utc) - start_table_time).total_seconds()

    logger.info(f"Table {tenant}.{name}: {rows_in} → {rows_written} rows in {elapsed_sec:.2f}s")

    return {
        "name": name,
        "tenant": tenant,
        "target_table": f"{namespace}.{name}",
        "mode": mode,
        "format": fmt,
        "schema_source": schema_source,
        "bronze_path": bronze_path,
        "silver_path": silver_path,
        "rows_in": rows_in,
        "rows_written": rows_written,
        "rows_rejected": rows_rejected,
        "partitions_written": partitions_written,
        "quarantine_path": quarantine_path,
        "elapsed_sec": elapsed_sec,
        "status": "success",
    }
