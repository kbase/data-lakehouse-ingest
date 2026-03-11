"""
Purpose:
    Provides a unified entry point for processing a single table ingestion within
    the Data Lakehouse Ingest framework.

    The `process_table()` function orchestrates:
      - Schema resolution (using SQL schema strings, structured column definitions, or DataFrame schemas)
      - Data loading either from the Bronze layer (via Spark) or from a provided DataFrame
      - Schema application and column alignment
      - Writing curated data to the Silver layer in Delta format
"""

from __future__ import annotations
import logging
from datetime import datetime, timezone
from typing import Any
from minio import Minio
from pyspark.sql import SparkSession, DataFrame

from data_lakehouse_ingest.orchestrator.schema_utils import (
    resolve_schema,
    apply_schema_columns,
)
from data_lakehouse_ingest.orchestrator.io_utils import (
    detect_format,
    load_table_data,
    write_to_delta,
)
from data_lakehouse_ingest.utils.delta_comments import apply_comments_from_table_schema

from .models import TableProcessSuccess, TableProcessFailure, TableProcessResult


def process_table(
    spark: SparkSession,
    logger: logging.Logger,
    loader: Any,
    ctx: dict[str, Any],
    table: dict,
    run_started_at_iso: str,
    minio_client: Minio | None = None,
    df_override: DataFrame | None = None,
) -> TableProcessResult:
    """
    Ingest a single table from the Bronze layer into the Silver Delta layer.

    This function handles both standard and specialized ingestion workflows:
    - Determines file format (CSV, TSV, JSON, XML, Parquet, etc.) when reading from Bronze storage
    - Resolves the applicable schema using SQL schema strings, structured column definitions, or DataFrame schemas
    - Loads data either from the Bronze path via Spark or from a provided DataFrame override
    - Applies schema alignment and column cleanup
    - Writes the processed DataFrame to the Silver Delta location
    - Applies Delta column comments when a structured (list-of-maps) schema is used
    - Returns a structured result object summarizing ingestion results

    Column comments are applied when a structured schema with comments metadata is available.

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

        df_override (DataFrame | None, optional):
            Optional Spark DataFrame to ingest instead of loading from the Bronze path.
            When provided, the Bronze read path is skipped. The result reports
            `input_source="dataframe"`, and `bronze_path` and `format` are returned as None.

    Returns:
        TableProcessResult:
            A structured table-processing result object. Returns:
                - TableProcessSuccess when ingestion completes successfully
                - TableProcessFailure when data loading fails

            The success result includes fields such as:
                - name, tenant, target_table
                - bronze_path, silver_path
                - rows_in, rows_written, elapsed_sec
                - comments_report
                - status

            The failure result includes fields such as:
                - name, error, phase
                - bronze_path, format, input_source
                - status

    Raises:
        KeyError: If required keys (e.g., "name") are missing from the table config.
        Exception: May propagate errors from schema resolution or downstream processing
                   phases that are not handled internally.

    Example:
        >>> result = process_table(
        ...     spark=spark,
        ...     logger=logger,
        ...     loader=loader,
        ...     ctx={
        ...         "tenant": "tenant_alpha",
        ...         "namespace": "tenant_alpha__dataset",
        ...         "namespace_base_path": "s3a://silver/"
        ...     },
        ...     table={"name": "genome", "format": "csv"},
        ...     run_started_at_iso="2025-10-31T12:00:00Z"
        ... )
        >>> print(result.status)
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
    silver_path = namespace_base_path

    start_table_time = datetime.now(timezone.utc)

    # --- Resolve schema (needed for both modes) ---
    resolved = resolve_schema(spark=spark, table=table, logger=logger, minio_client=minio_client)

    # We'll fill these depending on mode
    bronze_path: str | None = None
    fmt: str | None = None
    input_source = "dataframe" if df_override is not None else "bronze"

    logger.info(f"   Silver: {silver_path}")

    # --- Load data (either DF override OR Bronze path) ---
    try:
        if df_override is not None:
            df = df_override
            rows_in = df.count()
            bronze_path = None
            fmt = None
            logger.info("   Bronze: <dataframe override>")
        else:
            bronze_path = loader.get_bronze_path(name)
            fmt = detect_format(bronze_path, table.get("format"))

            # --- Load format defaults ---
            if hasattr(loader, "get_defaults_for"):
                opts = loader.get_defaults_for(fmt)
            else:
                delimiter = "\t" if fmt == "tsv" else ","
                opts = {"header": True, "delimiter": delimiter, "inferSchema": False}

            # Normalize bools to Spark-friendly strings
            opts = {k: (str(v).lower() if isinstance(v, bool) else v) for k, v in opts.items()}
            opts["recursiveFileLookup"] = "true"

            logger.info(f"   Bronze: {bronze_path}")

            df, rows_in = load_table_data(spark, bronze_path, fmt, opts, logger)

    except Exception as e:
        logger.error(f"Failed to load data for table '{name}': {e}", exc_info=True)
        return TableProcessFailure(
            name=name,
            error=str(e),
            phase="data_loading",
            bronze_path=bronze_path,
            format=fmt,
            input_source=input_source,
            status="failed",
        )

    # --- Apply schema  ---
    df, schema_meta = apply_schema_columns(
        df=df,
        schema_defs=resolved.schema_defs,
        logger=logger,
    )

    dropped_cols = schema_meta.get("dropped_columns", [])

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

    # Applies Delta column comments when comment metadata is available (from structured schema)
    comments_report = None

    if resolved.comment_metadata:
        full_table_name = f"{namespace}.{name}"
        comments_report = apply_comments_from_table_schema(
            spark=spark,
            full_table_name=full_table_name,
            table_schema=resolved.comment_metadata,
            logger=logger,
            require_existing_table=True,
        )
        logger.info(f"Column comment apply report: {comments_report}")

    rows_rejected = 0
    partitions_written = None
    quarantine_path = f"{silver_path}/quarantine/{run_started_at_iso.replace(':', '-')}/"
    elapsed_sec = (datetime.now(timezone.utc) - start_table_time).total_seconds()

    logger.info(f"Table {namespace}.{name}: {rows_in} → {rows_written} rows in {elapsed_sec:.2f}s")

    return TableProcessSuccess(
        name=name,
        tenant=tenant,
        target_table=f"{namespace}.{name}",
        mode=mode,
        format=fmt,
        schema_source=str(resolved.schema_source),
        input_source=input_source,
        bronze_path=bronze_path,
        silver_path=silver_path,
        rows_in=rows_in,
        rows_written=rows_written,
        rows_rejected=rows_rejected,
        extra_columns_dropped=dropped_cols,
        partitions_written=partitions_written,
        quarantine_path=quarantine_path,
        elapsed_sec=elapsed_sec,
        status="success",
        comments_report=comments_report,
    )
