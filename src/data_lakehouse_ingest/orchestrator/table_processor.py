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
    tenant: str,
    table: dict,
    run_started_at_iso: str,
    minio_client: Minio | None = None,
) -> dict[str, Any]:
    """
    Handles a single table ingestion: schema resolution, data load, Delta write.
    Returns a dict entry for report.
    """

    name = table["name"]
    bronze_path = loader.get_bronze_path(name)
    silver_path = loader.get_silver_path(name)

    # Special handling: custom parser (UniProt)
    if table.get("process_with") == "uniprot":
        from ..parsers.uniprot_ingest import process_uniprot_to_delta
        logger.info(f"🚀 Delegating to UniProt ingestion pipeline for table: {name}")

        start_table_time = datetime.now(timezone.utc)
        process_uniprot_to_delta(
            xml_path=bronze_path,
            namespace=tenant,
            s3_silver_base=silver_path,
            batch_size=table.get("batch_size", 5000)
        )
        elapsed_sec = (datetime.now(timezone.utc) - start_table_time).total_seconds()

        try:
            rows_written = spark.read.format("delta").load(silver_path).count()
        except Exception:
            rows_written = None

        return {
            "name": name,
            "tenant": tenant,
            "target_table": f"{tenant}.{name}",
            "mode": table.get("mode", "overwrite"),
            "format": "xml",
            "schema_source": "custom_parser",
            "bronze_path": bronze_path,
            "silver_path": silver_path,
            "rows_in": None,
            "rows_written": rows_written,
            "rows_rejected": 0,
            "partitions_written": None,
            "quarantine_path": f"{silver_path}/quarantine/{start_table_time.isoformat().replace(':','-')}/",
            "elapsed_sec": elapsed_sec,
            "process_with": "uniprot",
            "status": "success",
        }

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

    logger.info(f"📦 Processing table: {name}")
    logger.info(f"   Bronze: {bronze_path}")
    logger.info(f"   Silver: {silver_path}")

    # --- Load data ---
    try:
        df, rows_in = load_table_data(spark, bronze_path, fmt, opts, logger)
    except Exception as e:
        logger.error(f"❌ Failed to load data for table '{name}': {e}", exc_info=True)
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
        tenant=tenant,
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

    logger.info(f"✅ Table {tenant}.{name}: {rows_in} → {rows_written} rows in {elapsed_sec:.2f}s")

    return {
        "name": name,
        "tenant": tenant,
        "target_table": f"{tenant}.{name}",
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
