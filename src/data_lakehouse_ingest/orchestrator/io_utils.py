"""
File name: src/data_lakehouse_ingest/orchestrator/io_utils.py

Input/output utilities for the Data Lakehouse Ingest framework.
Handles file format detection, data loading from Bronze sources,
and writing curated data to Silver Delta tables.

Provides a unified interface for reading CSV, TSV, JSON, and XML formats,
and ensures consistent creation and registration of Delta tables in Spark.
"""
from typing import Tuple
import logging
from pyspark.sql import SparkSession

# reuse your existing loaders
from ..loaders.json_loader import load_json_data
from ..loaders.xml_loader import load_xml_data
from ..loaders.dsv_loader import load_csv_data, load_tsv_data


def detect_format(bronze_path: str, explicit_fmt: str | None) -> str:
    """
    Detect the input file format for a given Bronze layer path.

    Determines the file format based on either an explicit configuration
    value (`explicit_fmt`) or by inspecting the file extension.

    Supported extensions: `.csv`, `.tsv`, `.json`, `.xml`.

    Args:
        bronze_path (str): Full S3/local path to the input data file.
        explicit_fmt (str | None): Optional explicit format (csv, tsv, json, xml).

    Returns:
        str: The detected format name ("csv", "tsv", "json", or "xml").

    Notes:
        - Explicit format overrides file extension detection.
        - Defaults to "csv" when no recognizable extension is found.
        - Ensures consistent downstream loader selection in ingestion pipelines.
    """
    if explicit_fmt:
        return explicit_fmt
    ext = bronze_path.split(".")[-1].lower()
    return "xml" if ext == "xml" else ("json" if ext == "json" else ("tsv" if ext == "tsv" else "csv"))


def load_table_data(
    spark: SparkSession,
    bronze_path: str,
    fmt: str,
    opts: dict,
    logger: logging.Logger,
) -> Tuple[object, int]:
    """
    Loads a DataFrame and returns (df, rows_in).
    """
    fmt_to_loader = {
        "json": load_json_data,
        "xml": load_xml_data,
        "csv": load_csv_data,
        "tsv": load_tsv_data,
    }

    if fmt not in fmt_to_loader:
        raise ValueError(f"❌ Unsupported file format '{fmt}'")

    loader_fn = fmt_to_loader[fmt]
    df = loader_fn(spark, bronze_path, opts, logger)
    rows_in = df.count()
    logger.info(f"✅ Loaded {rows_in} {fmt.upper()} records from {bronze_path}")
    return df, rows_in


def write_to_delta(
    df,
    spark: SparkSession,
    tenant: str,
    name: str,
    silver_path: str,
    partition_by: str | list[str] | None,
    mode: str,
    logger: logging.Logger,
) -> int:
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.save(silver_path)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS `{tenant}`.`{name}`
        USING DELTA
        LOCATION '{silver_path}'
    """)
    rows_written = spark.read.format("delta").load(silver_path).count()
    logger.info(f"✅ Wrote {rows_written} rows to {tenant}.{name} at {silver_path}")
    return rows_written
