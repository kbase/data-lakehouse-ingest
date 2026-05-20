"""
Input/output utilities for the Data Lakehouse Ingest framework.
Handles file format detection, data loading from Bronze sources,
and writing curated data to Silver tables via Iceberg catalogs.

Provides a unified interface for reading CSV, TSV, JSON, and XML formats,
and writes tables using catalog-driven APIs (no explicit path management).
"""

import logging
from pyspark.sql import SparkSession, DataFrame

from data_lakehouse_ingest.loaders.json_loader import load_json_data
from data_lakehouse_ingest.loaders.xml_loader import load_xml_data
from data_lakehouse_ingest.loaders.dsv_loader import load_csv_data, load_tsv_data
from data_lakehouse_ingest.loaders.parquet_loader import load_parquet_data


def detect_format(bronze_path: str, explicit_fmt: str | None) -> str:
    """
    Detect the input file format for a given Bronze layer path.

    Determines the file format based on either an explicit configuration
    value (`explicit_fmt`) or by inspecting the file extension.

     Supported extensions: `.csv`, `.tsv`, `.json`, `.xml`, `.parquet`.

    Args:
        bronze_path (str): Full S3/local path to the input data file.
        explicit_fmt (str | None): Optional explicit format (csv, tsv, json, xml, parquet).

    Returns:
         str: The detected format name.

    Notes:
        - Explicit format overrides file extension detection.
        - Defaults to "csv" when no recognizable extension is found.
    """

    # TODO: Explore using python-magic or content-based format detection.
    #
    # Current behavior:
    #   - Relies solely on file extensions (csv, tsv, json, xml).
    #   - Explicit format always overrides auto-detection.
    #
    # Future improvement:
    #   - Use `python-magic` or similar libraries to inspect file headers
    #     instead of relying only on extensions.

    if explicit_fmt:
        return explicit_fmt.lower()

    extension_map = {
        "csv": "csv",
        "tsv": "tsv",
        "json": "json",
        "xml": "xml",
        "parquet": "parquet",
    }

    ext = bronze_path.split(".")[-1].lower()
    return extension_map.get(ext, "csv")


def load_table_data(
    spark: SparkSession,
    bronze_path: str,
    fmt: str,
    opts: dict,
    logger: logging.Logger,
) -> tuple[DataFrame, int]:
    """
    Load source data into a DataFrame and return the DataFrame with its input row count.

    Returns:
        tuple[DataFrame, int]: The loaded DataFrame and number of rows read from the source.
    """
    fmt_to_loader = {
        "json": load_json_data,
        "xml": load_xml_data,
        "csv": load_csv_data,
        "tsv": load_tsv_data,
        "parquet": load_parquet_data,
    }

    if fmt not in fmt_to_loader:
        raise ValueError(f"Unsupported file format '{fmt}'")

    loader_fn = fmt_to_loader[fmt]
    df = loader_fn(spark, bronze_path, opts, logger)
    rows_in = df.count()
    return df, rows_in


def table_exists(spark: SparkSession, full_table: str) -> bool:
    """
    Check whether a catalog table exists.

    Uses Spark table access so it works with fully qualified catalog table names,
    including Iceberg tables.
    """
    try:
        spark.table(full_table).limit(1).count()
        return True
    except Exception:
        return False


def write_table(
    df: DataFrame,
    spark: SparkSession,
    namespace: str,
    name: str,
    partition_by: str | list[str] | None,
    mode: str,
    rows_in: int,
    logger: logging.Logger,
) -> int:
    """
    Write a DataFrame to a table using catalog-driven Iceberg APIs.

    The Iceberg catalog manages table storage locations, so this function does
    not construct explicit paths or use LOCATION clauses. For overwrite mode,
    the table is created or replaced. For append mode, the table must already
    exist.

    Args:
        df: DataFrame to write.
        spark: Active SparkSession.
        namespace: Fully qualified namespace (e.g., ``my.dataset`` or ``kbase.dataset``).
        name: Table name.
        partition_by: Optional partition column(s).
        mode: Write mode. Defaults to ``"overwrite"`` when omitted.
              Supported values are ``"overwrite"`` and ``"append"``.
        rows_in: Number of rows read from the source DataFrame. This value is
                 returned and logged as rows written, rather than counting the
                 full target table after write.
        logger: Logger for structured output.

    Returns:
        Number of rows written.
    """

    full_table = f"{namespace}.{name}"
    # Default mode
    mode = (mode or "overwrite").lower()

    if mode not in {"overwrite", "append"}:
        raise ValueError(
            f"Unsupported write mode '{mode}' for {full_table}. "
            "Supported modes are 'overwrite' and 'append'."
        )

    exists = table_exists(spark, full_table)

    logger.info(f"Writing table: {full_table} (mode={mode}, exists={exists})")

    if mode == "append" and not exists:
        raise ValueError(
            f"Cannot append to {full_table} because the table does not exist. "
            "Use mode='overwrite' or omit mode to create the table."
        )

    rows_written = rows_in

    writer = df.writeTo(full_table)

    if partition_by:
        cols = [partition_by] if isinstance(partition_by, str) else list(partition_by)
        writer = writer.partitionedBy(*cols)

    if mode == "append":
        writer.append()
    else:
        writer.createOrReplace()

    logger.info(f"Wrote {rows_written} rows → {full_table}")

    return rows_written
