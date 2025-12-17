"""
File: src/data_lakehouse_ingest/loaders/dsv.py

Purpose:
    Provides functions to load Delimiter-Separated Values (DSV) files into Spark DataFrames.
    Supports CSV, TSV, and custom-delimited formats using Spark’s CSV reader, with structured logging.
"""

import logging
from pyspark.sql import SparkSession, DataFrame


def load_dsv_data(
    spark: SparkSession, path: str, opts: dict, logger: logging.Logger, delimiter: str = ","
) -> DataFrame:
    """
    Load DSV (Delimiter-Separated Values) data into a Spark DataFrame.

    Supports common formats like CSV (comma-delimited) and TSV (tab-delimited),
    as well as any custom delimiter provided by the caller.

    Args:
        spark (SparkSession): Active Spark session used to read the file.
        path (str): Path to the DSV file or directory (supports s3a://, file://, etc.).
        opts (dict): Reader options for Spark’s CSV reader (e.g., {"header": "true"}).
        logger (logging.Logger): Logger for recording progress and errors.
        delimiter (str, optional): Field delimiter (default: ","). Use "\t" for TSV.

    Returns:
        DataFrame: Spark DataFrame containing parsed DSV records.

    Raises:
        Exception: If reading or parsing the DSV data fails.
    """
    # Identify format label based on delimiter (for logging readability)
    format_name = (
        "TSV" if delimiter == "\t" else "CSV" if delimiter == "," else f"DSV('{delimiter}')"
    )

    logger.info(f"📂 Reading {format_name} data from: {path}")

    # Ensure opts is a valid mapping
    if opts is None:
        opts = {}

    # Merge user-provided options with enforced delimiter (creates a new dict)
    spark_opts = {**opts, "delimiter": delimiter}

    try:
        # Use Spark’s CSV reader for all DSV formats (CSV/TSV/etc.)
        df = spark.read.options(**spark_opts).format("csv").load(path)

        # Count and log how many records were successfully loaded
        record_count = df.count()
        logger.info(f"✅ Loaded {record_count} {format_name} records from {path}")

        return df

    except Exception as e:
        # Log the full stack trace and re-raise to be handled by the caller
        logger.error(f"❌ Failed to load {format_name} from {path}: {e}", exc_info=True)
        raise


def load_csv_data(spark: SparkSession, path: str, opts: dict, logger: logging.Logger) -> DataFrame:
    """
    Convenience wrapper for reading CSV files using load_dsv_data.
    """
    return load_dsv_data(spark, path, opts, logger, delimiter=",")


def load_tsv_data(spark: SparkSession, path: str, opts: dict, logger: logging.Logger) -> DataFrame:
    """
    Convenience wrapper for reading TSV files using load_dsv_data.
    """
    return load_dsv_data(spark, path, opts, logger, delimiter="\t")
