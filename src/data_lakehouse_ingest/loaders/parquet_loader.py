"""
Purpose:
    Provides functions to load Parquet files into Spark DataFrames.
    Parquet is a columnar, schema-aware format — so schema inference is automatic.
"""

import logging
from pyspark.sql import SparkSession, DataFrame


def load_parquet_data(
    spark: SparkSession,
    path: str,
    opts: dict | None,
    logger: logging.Logger,
) -> DataFrame:
    """
    Load Parquet data into a Spark DataFrame.

    This function reads one or more Parquet files from the given path using
    Spark's native Parquet reader. Parquet is a schema-aware, columnar format,
    so schema inference is handled automatically by Spark at read time.

    Reader options are optional and typically not required for Parquet, but
    custom options may be supplied (e.g., for schema merging). The function
    logs the source path and the number of records successfully loaded.

    Args:
        spark (SparkSession):
            Active Spark session used to read the Parquet data.
        path (str):
            Path to the Parquet file or directory (local filesystem, s3a://, etc.).
        opts (dict | None):
            Optional Spark reader options. If None, no options are applied.
        logger (logging.Logger):
            Structured logger used for emitting ingestion progress and errors.

    Returns:
        DataFrame:
            Spark DataFrame containing the loaded Parquet data.

    Raises:
        Exception:
            Propagates any exception raised during read or count operations
            after logging the error, allowing upstream orchestration logic
            to handle failures appropriately.
    """

    logger.info(f"📂 Reading PARQUET data from: {path}")

    # Parquet usually does not need options but allow passing custom opts
    spark_opts = opts or {}

    try:
        df = spark.read.options(**spark_opts).parquet(path)

        record_count = df.count()
        logger.info(f"✅ Loaded {record_count} PARQUET records from {path}")

        return df

    except Exception as e:
        logger.error(f"❌ Failed to load PARQUET from {path}: {e}", exc_info=True)
        raise


# Optional wrapper for naming consistency (parquet vs parquet_data)
def load_parquet(spark: SparkSession, path: str, opts: dict, logger: logging.Logger) -> DataFrame:
    return load_parquet_data(spark, path, opts, logger)
