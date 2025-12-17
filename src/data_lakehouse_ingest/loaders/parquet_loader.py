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

    Args:
        spark (SparkSession): Active Spark session.
        path (str): Path to Parquet file(s) (local, s3a://, etc.).
        opts (dict | None): Reader options (rarely needed for Parquet).
        logger (logging.Logger): Structured logger.

    Returns:
        DataFrame: Loaded DataFrame.

    Raises:
        Exception: On any read or parse failure.
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
