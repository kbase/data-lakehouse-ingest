"""
File: src/data_lakehouse_ingest/loaders/json_loader.py

Purpose:
    Provides functionality to load JSON data files into Spark DataFrames.
    Supports newline-delimited JSON records with configurable reader options
    and structured logging for traceability and error handling.
"""

import logging
from pyspark.sql import SparkSession, DataFrame

def load_json_data(spark: SparkSession, path: str, opts: dict, logger: logging.Logger) -> DataFrame:
    """
    Load newline-delimited JSON files into a Spark DataFrame.

    Args:
        spark (SparkSession): Active Spark session.
        path (str): Path to the JSON file or directory (supports s3a://, file://, etc.).
        opts (dict): Reader options (e.g., {"multiLine": "false"}).
        logger (logging.Logger): Logger for recording progress and errors.

    Returns:
        DataFrame: Loaded Spark DataFrame.

    Raises:
        Exception: If the JSON read operation fails.
    """

    logger.info(f"📂 Reading JSON data from: {path}")
    try:
        # Use Spark's JSON reader with the specified options
        # "multiLine" set to false by default for newline-delimited JSON records
        df = spark.read.option("multiLine", "false").options(**opts).json(path)

        # Count the number of records read and log for traceability
        record_count = df.count()
        logger.info(f"✅ Loaded {record_count} JSON records from {path}")

        return df

    except Exception as e:
        # Log full stack trace for debugging and re-raise the exception
        logger.error(f"❌ Failed to load JSON: {e}", exc_info=True)
        raise
