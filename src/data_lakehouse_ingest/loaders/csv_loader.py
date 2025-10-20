# src/data_lakehouse_ingest/loaders/csv_loader.py

import logging
from pyspark.sql import SparkSession, DataFrame

def load_csv_data(spark: SparkSession, path: str, opts: dict, logger: logging.Logger) -> DataFrame:
    """
    Load CSV data into a Spark DataFrame.

    Args:
        spark (SparkSession): Active Spark session used to read the file.
        path (str): Path to the CSV file or directory (supports s3a://, file://, etc.).
        opts (dict): Reader options for CSV parsing (e.g., {"header": "true", "delimiter": ","}).
        logger (logging.Logger): Logger for recording progress and errors.

    Returns:
        DataFrame: Spark DataFrame containing parsed CSV records.

    Raises:
        Exception: If reading or parsing the CSV data fails.
    """

    logger.info(f"📂 Reading CSV data from: {path}")
    try:
        # Read the CSV file(s) using the provided Spark session and options.
        # Common options include 'header', 'delimiter', and 'inferSchema'.
        df = spark.read.options(**opts).format("csv").load(path)

        # Count and log how many rows were successfully loaded.
        record_count = df.count()
        logger.info(f"✅ Loaded {record_count} CSV records from {path}")

        return df

    except Exception as e:
        # Log the full exception stack trace for debugging and re-raise it.
        logger.error(f"❌ Failed to load CSV: {e}", exc_info=True)
        raise
