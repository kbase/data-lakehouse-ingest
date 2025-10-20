# src/data_lakehouse_ingest/loaders/tsv_loader.py

import logging
from pyspark.sql import SparkSession, DataFrame

def load_tsv_data(spark: SparkSession, path: str, opts: dict, logger: logging.Logger) -> DataFrame:
    """
    Load TSV (Tab-Separated Values) data into a Spark DataFrame.

    Args:
        spark (SparkSession): Active Spark session used to read the file.
        path (str): Path to the TSV file or directory (supports s3a://, file://, etc.).
        opts (dict): Reader options for CSV-style parsing (e.g., {"header": "true"}).
        logger (logging.Logger): Logger for recording progress and errors.

    Returns:
        DataFrame: Spark DataFrame containing parsed TSV records.

    Raises:
        Exception: If reading or parsing the TSV data fails.
    """
    logger.info(f"📂 Reading TSV data from: {path}")
    try:
        # Copy options to avoid mutating the caller’s dictionary
        opts = opts.copy()

        # Force the delimiter to a tab character, ensuring proper TSV parsing
        opts["delimiter"] = "\t"

        # Use Spark’s CSV reader (works for any delimited text format)
        df = spark.read.options(**opts).format("csv").load(path)

        # Count and log how many records were successfully loaded
        record_count = df.count()
        logger.info(f"✅ Loaded {record_count} TSV records from {path}")

        return df
    except Exception as e:
        # Log the complete error stack trace and re-raise for higher-level handling
        logger.error(f"❌ Failed to load TSV: {e}", exc_info=True)
        raise
