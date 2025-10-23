# src/data_lakehouse_ingest/loaders/xml_loader.py

import logging
from pyspark.sql import SparkSession, DataFrame

def load_xml_data(spark: SparkSession, path: str, opts: dict, logger: logging.Logger) -> DataFrame:
    """
    Load XML data into a Spark DataFrame using the spark-xml library.

    Args:
        spark (SparkSession): Active Spark session used to read XML files.
        path (str): Path to the XML file or directory (supports s3a://, file://, etc.).
        opts (dict): Reader options for spark-xml. Must include 'rowTag'.
        logger (logging.Logger): Logger for tracking progress and errors.

    Returns:
        DataFrame: Spark DataFrame containing parsed XML records.

    Raises:
        ValueError: If 'rowTag' is missing in opts.
        Exception: If the XML read operation fails.
    """

    logger.info(f"📂 Reading XML data from: {path}")

    # Ensure 'rowTag' is provided — spark-xml requires this to define the row structure
    row_tag = opts.get("rowTag")
    if not row_tag:
        logger.error("❌ Missing 'rowTag' for XML loader.")
        raise ValueError("XML reader requires a 'rowTag' option in config defaults or table definition.")

    try:
        # Read XML files using spark-xml and provided options
        df = spark.read.format("xml").options(**opts).load(path)

        # Count and log the number of records read successfully
        record_count = df.count()
        logger.info(f"✅ Loaded {record_count} XML records using rowTag='{row_tag}'")

        return df

    except Exception as e:
        # Log full stack trace for debugging and re-raise the exception
        logger.error(f"❌ Failed to load XML: {e}", exc_info=True)
        raise
