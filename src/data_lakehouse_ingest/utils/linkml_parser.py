"""
LinkML Schema Parsing Utilities

This module provides helper functionality for loading and parsing LinkML YAML
schemas from either local files or S3A/MinIO object storage. It converts LinkML
slot definitions into a Spark-compatible column/type mapping that can be used
for schema enforcement in the Data Lakehouse Ingest framework.

Features:
    - Supports both local filesystem paths and `s3a://` URLs.
    - Uses MinIO client for remote object retrieval.
    - Converts LinkML ranges into Spark SQL types (STRING, INT, DOUBLE, etc.).
    - Returns a simple Python dict mapping {column_name: spark_type}.
"""

import logging
from typing import Dict, Any
from pyspark.sql import SparkSession


def load_linkml_schema(
    spark: SparkSession,
    path: str,
    logger: logging.Logger,
    minio_client: object | None = None,
) -> dict[str, str]:
    """
    Load and parse a LinkML YAML schema, returning a dict mapping columns to
    Spark-compatible SQL types.

    This function supports both local paths and remote schemas stored in S3A /
    MinIO. When a remote schema is referenced via an `s3a://` URI, the file is
    fetched through the provided MinIO client and stored temporarily on disk
    so it can be parsed by `linkml_runtime`.

    Steps Performed:
        1. Resolve the schema path (local or S3A).
        2. Fetch and materialize S3A schemas into a temporary YAML file.
        3. Initialize a SchemaView from the YAML.
        4. Identify the first LinkML class and extract its slots.
        5. Convert LinkML ranges (string, integer, boolean, etc.) into Spark
           SQL types using a built-in mapping.
        6. Return a {column_name -> spark_sql_type} dict.

    Args:
        spark (SparkSession):
            Active SparkSession. Currently unused directly but kept for future
            Spark-based schema helpers.
        path (str):
            Path to the LinkML schema. Can be a local filesystem path or an
            `s3a://bucket/prefix/file.yaml` URI.
        logger (logging.Logger):
            Logger for structured debug and info output.
        minio_client (Any, optional):
            MinIO client instance used to fetch S3A schema files. Required if
            `path` begins with `s3a://`.

    Returns:
        Dict[str, str]:
            A dictionary mapping column names to Spark SQL type strings
            (e.g., `"STRING"`, `"INT"`, `"BOOLEAN"`).

    Raises:
        ValueError:
            If the LinkML schema contains no classes.
        Exception:
            If the schema cannot be fetched or parsed.

    Notes:
        - Only the first class found in the schema is parsed. This matches the
          common pattern of a single-table ingestion schema.
        - Temporary files created for S3A fetches are automatically cleaned up.
    """
    from linkml_runtime.utils.schemaview import SchemaView
    import tempfile
    import os

    logger.info(f"Parsing LinkML schema from {path}")

    # ------------------------------------------------------------------
    # 1. Handle S3A/MinIO fetch
    # ------------------------------------------------------------------
    local_path = path
    if path.startswith("s3a://"):
        bucket, key = path.replace("s3a://", "").split("/", 1)
        obj = minio_client.get_object(bucket, key)
        yaml_str = obj.read().decode("utf-8")
        obj.close()

        # Write to a temporary local file so SchemaView can open it
        tmpfile = tempfile.NamedTemporaryFile(delete=False, suffix=".yaml")
        tmpfile.write(yaml_str.encode("utf-8"))
        tmpfile.close()
        local_path = tmpfile.name
        logger.debug(f"Temporary local schema file: {local_path}")

    # ------------------------------------------------------------------
    # 2. Load with SchemaView
    # ------------------------------------------------------------------
    try:
        view = SchemaView(local_path)
    except Exception as e:
        logger.error(f"Failed to load SchemaView for {path}: {e}", exc_info=True)
        if local_path != path and os.path.exists(local_path):
            os.remove(local_path)
        raise

    # ------------------------------------------------------------------
    # 3. Extract first class and build mapping
    # ------------------------------------------------------------------
    all_classes = list(view.all_classes().keys())
    if not all_classes:
        raise ValueError(f"No classes found in LinkML schema: {path}")
    class_name = all_classes[0]
    class_def = view.get_class(class_name)

    type_map: dict[str, str] = {
        "integer": "INT",
        "float": "DOUBLE",
        "double": "DOUBLE",
        "boolean": "BOOLEAN",
        "string": "STRING",
        "datetime": "TIMESTAMP",
        "date": "DATE",
    }

    schema_cols: dict[str, str] = {}
    for slot_name in class_def.slots:
        slot = view.induced_slot(slot_name, class_name)
        spark_type = type_map.get(slot.range, "STRING")
        schema_cols[slot.name] = spark_type

    schema_sql = ", ".join([f"{c} {t}" for c, t in schema_cols.items()])
    logger.info(f"Derived schema_sql from LinkML: {schema_sql}")

    # Cleanup temp file if used
    if local_path != path and os.path.exists(local_path):
        os.remove(local_path)

    return schema_cols
