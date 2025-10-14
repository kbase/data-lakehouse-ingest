import logging
from typing import Dict, Any
from pyspark.sql import SparkSession


def load_linkml_schema(
    spark: SparkSession,
    path: str,
    logger: logging.Logger,
    minio_client: Any = None
) -> Dict[str, str]:
    """
    Parse a LinkML YAML schema (local or s3a) and return
    a mapping of column_name -> Spark-compatible type.
    """
    from linkml_runtime.utils.schemaview import SchemaView
    import tempfile
    import os

    logger.info(f"🧩 Parsing LinkML schema from {path}")

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
        logger.debug(f"   Temporary local schema file: {local_path}")

    # ------------------------------------------------------------------
    # 2. Load with SchemaView
    # ------------------------------------------------------------------
    try:
        view = SchemaView(local_path)
    except Exception as e:
        logger.error(f"❌ Failed to load SchemaView for {path}: {e}", exc_info=True)
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

    type_map = {
        "integer": "INT",
        "float": "DOUBLE",
        "double": "DOUBLE",
        "boolean": "BOOLEAN",
        "string": "STRING",
        "datetime": "TIMESTAMP",
        "date": "DATE",
    }

    schema_cols = {}
    for slot_name in class_def.slots:
        s = view.induced_slot(slot_name, class_name)
        spark_type = type_map.get(s.range, "STRING")
        schema_cols[s.name] = spark_type

    schema_sql = ", ".join([f"{c} {t}" for c, t in schema_cols.items()])
    logger.info(f"✅ Derived schema_sql from LinkML: {schema_sql}")

    # Cleanup temp file if used
    if local_path != path and os.path.exists(local_path):
        os.remove(local_path)

    return schema_cols
