"""
Schema management utilities for the Data Lakehouse Ingest framework.
Handles schema resolution using inline SQL and column alignment for ingested DataFrames.
Provides helpers to enforce consistent structure between raw data and curated Delta tables.
"""
from minio import Minio
from pyspark.sql import SparkSession, DataFrame
import logging
from enum import Enum

class SchemaSource(Enum):
    """Enum describing the origin of a resolved schema."""
    SCHEMA_SQL = "schema_sql"
    INFERRED = "inferred"


def resolve_schema(
    spark: SparkSession,
    table: dict[str, object],
    logger: logging.Logger,
    minio_client: Minio | None = None,
) -> tuple[str | None, str]:
    """
    Resolve the schema definition for a given table.

    Current behavior (LinkML not yet supported):
        - If a LinkML schema path is provided, the function raises
          NotImplementedError (LinkML support not implemented yet).
        - If `schema_sql` is provided (and no LinkML schema), it is returned.
        - If neither is provided, the schema is treated as inferred.

    Args:
        spark (SparkSession): Active Spark session (unused until LinkML is implemented).
        table (dict): Full table definition from the ingestion config. 
            This dict may include many fields (e.g., name, bronze_path, enabled, 
            schema_sql, linkml_schema, partition_by, drop_extra_columns, etc.). 
            Only the schema-related fields are used in this function.
        logger (logging.Logger): Logger for reporting resolution decisions.
        minio_client (Minio | None): Placeholder for future LinkML support.

    Returns:
        tuple[str | None, str]:
            - schema_sql (str | None): The resolved SQL-style schema string,
              or None if inferred.
            - schema_source (str): One of:
                - "fallback_sql": LinkML was provided but not supported, SQL used instead.
                - "schema_sql": SQL schema was provided and used.
                - "inferred": No schema provided; caller should infer schema.

    Notes:
        - LinkML parsing is not implemented yet. When a LinkML path is present,
          the function does NOT attempt to parse it and instead falls back to SQL
          or inference. This behavior will change once LinkML support is added.
    """
    schema_sql = table.get("schema_sql")
    linkml_schema = table.get("linkml_schema")

    if linkml_schema:
        msg = (
            f"LinkML schema provided for table '{table.get('name')}', "
            "but LinkML schema parsing is not implemented yet."
        )
        logger.error(msg)
        raise NotImplementedError(msg)
        # TODO: Implement LinkML schema parsing once linkml_parser is ready

    if schema_sql:
        return schema_sql, SchemaSource.SCHEMA_SQL

    return None, SchemaSource.INFERRED


def apply_schema_columns(
    df: DataFrame,
    schema_sql: str | None,
    logger: logging.Logger,
    drop_extra_columns: bool = False,
):
    """
    Align DataFrame columns with a provided SQL-style schema definition.

    This function is intentionally safe to call even when no schema is provided.
    In the ingestion pipeline, `apply_schema_columns()` is invoked unconditionally
    so every table follows the same processing path. When `schema_sql` is None,
    the function becomes a no-op and simply returns the input DataFrame unchanged.

    When a schema string *is* provided, the function attempts to align the
    DataFrame's columns by **renaming** them to match the column names defined in
    `schema_sql`. The function does not reorder columns; it preserves the existing
    DataFrame column order. If extra columns appear in the data, they can
    optionally be dropped before renaming.

    Args:
        df (pyspark.sql.DataFrame): The input DataFrame to adjust.
        schema_sql (str | None): The schema string used for alignment, e.g.
            "id INT, name STRING, age INT". If None, the DataFrame is returned unchanged.
        logger (logging.Logger): Logger for schema application details.
        drop_extra_columns (bool, optional): Whether to drop extra columns not
            present in the schema before renaming. Defaults to False.

    Returns:
        pyspark.sql.DataFrame: A DataFrame with columns renamed (and optionally filtered)
        to align with the given schema.

    Notes:
        - Renaming only occurs when the number of columns matches exactly.
        - If counts differ, a warning is logged and no renaming is performed.
        - This function does not enforce column types, only names.
        - Setting `drop_extra_columns=True` is useful when the source data
          contains additional fields not defined in the target schema.
    """
    if not schema_sql:
        return df

    target_cols = [x.strip().split(" ")[0] for x in schema_sql.split(",")]
    current_cols = df.columns

    # Determine column differences
    extra_cols = [c for c in current_cols if c not in target_cols]
    missing_cols = [c for c in target_cols if c not in current_cols]

    # Log column differences ALWAYS
    if extra_cols or missing_cols:
        logger.warning(
            "Column mismatch detected:\n"
            f"   Extra columns in data: {extra_cols if extra_cols else 'None'}\n"
            f"   Missing columns from data: {missing_cols if missing_cols else 'None'}"
        )

    # Drop extra columns if requested
    if drop_extra_columns and extra_cols:
        logger.info(f"   Dropping extra columns not in schema: {extra_cols}")
        df = df.select(*[c for c in current_cols if c in target_cols])
        current_cols = df.columns  # refresh after projection

    if len(target_cols) == len(current_cols):
        df = df.toDF(*target_cols)
        logger.info(f"   Applied inline schema columns: {', '.join(target_cols)}")
    else:
        logger.warning(
            "Schema column mismatch: "
            f"{len(current_cols)} in data vs {len(target_cols)} in schema. Skipping rename."
        )
    return df
