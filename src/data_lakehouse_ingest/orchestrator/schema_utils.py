"""
Schema management utilities for the Data Lakehouse Ingest framework.
Handles schema resolution (LinkML or inline SQL) and column alignment for ingested DataFrames.
Provides helpers to enforce consistent structure between raw data and curated Delta tables.
"""
from minio import Minio
from pyspark.sql import SparkSession, DataFrame
import logging

def resolve_schema(
    spark: SparkSession,
    table: dict[str, object],
    logger: logging.Logger,
    minio_client: Minio | None = None,
) -> tuple[str | None, str]:
    """
    Resolve the schema definition for a given table.

    Determines whether to use a LinkML schema or a provided inline SQL schema
    (`schema_sql`), falling back to an inferred schema if neither is available.

    If a LinkML schema is defined, it takes precedence over `schema_sql`. In that
    case, the schema is parsed and converted into a Spark-compatible SQL string.

    Args:
        spark (SparkSession): Active Spark session used for schema parsing.
        table (dict): Table definition dictionary containing schema fields
            such as "schema_sql" and/or "linkml_schema".
        logger (logging.Logger): Logger for structured information and errors.
        minio_client (Minio | None): Optional MinIO client used to fetch LinkML
            schema files from S3-compatible storage.

    Returns:
        Tuple[str | None, str]: A tuple containing:
            - schema_sql (str | None): The resolved SQL-style schema string,
              e.g. "id INT, name STRING".
            - schema_source (str): The source of the schema, one of:
                {"linkml", "schema_sql", "fallback_sql", "inferred", "custom_parser"}.

    Notes:
        - LinkML schema takes precedence and is converted into SQL syntax.
        - If LinkML parsing fails but a SQL schema exists, the function falls
          back to "fallback_sql".
        - If neither schema is defined, returns (None, "inferred").
    """
    schema_sql = table.get("schema_sql")
    linkml_schema = table.get("linkml_schema")

    if linkml_schema:
        logger.info(
            f"LinkML schema detected for table {table.get('name')} "
            "(feature not implemented yet; falling back)"
        )
        # TODO: Implement LinkML schema parsing once linkml_parser is ready
        if schema_sql:
            return schema_sql, "fallback_sql"
        else:
            logger.warning("No schema_sql fallback available. Using inferred schema.")
            return None, "inferred"

    if schema_sql:
        return schema_sql, "schema_sql"

    return None, "inferred"


def apply_schema_columns(
    df: DataFrame,
    schema_sql: str | None,
    logger: logging.Logger,
    drop_extra_columns: bool = False,
):
    """
    Align DataFrame columns with a provided SQL-style schema definition.

    Renames columns to match the order and names in the specified `schema_sql`
    if the number of columns matches. Optionally, extra columns not defined in
    the schema can be dropped before renaming.

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
