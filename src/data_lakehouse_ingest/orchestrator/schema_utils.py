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
):
    """
    Align DataFrame columns using a provided SQL-style schema definition.

    This function standardizes column alignment for ingested DataFrames by
    interpreting the SQL-style schema (schema_sql) as the *authoritative* column
    ordering and set of expected columns. Alignment is performed using
    name-based matching—no positional renaming occurs—ensuring that values from
    the raw data file map to the correct columns even when the file's header
    order differs from the schema definition.

    Behavior overview:
        • When `schema_sql` is None:
            - No schema alignment is performed.
            - The DataFrame is returned unchanged.
            - This corresponds to the “inferred schema” path, where the ingestion
            pipeline relies on Spark's natural column order (from file headers).

        • When `schema_sql` is provided:
            - Column order in the output DataFrame follows the order listed
            in schema_sql.
            - Columns are selected by name (df.select), avoiding silent positional
            mismatches.
            - Extra columns present in the data but not in schema_sql are ignored
            (excluded automatically by the projection).
            - Missing columns cause an immediate failure via ValueError, preventing
            ingestion of incomplete or corrupted datasets.
            - No type enforcement is performed here; only column selection
            and ordering are handled.

    This design ensures that:
        - Users can specify schema_sql columns in any order, independent of the raw
        data file's header order.
        - Column mismatches are detected reliably.
        - No silent column swaps or implicit type changes occur.
        - Schema inference happens only when schema_sql is not defined.

    Args:
        df (pyspark.sql.DataFrame):
            The input DataFrame loaded from raw/bronze data.
        schema_sql (str | None):
            SQL-style schema definition, e.g.,
                "id INT, name STRING, age INT".
            Determines column order and the required set of columns.
            If None, no alignment is applied.
        logger (logging.Logger):
            Logger used to report alignment decisions and mismatch warnings.

    Returns:
        pyspark.sql.DataFrame:
            If schema_sql is provided:
                A DataFrame ordered according to schema_sql and containing only
                those columns.
            If schema_sql is None:
                The original DataFrame, unchanged.
    """
    # No schema provided → return as-is
    if not schema_sql:
        logger.info("No schema_sql provided; skipping schema alignment.")
        return df

    # Extract column names from schema definition
    target_cols = [x.strip().split(" ")[0] for x in schema_sql.split(",")]
    current_cols = df.columns

    # Identify mismatches
    missing_cols = [c for c in target_cols if c not in current_cols]
    extra_cols = [c for c in current_cols if c not in target_cols]

    # Missing columns → raise error to avoid corrupted data
    if missing_cols:
        logger.error(f"Missing required columns for schema alignment: {missing_cols}")
        raise ValueError(f"Cannot apply schema: missing columns: {missing_cols}")

    # Log extra columns (they will be dropped automatically via select)
    if extra_cols:
        logger.warning(
            f"Extra columns in data not present in schema_sql: {extra_cols}. "
            "These columns will be excluded from the output."
        )

    # Name-based projection (safe): keeps only schema columns, in schema order
    df = df.select(*target_cols)

    logger.info(f"Applied name-based schema alignment with columns: {target_cols}")

    return df
