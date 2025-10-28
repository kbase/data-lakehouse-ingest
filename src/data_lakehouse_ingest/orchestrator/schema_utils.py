"""
File name: src/data_lakehouse_ingest/orchestrator/schema_utils.py

Schema management utilities for the Data Lakehouse Ingest framework.
Handles schema resolution (LinkML or inline SQL) and column alignment for ingested DataFrames.
Provides helpers to enforce consistent structure between raw data and curated Delta tables.
"""
from typing import Tuple
from minio import Minio
from pyspark.sql import SparkSession
import logging

from ..utils.linkml_parser import load_linkml_schema


def resolve_schema(
    spark: SparkSession,
    table: dict,
    logger: logging.Logger,
    minio_client: Minio | None = None,
) -> Tuple[str | None, str]:
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
    schema_source = "inferred"

    if linkml_schema:
        logger.info(f"🧬 Using LinkML schema for table {table.get('name')}"
                    " (takes precedence over schema_sql)")
        try:
            schema_cols = load_linkml_schema(spark, linkml_schema, logger, minio_client=minio_client)
            schema_sql = ", ".join([f"{c} {t}" for c, t in schema_cols.items()])
            logger.info(f"✅ Derived schema_sql from LinkML for {table.get('name')}: {schema_sql}")
            return schema_sql, "linkml"
        except Exception as e:
            logger.error(f"❌ Failed to parse LinkML schema for {table.get('name')}: {e}", exc_info=True)
            if schema_sql:
                logger.warning(f"⚠️ Falling back to inline schema_sql for {table.get('name')}.")
                return schema_sql, "fallback_sql"
            else:
                logger.warning("⚠️ No schema_sql fallback available. Using inferred schema.")
                return None, "inferred"

    if schema_sql:
        return schema_sql, "schema_sql"

    return None, "inferred"


def apply_schema_columns(
    df,
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

    # Optional projection to drop extras if requested
    if drop_extra_columns:
        keep_set = set(target_cols)
        projected = [c for c in current_cols if c in keep_set]
        if projected != current_cols:
            df = df.select(*projected)
            current_cols = df.columns

    if len(target_cols) == len(current_cols):
        df = df.toDF(*target_cols)
        logger.info(f"   Applied inline schema columns: {', '.join(target_cols)}")
    else:
        logger.warning(
            "⚠️ Schema column mismatch: "
            f"{len(current_cols)} in data vs {len(target_cols)} in schema. Skipping rename."
        )
    return df
