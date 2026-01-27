"""
Delta table column comment utilities.

Provides helpers to apply column-level comments to existing Spark/Delta
tables using a structured list-of-maps schema definition. The module
handles Spark SQL syntax differences across versions, safely escapes
comment strings, and falls back between supported ALTER TABLE forms.
It skips missing or empty comments gracefully and returns a structured
report suitable for logging, auditing, and ingestion metadata.
"""

from __future__ import annotations

import logging
from typing import Any

from pyspark.sql import SparkSession


def _escape_sql_string(s: str) -> str:
    """
    Escape a Python string for safe use inside a Spark SQL string literal.

    Spark SQL uses single quotes for string literals. To safely embed
    user-provided text (such as column comments), any single quote
    characters must be escaped by doubling them.

    Example:
        Input:  "Bob's column"
        Output: "Bob''s column"

    Args:
        s: Raw string value to escape.

    Returns:
        A SQL-safe string with single quotes escaped.
    """
    return s.replace("'", "''")


def _get_table_coltypes(spark: SparkSession, full_table_name: str) -> dict[str, str]:
    """
    Retrieve column data types for an existing Spark table.

    This function queries the table metadata using `DESCRIBE <table>`
    and extracts column names along with their Spark SQL type strings.

    It is primarily used to support legacy `CHANGE COLUMN` syntax,
    which requires explicitly specifying the column's data type when
    modifying column metadata such as comments.

    Non-column rows produced by DESCRIBE (e.g. section headers like
    '# Partitioning' or '# Detailed Table Information') are ignored.

    Args:
        spark: Active SparkSession.
        full_table_name: Fully qualified table name
            (e.g. `catalog.schema.table` or `schema.table`).

    Returns:
        A dictionary mapping column names to Spark SQL type strings.

    Example return value:
        {
            "gene_id": "string",
            "gene_cluster_id": "string",
            "score": "double"
        }
    """
    rows = spark.sql(f"DESCRIBE {full_table_name}").collect()
    coltypes: dict[str, str] = {}

    for r in rows:
        col = (r["col_name"] or "").strip()
        dtype = (r["data_type"] or "").strip()

        # DESCRIBE output contains section headers like '# Partitioning', '# Detailed Table Information'
        if not col or col.startswith("#"):
            continue
        if col.lower() == "partition":
            continue
        if col.startswith("`") and col.endswith("`"):
            col = col[1:-1]

        # Stop when hitting details section (some Spark builds show empty col_name then details)
        if col.lower().startswith("detailed table information"):
            break

        coltypes[col] = dtype

    return coltypes


def _try_alter_column_comment(
    spark: SparkSession,
    full_table_name: str,
    col: str,
    comment: str,
    logger: logging.Logger,
) -> bool:
    """
    Attempt to set a column comment using supported Spark SQL syntaxes.

    This function first attempts the modern syntax:

        ALTER TABLE <table> ALTER COLUMN <col> COMMENT '<comment>'

    If that fails (due to Spark version or distribution limitations),
    it falls back to the more widely supported syntax:

        ALTER TABLE <table>
        CHANGE COLUMN <col> <col> <type> COMMENT '<comment>'

    The fallback requires fetching the column's existing data type.

    Args:
        spark: Active SparkSession.
        full_table_name: Fully qualified table name.
        col: Column name to modify.
        comment: Comment text to apply.
        logger: Logger used for debug, warning, and error messages.

    Returns:
        True if the comment was successfully applied using any method,
        False otherwise.
    """
    c_sql = _escape_sql_string(comment)

    # 1) Try: ALTER TABLE t ALTER COLUMN col COMMENT '...'
    # Some environments support this (esp newer Spark).
    try:
        spark.sql(f"ALTER TABLE {full_table_name} ALTER COLUMN `{col}` COMMENT '{c_sql}'")
        return True
    except Exception as e:
        logger.debug(
            f"ALTER COLUMN COMMENT not supported or failed for {full_table_name}.{col}: {e}"
        )

    try:
        coltypes = _get_table_coltypes(spark, full_table_name)
        if col not in coltypes:
            logger.warning(
                f"Column '{col}' not found in table {full_table_name}; skipping comment."
            )
            return False

        dtype = coltypes[col]

        spark.sql(
            f"ALTER TABLE {full_table_name} CHANGE COLUMN `{col}` `{col}` {dtype} COMMENT '{c_sql}'"
        )
        return True
    except Exception as e:
        logger.error(
            f"Failed to set comment for {full_table_name}.{col}: {e}",
            exc_info=True,
        )
        return False


def apply_comments_from_table_schema(
    spark: SparkSession,
    full_table_name: str,
    table_schema: list[dict[str, Any]],
    logger: logging.Logger | None = None,
    *,
    require_existing_table: bool = True,
) -> dict[str, Any]:
    """
    Apply column comments to a Delta table using a structured schema definition.

    This function iterates over a list-of-maps schema (as used throughout
    the Data Lakehouse Ingest framework) and applies column-level comments
    to an existing Spark/Delta table.

    Only schema entries that define both:
      - a column name (`column` or `name`)
      - a non-empty string `comment`

    will be applied. All other entries are skipped safely.

    Args:
        spark: Active SparkSession.
        full_table_name: Fully qualified table name
            (e.g. `catalog.schema.table`).
        table_schema: List of column definitions, where each item is a dict.
            Example:
                {
                  "column": "gene_id",
                  "type": "string",
                  "nullable": false,
                  "comment": "Unique gene identifier"
                }
        logger: Optional logger for structured logging.
        require_existing_table: If True, verifies that the table exists
            before attempting to apply comments.

    Returns:
        A structured report dictionary containing:
          - table: table name
          - status: success | partial | failed
          - applied: number of comments applied
          - skipped: number of columns skipped
          - failed: number of columns that failed
          - details: per-column result details

    This report is designed to be logged, persisted, or included in
    ingestion metadata for observability and auditing.
    """
    logger = logger or logging.getLogger(__name__)

    # Optional: ensure table exists
    if require_existing_table and not spark.catalog.tableExists(full_table_name):
        msg = f"Table does not exist: {full_table_name}"
        logger.error(msg)
        return {"table": full_table_name, "status": "failed", "error": msg}

    applied = 0
    skipped = 0
    failed = 0
    details: list[dict[str, Any]] = []

    for coldef in table_schema:
        col = coldef.get("column") or coldef.get("name")
        comment = coldef.get("comment")

        if not col:
            skipped += 1
            details.append({"status": "skipped", "reason": "missing column/name", "coldef": coldef})
            continue

        if not isinstance(comment, str) or not comment.strip():
            skipped += 1
            details.append({"status": "skipped", "column": col, "reason": "no comment"})
            continue

        ok = _try_alter_column_comment(spark, full_table_name, col, comment, logger)
        if ok:
            applied += 1
            details.append({"status": "applied", "column": col})
        else:
            failed += 1
            details.append({"status": "failed", "column": col})

    status = "success" if failed == 0 else ("partial" if applied > 0 else "failed")
    return {
        "table": full_table_name,
        "status": status,
        "applied": applied,
        "skipped": skipped,
        "failed": failed,
        "details": details,
    }
