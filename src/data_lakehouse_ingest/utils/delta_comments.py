"""
Delta table and column comment utilities.

Provides helpers to apply table-level and column-level comments to existing
Spark/Delta tables using structured metadata. The module supports both plain
string comments and JSON-style dict comments, safely escapes comment strings,
skips missing or empty comments gracefully, and returns structured reports
suitable for logging, auditing, and ingestion metadata.
"""

import logging
import json
from typing import Any

from pyspark.sql import SparkSession


def _escape_sql_string(s: str) -> str:
    """
    Escape a Python string for safe use inside a double-quoted Spark SQL string literal.

    This module applies comments using SQL of the form:

        COMMENT "..."

    Therefore, the only characters that must be escaped inside the comment
    text are:

      - backslash: \\
      - double quote: "

    Single quotes (apostrophes) do NOT need escaping for this syntax.

    Examples:
        Input:  Organism's taxonomic prefix
        Output: Organism's taxonomic prefix

        Input:  Column named "status"
        Output: Column named \\"status\\"

        Input:  Path is C:\\data\\input
        Output: Path is C:\\\\data\\\\input

    Args:
        s: Raw comment string.

    Returns:
        SQL-safe string for use inside COMMENT "..."
    """
    return s.replace("\\", "\\\\").replace('"', '\\"')


def _try_alter_column_comment(
    spark: SparkSession,
    full_table_name: str,
    col: str,
    comment: str,
    logger: logging.Logger,
) -> bool:
    """
    Attempt to set a column comment using Spark SQL.

    Uses the `ALTER TABLE ... ALTER COLUMN ... COMMENT` syntax to update
    column-level metadata for an existing Spark/Delta table.

    Args:
        spark: Active SparkSession.
        full_table_name: Fully qualified table name.
        col: Column name to modify.
        comment: Comment text to apply.
        logger: Logger used for debug, warning, and error messages.

    Returns:
        True if the comment was successfully applied,
        False otherwise.
    """
    c_sql = _escape_sql_string(comment)

    try:
        spark.sql(f'ALTER TABLE {full_table_name} ALTER COLUMN `{col}` COMMENT "{c_sql}"')
        return True
    except Exception:
        logger.exception(f"Failed to set comment for {full_table_name}.{col}")
        return False


def _try_set_table_comment(
    spark: SparkSession,
    full_table_name: str,
    comment: str,
    logger: logging.Logger,
) -> bool:
    """
    Attempt to set a table-level comment using Spark SQL.

    This function first tries `COMMENT ON TABLE ... IS ...`. If that fails,
    it falls back to `ALTER TABLE ... SET TBLPROPERTIES ("comment" = ...)`
    for compatibility with environments where direct table comment syntax
    is unsupported.

    Args:
        spark: Active SparkSession.
        full_table_name: Fully qualified table name.
        comment: Comment text to apply.
        logger: Logger used for warning and error messages.

    Returns:
        True if the table comment was successfully applied,
        False otherwise.
    """
    c_sql = _escape_sql_string(comment)
    try:
        spark.sql(f'COMMENT ON TABLE {full_table_name} IS "{c_sql}"')
        return True
    except Exception:
        logger.warning(
            f"COMMENT ON TABLE failed for {full_table_name}; trying TBLPROPERTIES fallback",
        )

    try:
        spark.sql(f'ALTER TABLE {full_table_name} SET TBLPROPERTIES ("comment" = "{c_sql}")')
        return True
    except Exception:
        logger.exception(f"Failed to set table comment for {full_table_name}")
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
      - a non-empty `comment`

    will be applied. All other entries are skipped safely.

    Supported comment formats:
      - Plain string comments
      - JSON-style dict comments, which are serialized to JSON before being stored

     Unicode characters in comments are preserved.

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

            Dict-based comments are also supported, for example:
                {
                  "column": "species",
                  "type": "string",
                  "comment": {
                      "description": "Scientific name",
                      "source": "/api/v1/species"
                  }
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

        if isinstance(comment, dict):
            comment = json.dumps(comment, ensure_ascii=False)

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


def apply_table_comment(
    spark: SparkSession,
    full_table_name: str,
    table_comment: str | dict[str, Any] | None,
    logger: logging.Logger | None = None,
    *,
    require_existing_table: bool = True,
) -> dict[str, Any]:
    logger = logger or logging.getLogger(__name__)

    if require_existing_table and not spark.catalog.tableExists(full_table_name):
        msg = f"Table does not exist: {full_table_name}"
        logger.error(msg)
        return {"table": full_table_name, "status": "failed", "error": msg}

    if isinstance(table_comment, dict):
        table_comment = json.dumps(table_comment, ensure_ascii=False)

    if not isinstance(table_comment, str) or not table_comment.strip():
        return {
            "table": full_table_name,
            "status": "skipped",
            "reason": "no table comment",
        }

    ok = _try_set_table_comment(spark, full_table_name, table_comment, logger)

    return {
        "table": full_table_name,
        "status": "success" if ok else "failed",
        "applied": bool(ok),
    }
