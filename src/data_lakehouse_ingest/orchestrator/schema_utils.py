"""
Schema utilities for the Data Lakehouse Ingest framework.

Provides helpers to resolve table schemas, parse SQL-style schema definitions,
and align DataFrame columns using governed Spark DataTypes, including support
for DECIMAL(p,s), ARRAY<T>, and MAP<K,V> types.
"""

from minio import Minio
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json, to_json
from pyspark.sql.types import (
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    FloatType,
    BooleanType,
    DateType,
    TimestampType,
    DecimalType,
    DataType,
    ArrayType,
    MapType,
    StructType,
)

import logging
from enum import Enum
import re
from dataclasses import dataclass
from typing import Any


def _split_top_level_map_types(inner: str) -> tuple[str, str]:
    """
    Split the inner portion of MAP<K,V> into key and value type strings.

    Splits only on the first comma found at top level, ignoring commas inside
    nested parentheses or angle brackets.

    Example:
        "STRING, ARRAY<DECIMAL(10,2)>"
        -> ("STRING", "ARRAY<DECIMAL(10,2)>")
    """
    depth_paren = 0
    depth_angle = 0

    for i, ch in enumerate(inner):
        if ch == "(":
            depth_paren += 1
        elif ch == ")":
            depth_paren -= 1
        elif ch == "<":
            depth_angle += 1
        elif ch == ">":
            depth_angle -= 1
        elif ch == "," and depth_paren == 0 and depth_angle == 0:
            left = inner[:i].strip()
            right = inner[i + 1 :].strip()

            if not left or not right:
                raise ValueError(f"Invalid MAP<K,V> definition: '{inner}'")

            return left, right

    raise ValueError(f"Invalid MAP<K,V> definition: '{inner}'")


def _to_pyspark_type(dt_raw: str, logger: logging.Logger, *, context: str) -> DataType:
    """
    Convert a type specification string into a PySpark DataType.

    This helper is used by both SQL-style schemas (schema_sql) and structured
    schemas (schema list-of-maps) to produce governed Spark DataTypes.

    Supported forms:
      - Primitive types (case-insensitive):
          STRING, INT, INTEGER, BIGINT, LONG, DOUBLE, FLOAT, BOOLEAN, DATE, TIMESTAMP
      - DECIMAL(p,s):
          e.g., DECIMAL(10,2)
      - ARRAY<T> (recursive, supports nesting):
          e.g., ARRAY<STRING>, ARRAY<DOUBLE>, ARRAY<ARRAY<INT>>
      - MAP<K,V> (recursive, supports nesting):
          e.g., MAP<STRING, INT>, MAP<STRING, ARRAY<DOUBLE>>

    Args:
        dt_raw: The raw type string from config (e.g., "int", "DECIMAL(10,2)", "ARRAY<STRING>").
        logger: Logger used for error reporting.
        context: Short label describing where the type came from (e.g., "schema_sql", "structured schema").

    Returns:
        A PySpark DataType instance corresponding to the provided type string.

    Raises:
        ValueError:
            - If DECIMAL(p,s) syntax is invalid.
            - If ARRAY<T> syntax is invalid.
            - If MAP<K,V> syntax is invalid.
            - If the type is unsupported.
    """
    dt = dt_raw.upper().strip()

    # DECIMAL(p,s)
    if dt.startswith("DECIMAL(") and dt.endswith(")"):
        inner = dt[len("DECIMAL(") : -1]
        try:
            precision_str, scale_str = inner.split(",")
            precision = int(precision_str.strip())
            scale = int(scale_str.strip())
        except ValueError as exc:
            logger.error(f"Invalid DECIMAL definition '{dt_raw}': {exc}")
            raise ValueError(f"Invalid DECIMAL definition '{dt_raw}'") from exc
        return DecimalType(precision=precision, scale=scale)

    # ARRAY<T>
    array_match = re.match(r"^ARRAY<(.+)>$", dt)
    if array_match:
        inner_type_raw = array_match.group(1).strip()
        try:
            inner_type = _to_pyspark_type(inner_type_raw, logger, context=context) # recursive
        except ValueError as exc:
            logger.error(f"Invalid ARRAY definition '{dt_raw}': {exc}")
            raise ValueError(f"Invalid ARRAY definition '{dt_raw}'") from exc
        return ArrayType(inner_type)

    # MAP<K,V>
    map_match = re.match(r"^MAP<(.+)>$", dt)
    if map_match:
        inner = map_match.group(1).strip()
        try:
            key_type_raw, value_type_raw = _split_top_level_map_types(inner)
            key_type = _to_pyspark_type(key_type_raw, logger, context=context)
            value_type = _to_pyspark_type(value_type_raw, logger, context=context)
        except ValueError as exc:
            logger.error(f"Invalid MAP definition '{dt_raw}': {exc}")
            raise ValueError(f"Invalid MAP definition '{dt_raw}'") from exc

        return MapType(key_type, value_type)

    # Primitive types
    mapping: dict[str, DataType] = {
        "STRING": StringType(),
        "INT": IntegerType(),
        "INTEGER": IntegerType(),
        "BIGINT": LongType(),
        "LONG": LongType(),
        "DOUBLE": DoubleType(),
        "FLOAT": FloatType(),
        "BOOLEAN": BooleanType(),
        "DATE": DateType(),
        "TIMESTAMP": TimestampType(),
    }

    if dt not in mapping:
        logger.error(
            f"Unsupported data type '{dt_raw}' in {context} "
            "(expected STRING, INT, INTEGER, BIGINT, LONG, DOUBLE, FLOAT, BOOLEAN, "
            "DATE, TIMESTAMP, DECIMAL(p,s), ARRAY<T>, MAP<K,V>)."
        )
        raise ValueError(f"Unsupported data type '{dt_raw}' in {context}.")

    return mapping[dt]


class SchemaSource(Enum):
    """
    Origin of the resolved schema for a table and how it is used downstream.

    The SchemaSource returned by `resolve_schema()` is used by the ingestion
    orchestrator to determine how strictly a table is governed and which
    schema-related behaviors are applied.

    Downstream usage:
        - Schema enforcement:
            When the resolved schema source is SCHEMA_SQL or SCHEMA_STRUCTURED,
            the normalized schema returned by `resolve_schema()` is passed to
            `apply_schema_columns()` to enforce required columns, drop extra
            columns, cast types, and reorder columns. When the source is
            INFERRED, schema alignment is skipped and Spark’s inferred schema
            is used as-is.

        - Metadata availability:
            Column-level metadata (e.g., comments) may be present when the schema
            is defined using a structured list-of-maps. Metadata application is
            driven by the presence of such metadata in the resolved schema, not
            by the schema source enum itself.

        - Reporting and auditing:
            The schema source is included in per-table ingestion reports so
            that each run explicitly records whether the table was governed
            via structured schema, SQL-style schema, or inference.

    Values:
        SCHEMA_SQL:
            Schema provided via `schema_sql` (SQL-style
            "col TYPE, col TYPE, ...").

        SCHEMA_STRUCTURED:
            Schema provided via `schema` as a structured list-of-maps
            (e.g., [{"column": "id", "type": "INT"}, ...]).

        INFERRED:
            No explicit schema provided; Spark infers the schema from
            the source files.
    """

    SCHEMA_SQL = "schema_sql"
    SCHEMA_STRUCTURED = "schema"
    INFERRED = "inferred"


NormalizedSchema = list[tuple[str, DataType]]


@dataclass(frozen=True)
class ResolvedSchema:
    """
    Result of resolving a table schema for downstream orchestration.

    - schema_defs: normalized (name, DataType) tuples used for schema enforcement
    - schema_source: enum used for reporting/auditing
    - comment_metadata: minimized list-of-maps containing only comment info for delta_comments
    """

    schema_defs: NormalizedSchema | None
    schema_source: SchemaSource
    comment_metadata: list[dict[str, Any]] | None


def resolve_schema(
    spark: SparkSession,
    table: dict[str, object],
    logger: logging.Logger,
    minio_client: Minio | None = None,
) -> ResolvedSchema:
    """
    Resolve the schema for a table and return both a normalized schema and its origin.

    This function normalizes either supported schema representation into a common
    `NormalizedSchema` form (list of (column_name, Spark DataType) tuples), and returns a
    `SchemaSource` enum that downstream orchestration uses to decide how to handle the
    table (e.g., whether to apply schema alignment and whether column comments are
    eligible to be applied from structured schema metadata).

    Behavior and validation (LinkML not yet supported):
        1) LinkML:
           - If `linkml_schema` is provided, the function logs an error and raises
             NotImplementedError. There is no fallback when LinkML is present.

        2) Structured schema (`schema`):
           - If `schema` is provided as a **non-empty** list-of-maps, it is parsed
             via `parse_schema_structured()` and returned as normalized
             `(name, DataType)` tuples with `SchemaSource.SCHEMA_STRUCTURED`.

        3) SQL-style schema (`schema_sql`):
           - If `schema` is absent/empty and `schema_sql` is a non-empty string, it is
             parsed via `parse_schema_sql()` and returned as normalized tuples with
             `SchemaSource.SCHEMA_SQL`.

        4) Invalid combination:
           - If both `schema` and `schema_sql` are provided in the same table
             definition, this function raises `ValueError`.

        5) Inferred:
           - If neither a non-empty structured schema nor a non-empty `schema_sql` is
             provided, this function returns `(None, SchemaSource.INFERRED)`. Downstream
             code may rely on Spark’s inferred schema for loading and column order.

    Validation note:
        - In the ingest pipeline, config is validated by `ConfigLoader` before this
          function runs (e.g., `schema` must be a list when provided, `schema_sql` must be
          a string when provided). This function assumes config is validated upstream,
          but also performs defensive type checks and fails fast if invalid schema
          definitions are encountered.


    Args:
        spark (SparkSession):
            Active Spark session (currently unused until LinkML parsing is implemented).
        table (dict[str, object]):
            Table definition from the ingestion config. Only schema-related keys are used
            here: `schema`, `schema_sql`, and `linkml_schema`.
        logger (logging.Logger):
            Logger for reporting schema resolution decisions.
        minio_client (Minio | None):
            Placeholder for future MinIO-based schema retrieval (not currently used).

    Returns:
        tuple[NormalizedSchema | None, SchemaSource]:
            - schema_defs:
                Normalized schema (list of (column_name, DataType) tuples) when the
                source is SCHEMA_STRUCTURED or SCHEMA_SQL; otherwise None.
            - schema_source:
                Enum indicating schema origin: SCHEMA_STRUCTURED, SCHEMA_SQL, or INFERRED.

    Raises:
        NotImplementedError:
            If `linkml_schema` is provided (LinkML parsing not implemented yet).
    """
    schema_sql = table.get("schema_sql")
    schema = table.get("schema")
    linkml_schema = table.get("linkml_schema")

    # LinkML schemas are not supported yet; fail fast if provided
    if linkml_schema:
        msg = (
            f"LinkML schema provided for table '{table.get('name')}', "
            "but LinkML schema parsing is not implemented yet."
        )
        logger.error(msg)
        raise NotImplementedError(msg)
        # TODO: Implement LinkML schema parsing once linkml_parser is ready

    # Defensive check: schema must be a list if present
    if schema is not None and not isinstance(schema, list):
        raise ValueError(
            f"Invalid schema definition for table '{table.get('name')}': "
            f"'schema' must be a list of column definitions, got {type(schema).__name__}."
        )

    # Defensive check: schema_sql must be a string if present
    if schema_sql is not None and not isinstance(schema_sql, str):
        raise ValueError(
            f"Invalid schema definition for table '{table.get('name')}': "
            f"'schema_sql' must be a string, got {type(schema_sql).__name__}."
        )

    # Disallow providing both schema and schema_sql
    if schema is not None and schema_sql is not None:
        raise ValueError(
            f"Invalid schema definition for table '{table.get('name')}': "
            "both 'schema' and 'schema_sql' are defined. Please provide only one."
        )

    # Structured schema takes precedence and is normalized immediately
    if schema:
        logger.info(f"Using structured schema for table {table.get('name')}")

        comment_metadata = [
            {"column": coldef["column"], "comment": coldef.get("comment")} for coldef in schema
        ]

        return ResolvedSchema(
            schema_defs=parse_schema_structured(schema, logger),
            schema_source=SchemaSource.SCHEMA_STRUCTURED,
            comment_metadata=comment_metadata or None,
        )

    # SQL-style schema is parsed into the same normalized representation
    if schema_sql and schema_sql.strip():
        logger.info(f"Using schema_sql for table {table.get('name')}")
        return ResolvedSchema(
            schema_defs=parse_schema_sql(schema_sql, logger),
            schema_source=SchemaSource.SCHEMA_SQL,
            comment_metadata=None,
        )

    # No explicit schema → downstream code relies on Spark inference
    logger.info(f"No schema provided for table {table.get('name')}; using inferred schema")
    return ResolvedSchema(
        schema_defs=None,
        schema_source=SchemaSource.INFERRED,
        comment_metadata=None,
    )


def apply_schema_columns(
    df: DataFrame,
    schema_defs: list[tuple[str, DataType]] | None,
    logger: logging.Logger,
) -> tuple[DataFrame, dict[str, list[str]]]:
    """
    Align DataFrame columns using a normalized schema definition.

    This function treats `schema_defs`—a normalized list of
    (column_name, Spark DataType) tuples—as the authoritative definition
    of the DataFrame's final structure. It enforces both column *order*
    and column *data types*, ensuring curated Delta tables follow a
    consistent, governed schema.

    The normalized schema is produced upstream by `resolve_schema()`
    (via `parse_schema_sql()` or `parse_schema_structured()`), so this
    function is intentionally agnostic to the original configuration
    representation (SQL-style schema string vs structured list-of-maps).

    The function performs the following operations when a schema is provided:

      1. **Validation**
         - Ensures every column declared in `schema_defs` exists in the
           input DataFrame.
         - Raises ValueError if any required column is missing
           (fail-fast behavior).

      2. **Column pruning**
         - Drops extra columns present in the input data but not declared
           in the schema.

      3. **Type enforcement (casting)**
         - Casts each column to its declared Spark DataType.
         - Supports primitive and complex types, including:
           STRING, INT/INTEGER, BIGINT/LONG, DOUBLE, FLOAT, BOOLEAN,
           DATE, TIMESTAMP, DECIMAL(p,s), ARRAY<T>, MAP<K,V>.

      4. **Array conversion**
         - For ARRAY<T> and MAP<K,V> columns, converts JSON-encoded string values
           into proper Spark complex types using `from_json`.
         - Supports nested array/map combinations.
         - Note: Input values for ARRAY<T> are expected to be JSON strings;
           other encodings are not automatically converted.

      5. **Ordered projection**
         - Projects only schema-defined columns, in the exact order
           specified by `schema_defs`.

    Behavior overview:
        • When `schema_defs` is None or empty:
            - No schema alignment is performed.
            - The DataFrame is returned unchanged.
            - This corresponds to the inferred schema path, where the ingestion
              pipeline relies on Spark's natural column order.

        • When `schema_defs` is provided:
            - Validates required columns.
            - Drops extra columns.
            - Casts columns to their declared types.
            - Reorders columns to match the declared schema.

    This design ensures that:
        - Schema enforcement is deterministic and explicit.
        - Column mismatches are detected early.
        - No silent column reordering or implicit type changes occur.
        - Schema inference is used only when no explicit schema is supplied.

    Args:
        df (pyspark.sql.DataFrame):
            The input DataFrame loaded from raw/bronze data.
        schema_defs (list[tuple[str, DataType]] | None):
            Normalized schema definition produced by schema resolution.
            If None or empty, schema alignment is skipped.
        logger (logging.Logger):
            Logger used to report alignment decisions and mismatch warnings.

    Returns:
         tuple[pyspark.sql.DataFrame, dict[str, list[str]]]:
            A tuple containing:
            - DataFrame:
                Aligned DataFrame when a schema is provided, otherwise
                the original DataFrame.
            - dict:
                Metadata including:
                - "dropped_columns": list of column names that were present
                  in the input data but not declared in the schema.
    """
    if not schema_defs:
        logger.info("No schema provided; skipping schema alignment.")
        return df, {"dropped_columns": []}

    target_cols = [name for name, _ in schema_defs]
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
            f"Extra columns in data not present in declared schema: {extra_cols}. "
            "These columns will be excluded from the output."
        )

    # Enforce ordering + type casting
    projected_cols = []
    source_types = {field.name: field.dataType for field in df.schema.fields}

    for col_name, col_type in schema_defs:
        source_type = source_types.get(col_name)

        if isinstance(col_type, (ArrayType, MapType)):
            if isinstance(source_type, StringType):
                projected_cols.append(from_json(col(col_name), col_type).alias(col_name))
            elif isinstance(source_type, StructType) and isinstance(col_type, MapType):
                projected_cols.append(from_json(to_json(col(col_name)), col_type).alias(col_name))
            else:
                projected_cols.append(col(col_name).cast(col_type).alias(col_name))
        else:
            projected_cols.append(col(col_name).cast(col_type).alias(col_name))

    # Name-based projection (safe): keeps only schema columns, in schema order
    df = df.select(*projected_cols)

    logger.info(f"Applied name-based schema alignment with columns: {target_cols}")
    return df, {"dropped_columns": extra_cols}


def parse_schema_sql(schema_sql: str, logger: logging.Logger) -> list[tuple[str, DataType]]:
    """
    Parse a SQL-style schema definition into a structured list of
    (column_name, DataType) tuples.

    The function expects `schema_sql` to contain one or more column
    specifications separated by commas. Each specification must follow the form:

        <column_name> <data_type>

    Example:
        "id INT, name STRING, score DOUBLE"

    is parsed into:

        [("id", IntegerType()), ("name", StringType()), ("score", DoubleType())]

    Supported data types:
        • Primitive Spark types:
              STRING, INT, INTEGER, BIGINT, LONG, DOUBLE, FLOAT,
              BOOLEAN, DATE, TIMESTAMP
        • DECIMAL(p,s) with validated precision/scale
        • ARRAY<T> with recursive parsing, including nested arrays
              ARRAY<STRING>
              ARRAY<DOUBLE>
              ARRAY<ARRAY<INT>>
        • MAP<K,V> with recursive parsing, including nested maps/arrays
              MAP<STRING, INT>
              MAP<STRING, ARRAY<DOUBLE>>
              ARRAY<MAP<STRING, INT>>

    Parsing behavior:
        • Leading/trailing whitespace around tokens is ignored.
        • Data types are normalized to uppercase.
        • A definition must contain exactly two tokens: <name> <type>.
        • Additional trailing tokens are not allowed (fail-fast behavior).
        • Invalid or unsupported data types raise ValueError with a
          descriptive error message.

    Args:
        schema_sql (str):
            A comma-separated SQL-style schema definition.
        logger (logging.Logger):
            Logger used for parse diagnostics and error reporting.

    Returns:
        list[tuple[str, DataType]]:
            A list of (column_name, SparkDataType) tuples in the order they
            appear in the schema_sql string.

    Raises:
        ValueError:
            • If any column definition is malformed.
            • If a data type is unsupported.
            • If DECIMAL(p,s) ARRAY<T>, or MAP<K,V> syntax is invalid.
            • If schema_sql contains unmatched or unclosed delimiters in types (e.g., DECIMAL(...), ARRAY<...>).

    Examples:
        >>> parse_schema_sql("id INT, name STRING")
        [('id', IntegerType()), ('name', StringType())]

        >>> parse_schema_sql("values ARRAY<DOUBLE>")
        [('values', ArrayType(DoubleType()))]

        >>> parse_schema_sql("amount DECIMAL(10,2)")
        [('amount', DecimalType(10, 2))]

        >>> parse_schema_sql("invalid")
        ValueError: Invalid column definition in schema_sql: 'invalid'
    """

    def _split_schema_sql_defs(schema_sql: str) -> list[str]:
        """
        Split a SQL-style schema string into column definitions.

        Commas are treated as separators **only at top level**; commas inside
        DECIMAL(p,s), ARRAY<T>, or MAP<K,V> (including nested complex types) are ignored.

        Args:
            schema_sql (str): SQL-style schema definition string.

        Returns:
            list[str]: Ordered list of column definition segments.
        """
        parts: list[str] = []
        depth_paren = 0
        depth_angle = 0

        start = 0  # start index of the current segment

        for i, ch in enumerate(schema_sql):
            if ch == "(":
                depth_paren += 1
            elif ch == ")":
                depth_paren -= 1
                if depth_paren < 0:
                    logger.error(f"Unmatched ')' at position {i} in schema_sql: {schema_sql!r}")
                    raise ValueError(f"Invalid schema_sql: unmatched ')' at position {i}.")
            elif ch == "<":
                depth_angle += 1
            elif ch == ">":
                depth_angle -= 1
                if depth_angle < 0:
                    logger.error(f"Unmatched '>' at position {i} in schema_sql: {schema_sql!r}")
                    raise ValueError(f"Invalid schema_sql: unmatched '>' at position {i}.")

            # split only on commas at top level
            if ch == "," and depth_paren == 0 and depth_angle == 0:
                segment = schema_sql[start:i].strip()
                parts.append(segment)
                start = i + 1  # next segment starts after the comma

        if depth_paren != 0:
            logger.error(f"Unclosed '(' in schema_sql: {schema_sql!r}")
            raise ValueError("Invalid schema_sql: unclosed '(' in type definition.")
        if depth_angle != 0:
            logger.error(f"Unclosed '<' in schema_sql: {schema_sql!r}")
            raise ValueError("Invalid schema_sql: unclosed '<' in type definition.")

        # last segment
        parts.append(schema_sql[start:].strip())
        return parts

    logger.info(f"Parsing SQL schema definition: {schema_sql}")

    columns: list[tuple[str, DataType]] = []
    for raw_def in _split_schema_sql_defs(schema_sql):
        col_def = raw_def.strip()
        if not col_def:
            # Empty segment (e.g., trailing comma) → fail fast
            logger.error(f"Empty column definition in schema_sql: '{schema_sql}'")
            raise ValueError(f"Empty column definition in schema_sql: '{schema_sql}'")

        logger.debug(f"Processing schema column definition: '{col_def}'")
        parts = col_def.split()

        if len(parts) != 2:
            logger.error(
                f"Invalid column definition in schema_sql: '{col_def}'. "
                "Expected exactly two tokens: <name> <type>."
            )
            raise ValueError(
                f"Invalid column definition in schema_sql: '{col_def}'. "
                "Expected format: '<column_name> <data_type>'."
            )

        col_name, col_type_raw = parts[0], parts[1]
        dtype = _to_pyspark_type(col_type_raw, logger, context="schema_sql")

        columns.append((col_name.strip(), dtype))
        logger.debug(f"Parsed column: name='{col_name}', type='{dtype.simpleString()}'")

    logger.info(f"Successfully parsed {len(columns)} columns from schema_sql.")
    return columns


def parse_schema_structured(
    schema: list[dict[str, object]],
    logger: logging.Logger,
) -> list[tuple[str, DataType]]:
    """
    Parse a structured schema (list-of-maps) into a list of (column_name, DataType) tuples.

    Expected input format:
        [
          {"column": "id", "type": "INT", "nullable": false, "comment": "Primary key"},
          {"column": "gene_id", "type": "STRING"},
          ...
        ]

    Key rules:
      - Each entry must be a dict/map.
      - Only the following keys are allowed: "column", "type", "nullable", "comment".
      - Column name must be provided under "column".
      - Type must be provided under "type".
      - "nullable" (bool) and "comment" (string) are optional when provided.
      - Types support the same set as SQL schemas via _to_pyspark_type(...):
        primitives, DECIMAL(p,s), ARRAY<T>, and MAP<K,V> (including nested complex types)

    Args:
        schema: Structured schema list-of-maps.
        logger: Logger used for parse diagnostics.

    Returns:
        List of (column_name, Spark DataType) tuples, in the same order as the input list.

    Raises:
        ValueError:
            - If schema is not a list.
            - If any entry is not a dict.
            - If required keys are missing ("column", "type").
            - If unsupported keys are present (keys other than "column", "type", "nullable", "comment").
            - If "nullable" is present and not a bool.
            - If "comment" is present and not a string (or None).
            - If a type is invalid or unsupported.
    """
    if not isinstance(schema, list):
        raise ValueError(f"Structured schema must be a list, got {type(schema).__name__}.")

    allowed_keys = {"column", "type", "nullable", "comment"}

    cols: list[tuple[str, DataType]] = []
    for i, coldef in enumerate(schema):
        if not isinstance(coldef, dict):
            raise ValueError(f"Invalid schema entry at index {i}: expected object/map")

        # Reject unknown keys (strict contract)
        extra_keys = set(coldef.keys()) - allowed_keys
        if extra_keys:
            raise ValueError(
                f"Schema entry at index {i} has unsupported keys {sorted(extra_keys)}; "
                f"allowed keys are {sorted(allowed_keys)}."
            )

        # Required: column
        if "column" not in coldef:
            raise ValueError(f"Schema entry at index {i} missing required key 'column'")

        col_raw = coldef["column"]

        if not isinstance(col_raw, str):
            raise ValueError(
                f"Schema entry at index {i} has invalid 'column' "
                f"(expected string, got {type(col_raw).__name__})"
            )

        col_name = col_raw.strip()
        if not col_name:
            raise ValueError(f"Schema entry at index {i} has empty 'column' value")

        # Required: type
        if "type" not in coldef:
            raise ValueError(f"Schema entry for '{col_name}' missing required key 'type'")

        type_raw = coldef["type"]
        if not isinstance(type_raw, str):
            raise ValueError(
                f"Schema entry for '{col_name}' has invalid 'type' "
                f"(expected string, got {type(type_raw).__name__})"
            )
        typ_raw = type_raw.strip()
        if not typ_raw:
            raise ValueError(f"Schema entry for '{col_name}' has empty 'type' value")

        # Optional: nullable
        if "nullable" in coldef and not isinstance(coldef["nullable"], bool):
            raise ValueError(
                f"Schema entry for '{col_name}' has invalid 'nullable' "
                f"(expected bool, got {type(coldef['nullable']).__name__})"
            )

        # Optional: comment
        if "comment" in coldef:
            comment_val = coldef["comment"]
            if comment_val is not None and not isinstance(comment_val, str):
                raise ValueError(
                    f"Schema entry for '{col_name}' has invalid 'comment' "
                    f"(expected string, got {type(comment_val).__name__})"
                )

        dtype = _to_pyspark_type(typ_raw, logger, context="structured schema")
        cols.append((col_name, dtype))

        logger.debug(f"Parsed column: name='{col_name}', type='{dtype.simpleString()}'")

    logger.info(f"Successfully parsed {len(cols)} columns from structured schema.")
    return cols
