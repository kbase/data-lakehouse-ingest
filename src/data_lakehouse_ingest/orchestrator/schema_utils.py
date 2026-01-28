"""
Schema utilities for the Data Lakehouse Ingest framework.

Provides helpers to resolve table schemas, parse SQL-style schema definitions,
and align DataFrame columns using governed Spark DataTypes, including support
for DECIMAL(p,s) and ARRAY<T> types.
"""

from minio import Minio
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, from_json
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
)

import logging
from enum import Enum
import re


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
        except Exception as exc:
            logger.error(f"Invalid DECIMAL definition '{dt_raw}': {exc}")
            raise ValueError(f"Invalid DECIMAL definition '{dt_raw}'") from exc
        return DecimalType(precision=precision, scale=scale)

    # ARRAY<T>
    array_match = re.match(r"^ARRAY<(.+)>$", dt)
    if array_match:
        inner_type_raw = array_match.group(1).strip()
        inner_type = _to_pyspark_type(inner_type_raw, logger, context=context)  # recursive
        return ArrayType(inner_type)

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
            "DATE, TIMESTAMP, DECIMAL(p,s), ARRAY<T>)."
        )
        raise ValueError(f"Unsupported data type '{dt_raw}' in {context}.")

    return mapping[dt]


class SchemaSource(Enum):
    """
    Origin of the resolved schema for a table.

    Values:
        SCHEMA_SQL:
            Schema provided via `schema_sql` (SQL-style "col TYPE, col TYPE, ...").
        SCHEMA_STRUCTURED:
            Schema provided via `schema` as a structured list-of-maps
            (e.g., [{"column": "id", "type": "INT"}, ...]).
        INFERRED:
            No explicit schema provided; Spark infers schema from the source files.
    """

    SCHEMA_SQL = "schema_sql"
    SCHEMA_STRUCTURED = "schema"
    INFERRED = "inferred"


def resolve_schema(
    spark: SparkSession,
    table: dict[str, object],
    logger: logging.Logger,
    minio_client: Minio | None = None,
) -> tuple[object | None, SchemaSource]:
    """
    Resolve the schema definition for a given table.

    Current behavior (LinkML not yet supported):
        - If a LinkML schema path is provided, the function logs an error and
          raises NotImplementedError. There is **no** automatic fallback to
          structured or SQL schemas when LinkML is present.
        - If `schema` is provided as a non-empty list, it is returned and marked
          as SchemaSource.SCHEMA_STRUCTURED.
        - If `schema_sql` is provided, it is returned and marked as
          SchemaSource.SCHEMA_SQL.
        - If neither is provided, the schema is treated as inferred and marked
          as SchemaSource.INFERRED.

    Args:
        spark (SparkSession): Active Spark session (unused until LinkML is implemented).
        table (dict): Full table definition from the ingestion config.
            This dict may include many fields (e.g., name, bronze_path, enabled,
            schema, schema_sql, linkml_schema, partition_by, drop_extra_columns, etc.).
            Only the schema-related fields are used in this function.
        logger (logging.Logger): Logger for reporting resolution decisions.
        minio_client (Minio | None): Placeholder for future MinIO-based schema retrieval.
            In the future, callers may want to supply MinIO paths to SQL DDL files,
            JSON Schemas, or other schema formats stored in MinIO.

    Returns:
        tuple[str | None, SchemaSource]:
            - schema_def (object | None):
                * list[dict] when SchemaSource.SCHEMA_STRUCTURED
                * str when SchemaSource.SCHEMA_SQL
                * None when SchemaSource.INFERRED
            - schema_source (SchemaSource):
                Enum indicating where the schema came from.

    Notes:
        - LinkML parsing is not implemented yet. When a LinkML path is present,
          the function logs an error and raises NotImplementedError. Once
          LinkML support is added, this behavior may change to parse the
          LinkML schema and return a corresponding SQL-style definition.
    """
    schema_sql = table.get("schema_sql")
    schema = table.get("schema")
    linkml_schema = table.get("linkml_schema")

    if linkml_schema:
        msg = (
            f"LinkML schema provided for table '{table.get('name')}', "
            "but LinkML schema parsing is not implemented yet."
        )
        logger.error(msg)
        raise NotImplementedError(msg)
        # TODO: Implement LinkML schema parsing once linkml_parser is ready

    if isinstance(schema, list) and schema:
        logger.info(f"Using structured schema for table {table.get('name')}")
        return schema, SchemaSource.SCHEMA_STRUCTURED

    if isinstance(schema_sql, str) and schema_sql.strip():
        logger.info(f"Using schema_sql for table {table.get('name')}")
        return schema_sql, SchemaSource.SCHEMA_SQL

    logger.info(f"No schema provided for table {table.get('name')}; using inferred schema")
    return None, SchemaSource.INFERRED


def apply_schema_columns(
    df: DataFrame,
    schema_def: object | None,
    schema_source: SchemaSource,
    logger: logging.Logger,
) -> tuple[DataFrame, dict[str, list[str]]]:
    """
    Align DataFrame columns using a provided SQL-style schema definition.

    This function treats `schema_sql` as the authoritative definition of the
    DataFrame's final structure. It enforces both column *order* and column
    *data types*, ensuring that curated Delta tables follow a consistent,
    governed schema. The function performs four key operations:

      1. **Validation**
         - Ensures every column declared in schema_sql exists in the input DataFrame.
         - Raises ValueError on any missing required column (fail-fast behavior).

      2. **Column pruning**
         - Drops extra columns that appear in the input data but not in schema_sql.

      3. **Type enforcement (casting)**
         - Each column is cast to its declared Spark DataType.
         - Supports full PySpark types:
            STRING, INTEGER, BIGINT, DOUBLE, FLOAT, BOOLEAN,
            DATE, TIMESTAMP, DECIMAL(p,s), ARRAY<T>
         - For ARRAY<T> types, the function uses:
                from_json(col(col_name), ArrayType(innerType))
            to convert JSON-encoded arrays to Spark arrays.

      4. **Array Conversion**
         - For ARRAY<T> columns, the function converts *JSON-encoded string*
           values into proper Spark arrays using:
               from_json(col(col_name), ArrayType(innerType))
         - Supports nested array element types (e.g., ARRAY<ARRAY<INT>>)
           as produced by `parse_schema_sql()`.
         - Note: The function expects the input column for ARRAY<T> to contain
           JSON strings. Non-JSON formats (e.g., PostgreSQL-style "{1,2}") are
           not automatically converted.

      5. **Ordered projection**
         - The output DataFrame contains only the schema_sql columns,
           in the exact order they were declared.

    Behavior overview:
        • When the schema is inferred:
            - No schema alignment is performed.
            - The DataFrame is returned unchanged.
            - This corresponds to the inferred schema path, where the ingestion
              pipeline relies on Spark's natural column order.

        • When an explicit schema is provided:
            - Validates required columns.
            - Drops extra columns.
            - Casts columns to their declared types.
            - Reorders columns to match the declared schema.

    This design ensures that:
        - Users can specify schema_sql columns in any order, independent of the raw
        data file's header order.
        - Column mismatches are detected reliably.
        - No silent column swaps or implicit type changes occur.
        - Schema inference happens only when schema_sql is not defined.

    Args:
        df (pyspark.sql.DataFrame):
            The input DataFrame loaded from raw/bronze data.
        schema_def (object | None):
            Schema definition:
              - str when schema_source is SCHEMA_SQL
              - list when schema_source is SCHEMA_STRUCTURED
              - None when schema_source is INFERRED
        schema_source (SchemaSource):
            Indicates how the schema_def should be interpreted.
        logger (logging.Logger):
            Logger used to report alignment decisions and mismatch warnings.

    Returns:
         tuple[pyspark.sql.DataFrame, dict[str, list[str]]]:
            A tuple containing:
            - DataFrame:
                If schema_sql is provided, a DataFrame ordered according to
                schema_sql and containing only those columns.
                If schema_sql is None, the original DataFrame, unchanged.
            - dict:
                Metadata about schema application, currently including:
                - "dropped_columns": list of column names that were present
                in the input data but not defined in schema_sql.
    """
    # No schema provided → return as-is
    if not schema_def or schema_source == SchemaSource.INFERRED:
        logger.info("No schema provided; skipping schema alignment.")
        return df, {"dropped_columns": []}

    # Parse SQL schema into structured representation
    if schema_source == SchemaSource.SCHEMA_SQL:
        if not isinstance(schema_def, str):
            raise ValueError("Expected schema_def to be a string for SCHEMA_SQL.")
        schema_defs = parse_schema_sql(schema_def, logger)

    elif schema_source == SchemaSource.SCHEMA_STRUCTURED:
        if not isinstance(schema_def, list):
            raise ValueError("Expected schema_def to be a list for SCHEMA_STRUCTURED.")
        schema_defs = parse_schema_structured(schema_def, logger)

    else:
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

    for col_name, col_type in schema_defs:
        if isinstance(col_type, ArrayType):
            # Correct way to turn JSON string -> Spark ARRAY
            projected_cols.append(from_json(col(col_name), col_type).alias(col_name))
        else:
            # Normal scalar cast
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
            • If DECIMAL(p,s) or ARRAY<T> syntax is invalid.

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

    logger.info(f"Parsing SQL schema definition: {schema_sql}")

    columns: list[tuple[str, DataType]] = []
    for raw_def in schema_sql.split(","):
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
          {"column": "id", "type": "INT"},
          {"name": "gene_id", "type": "STRING"},
          ...
        ]

    Key rules:
      - Each entry must be a dict/map.
      - Column name is taken from "column" or "name".
      - Type must be present under "type".
      - Types support the same set as SQL schemas via _to_pyspark_type(...):
          primitives, DECIMAL(p,s), ARRAY<T> (including nested arrays)

    Args:
        schema: Structured schema list-of-maps.
        logger: Logger used for parse diagnostics.

    Returns:
        List of (column_name, Spark DataType) tuples, in the same order as the input list.

    Raises:
        ValueError:
            - If schema is not a list.
            - If any entry is not a dict.
            - If required keys are missing ("column"/"name", "type").
            - If a type is invalid or unsupported.
    """
    if not isinstance(schema, list):
        raise ValueError(f"Structured schema must be a list, got {type(schema).__name__}.")

    cols: list[tuple[str, DataType]] = []
    for i, coldef in enumerate(schema):
        if not isinstance(coldef, dict):
            raise ValueError(f"Invalid schema entry at index {i}: expected object/map")

        col_name = coldef.get("column") or coldef.get("name")
        if not col_name:
            raise ValueError(f"Schema entry at index {i} missing 'column'/'name'")

        typ = coldef.get("type")
        if not typ:
            raise ValueError(f"Schema entry for '{col_name}' missing 'type'")

        dtype = _to_pyspark_type(str(typ), logger, context="structured schema")
        cols.append((str(col_name), dtype))

        logger.debug(f"Parsed column: name='{col_name}', type='{dtype.simpleString()}'")

    logger.info(f"Successfully parsed {len(cols)} columns from structured schema.")
    return cols
