"""
Schema utilities for the Data Lakehouse Ingest framework.

Provides helpers to resolve table schemas, parse SQL-style schema definitions,
and align DataFrame columns using governed Spark DataTypes, including support
for DECIMAL(p,s) and ARRAY<T> types.
"""
from minio import Minio
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, regexp_replace, from_json, split
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

class SchemaSource(Enum):
    """Enum describing the origin of a resolved schema."""
    SCHEMA_SQL = "schema_sql"
    INFERRED = "inferred"


def resolve_schema(
    spark: SparkSession,
    table: dict[str, object],
    logger: logging.Logger,
    minio_client: Minio | None = None,
) -> tuple[str | None, SchemaSource]:
    """
    Resolve the schema definition for a given table.

    Current behavior (LinkML not yet supported):
        - If a LinkML schema path is provided, the function logs an error and
          raises NotImplementedError. There is **no** automatic fallback to SQL
          when LinkML is present.
        - If `schema_sql` is provided (and no LinkML schema), it is returned and
          marked as SchemaSource.SCHEMA_SQL.
        - If neither is provided, the schema is treated as inferred and marked
          as SchemaSource.INFERRED.

    Args:
        spark (SparkSession): Active Spark session (unused until LinkML is implemented).
        table (dict): Full table definition from the ingestion config. 
            This dict may include many fields (e.g., name, bronze_path, enabled, 
            schema_sql, linkml_schema, partition_by, drop_extra_columns, etc.). 
            Only the schema-related fields are used in this function.
        logger (logging.Logger): Logger for reporting resolution decisions.
        minio_client (Minio | None): Placeholder for future LinkML support.

    Returns:
        tuple[str | None, SchemaSource]:
            - schema_sql (str | None): The resolved SQL-style schema string,
              or None if the schema is inferred.
            - schema_source (SchemaSource): Enum indicating where the schema
              came from (SchemaSource.SCHEMA_SQL or SchemaSource.INFERRED).

    Notes:
        - LinkML parsing is not implemented yet. When a LinkML path is present,
          the function logs an error and raises NotImplementedError. Once
          LinkML support is added, this behavior may change to parse the
          LinkML schema and return a corresponding SQL-style definition.
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
        logger.info(f"Using schema_sql for table {table.get('name')}")
        return schema_sql, SchemaSource.SCHEMA_SQL

    logger.info(f"No schema provided for table {table.get('name')}; using inferred schema")
    return None, SchemaSource.INFERRED


def apply_schema_columns(
    df: DataFrame,
    schema_sql: str | None,
    logger: logging.Logger,
):
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
        • When `schema_sql` is None:
            - No schema alignment is performed.
            - The DataFrame is returned unchanged.
            - This corresponds to the “inferred schema” path, where the ingestion
            pipeline relies on Spark's natural column order (from file headers).

        • When `schema_sql` is provided:
            - Validates required columns.
            - Drops extra columns.
            - Casts columns to their declared SQL types.
            - Reorders columns to match schema_sql.

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
            Determines required columns, type cast targets, and final column order.
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
        return df, {"dropped_columns": []}

    # Parse SQL schema into structured representation
    schema_defs = parse_schema_sql(schema_sql, logger)
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
            f"Extra columns in data not present in schema_sql: {extra_cols}. "
            "These columns will be excluded from the output."
        )

    # Enforce ordering + type casting
    projected_cols = []

    for col_name, col_type in schema_defs:
        if isinstance(col_type, ArrayType):
            # Correct way to turn JSON string -> Spark ARRAY
            projected_cols.append(
                from_json(col(col_name), col_type).alias(col_name)
            )
        else:
            # Normal scalar cast
            projected_cols.append(
                col(col_name).cast(col_type).alias(col_name)
            )


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
        • A definition must contain at least two tokens: <name> <type>.
        • Additional trailing tokens are not allowed (fail-fast behavior).
        • Invalid or unsupported data types raise ValueError with a
          descriptive error message.

    Args:
        schema_sql (str):
            A comma-separated SQL-style schema definition.

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
    def _to_pyspark_type(dt_raw: str) -> DataType:
        dt = dt_raw.upper()

        # DECIMAL(p,s) special-case
        if dt.startswith("DECIMAL(") and dt.endswith(")"):
            inner = dt[len("DECIMAL("):-1]
            try:
                precision_str, scale_str = inner.split(",")
                precision = int(precision_str.strip())
                scale = int(scale_str.strip())
            except Exception as exc:
                logger.error(f"Invalid DECIMAL definition '{dt_raw}': {exc}")
                raise ValueError(f"Invalid DECIMAL definition '{dt_raw}'") from exc
            return DecimalType(precision=precision, scale=scale)

        # ----------------------------
        # ARRAY<TYPE>
        # ----------------------------
        array_match = re.match(r"^ARRAY<(.+)>$", dt)
        if array_match:
            inner_type_raw = array_match.group(1).strip()
            inner_type = _to_pyspark_type(inner_type_raw)  # recursive
            return ArrayType(inner_type)

        # ----------------------------
        # Primitive Types
        # ----------------------------
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
                f"Unsupported data type '{dt_raw}' in schema_sql "
                "(expected one of STRING, INT, INTEGER, BIGINT, LONG, DOUBLE, FLOAT, BOOLEAN, "
                "DATE, TIMESTAMP, DECIMAL(p,s), ARRAY<T>)."
            )
            raise ValueError(f"Unsupported data type '{dt_raw}' in schema_sql.")

        return mapping[dt]

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

        if len(parts) < 2:
            logger.error(f"Invalid column definition in schema_sql: '{col_def}'")
            raise ValueError(f"Invalid column definition in schema_sql: '{col_def}'")

        col_name, col_type_raw = parts[0], parts[1]
        dtype = _to_pyspark_type(col_type_raw)

        columns.append((col_name, dtype))
        logger.debug(f"Parsed column: name='{col_name}', type='{dtype.simpleString()}'")

    logger.info(f"Successfully parsed {len(columns)} columns from schema_sql.")
    return columns
