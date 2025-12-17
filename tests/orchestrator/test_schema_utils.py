import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, DoubleType
from data_lakehouse_ingest.orchestrator.schema_utils import (
    resolve_schema,
    apply_schema_columns,
    parse_schema_sql,
)
from pyspark.sql.types import ArrayType


# ----------------------------------------------------------------------
# resolve_schema tests
# ----------------------------------------------------------------------
def test_resolve_schema_raises_when_linkml_present():
    table = {
        "name": "t1",
        "linkml_schema": "path/to/schema",
        "schema_sql": "id STRING, name STRING",
    }

    mock_spark = MagicMock()
    mock_logger = MagicMock()

    with pytest.raises(NotImplementedError):
        resolve_schema(mock_spark, table, mock_logger)


def test_resolve_schema_returns_schema_sql():
    table = {"name": "t1", "schema_sql": "id INT, name STRING"}

    mock_spark = MagicMock()
    mock_logger = MagicMock()

    schema, source = resolve_schema(mock_spark, table, mock_logger)

    assert schema == "id INT, name STRING"
    assert source.value == "schema_sql"


def test_resolve_schema_returns_inferred_when_none_present():
    table = {"name": "t1"}

    mock_spark = MagicMock()
    mock_logger = MagicMock()

    schema, source = resolve_schema(mock_spark, table, mock_logger)

    assert schema is None
    assert source.value == "inferred"


# ----------------------------------------------------------------------
# parse_schema_sql tests
# ----------------------------------------------------------------------
def test_parse_schema_sql_parses_correctly():
    mock_logger = MagicMock()

    schema_sql = "id INT, name STRING, score DOUBLE"
    parsed = parse_schema_sql(schema_sql, mock_logger)

    assert parsed[0][0] == "id"
    assert isinstance(parsed[0][1], IntegerType)

    assert parsed[1][0] == "name"
    assert isinstance(parsed[1][1], StringType)

    assert parsed[2][0] == "score"
    assert isinstance(parsed[2][1], DoubleType)


def test_parse_schema_sql_raises_on_invalid_column_def():
    mock_logger = MagicMock()

    with pytest.raises(ValueError):
        parse_schema_sql("id", mock_logger)  # missing type


def test_parse_schema_sql_raises_on_unsupported_type():
    mock_logger = MagicMock()

    with pytest.raises(ValueError):
        parse_schema_sql("id SUPERSTRING", mock_logger)


# ----------------------------------------------------------------------
# apply_schema_columns tests
# ----------------------------------------------------------------------
def test_apply_schema_columns_casts_and_orders_columns():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

    df = spark.createDataFrame([(1, "A", 3.14)], ["id", "name", "value"])

    schema_sql = "id INT, name STRING, value DOUBLE"
    mock_logger = MagicMock()

    df2, meta = apply_schema_columns(df, schema_sql, mock_logger)

    assert df2.columns == ["id", "name", "value"]

    # Use isinstance for type checks (works across Spark versions)
    assert isinstance(df2.schema["id"].dataType, IntegerType)
    assert isinstance(df2.schema["name"].dataType, StringType)
    assert isinstance(df2.schema["value"].dataType, DoubleType)
    assert meta["dropped_columns"] == []


def test_apply_schema_columns_raises_on_missing_columns():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

    df = spark.createDataFrame([(1, "A")], ["col1", "col2"])

    schema_sql = "id INT, name STRING, value DOUBLE"  # value is missing
    mock_logger = MagicMock()

    with pytest.raises(ValueError):
        apply_schema_columns(df, schema_sql, mock_logger)


def test_apply_schema_columns_drops_extra_columns():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()

    df = spark.createDataFrame([(1, "A", 99)], ["id", "name", "extra"])

    schema_sql = "id INT, name STRING"
    mock_logger = MagicMock()

    df2, meta = apply_schema_columns(df, schema_sql, mock_logger)

    assert df2.columns == ["id", "name"]
    assert "extra" not in df2.columns
    assert meta["dropped_columns"] == ["extra"]


def test_parse_schema_sql_array_string():
    logger = MagicMock()
    result = parse_schema_sql("tags ARRAY<STRING>", logger)
    assert isinstance(result[0][1], ArrayType)
    assert isinstance(result[0][1].elementType, StringType)


def test_parse_schema_sql_array_int():
    logger = MagicMock()
    result = parse_schema_sql("values ARRAY<INT>", logger)
    assert isinstance(result[0][1].elementType, IntegerType)


def test_parse_schema_sql_nested_array():
    logger = MagicMock()
    result = parse_schema_sql("matrix ARRAY<ARRAY<DOUBLE>>", logger)
    assert isinstance(result[0][1], ArrayType)
    assert isinstance(result[0][1].elementType, ArrayType)
