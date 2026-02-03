import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, DoubleType, DecimalType, ArrayType
from data_lakehouse_ingest.orchestrator.schema_utils import (
    resolve_schema,
    apply_schema_columns,
    parse_schema_sql,
    parse_schema_structured,
    SchemaSource,
)


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

    with pytest.raises(
        NotImplementedError,
        match="LinkML schema parsing is not implemented yet",
    ):
        resolve_schema(mock_spark, table, mock_logger)


def test_resolve_schema_returns_schema_sql():
    table = {"name": "t1", "schema_sql": "id INT, name STRING"}

    mock_spark = MagicMock()
    mock_logger = MagicMock()

    schema_defs, source = resolve_schema(mock_spark, table, mock_logger)

    assert source == SchemaSource.SCHEMA_SQL

    assert schema_defs[0][0] == "id"
    assert isinstance(schema_defs[0][1], IntegerType)

    assert schema_defs[1][0] == "name"
    assert isinstance(schema_defs[1][1], StringType)


def test_resolve_schema_returns_inferred_when_none_present():
    table = {"name": "t1"}

    mock_spark = MagicMock()
    mock_logger = MagicMock()

    schema, source = resolve_schema(mock_spark, table, mock_logger)

    assert schema is None
    assert source == SchemaSource.INFERRED


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

    with pytest.raises(
        ValueError,
        match="Expected format: '<column_name> <data_type>'",
    ):
        parse_schema_sql("id", mock_logger)


def test_parse_schema_sql_raises_on_unsupported_type():
    mock_logger = MagicMock()

    with pytest.raises(
        ValueError,
        match="Unsupported data type 'SUPERSTRING'",
    ):
        parse_schema_sql("id SUPERSTRING", mock_logger)


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


def test_parse_schema_sql_decimal_parses_success():
    logger = MagicMock()

    result = parse_schema_sql("amount DECIMAL(10,2)", logger)

    assert result[0][0] == "amount"
    assert isinstance(result[0][1], DecimalType)
    assert result[0][1].precision == 10
    assert result[0][1].scale == 2


def test_parse_schema_sql_decimal_raises_on_invalid_definition():
    logger = MagicMock()

    with pytest.raises(ValueError, match=r"Invalid DECIMAL definition"):
        parse_schema_sql("amount DECIMAL(10)", logger)  # missing scale part

    logger.error.assert_called()


def test_parse_schema_sql_raises_on_trailing_comma_empty_segment():
    logger = MagicMock()

    with pytest.raises(ValueError, match="Empty column definition in schema_sql"):
        parse_schema_sql("id INT,", logger)


# ----------------------------------------------------------------------
# apply_schema_columns tests
# ----------------------------------------------------------------------
def test_apply_schema_columns_casts_and_orders_columns():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame([(1, "A", 3.14)], ["id", "name", "value"])

    schema_sql = "id INT, name STRING, value DOUBLE"
    mock_logger = MagicMock()

    schema_defs = parse_schema_sql(schema_sql, mock_logger)
    df2, meta = apply_schema_columns(df, schema_defs, mock_logger)

    assert df2.columns == ["id", "name", "value"]
    assert isinstance(df2.schema["id"].dataType, IntegerType)
    assert isinstance(df2.schema["name"].dataType, StringType)
    assert isinstance(df2.schema["value"].dataType, DoubleType)
    assert df2.collect() == [(1, "A", 3.14)]
    assert meta["dropped_columns"] == []


def test_apply_schema_columns_raises_on_missing_columns():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame([(1, "A")], ["id", "name"])

    schema_sql = "id INT, name STRING, value DOUBLE"
    mock_logger = MagicMock()

    schema_defs = parse_schema_sql(schema_sql, mock_logger)

    with pytest.raises(ValueError, match=r"missing columns: \['value'\]"):
        apply_schema_columns(df, schema_defs, mock_logger)


def test_apply_schema_columns_drops_extra_columns():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame([(1, "A", 99)], ["id", "name", "extra"])

    schema_sql = "id INT, name STRING"
    mock_logger = MagicMock()

    schema_defs = parse_schema_sql(schema_sql, mock_logger)
    df2, meta = apply_schema_columns(df, schema_defs, mock_logger)

    assert df2.columns == ["id", "name"]
    assert "extra" not in df2.columns
    assert df2.collect() == [(1, "A")]
    assert meta["dropped_columns"] == ["extra"]


def test_apply_schema_columns_returns_unchanged_when_no_schema_defs():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame([(1,)], ["id"])
    logger = MagicMock()

    df2, meta = apply_schema_columns(df, None, logger)

    assert df2.columns == ["id"]
    assert df2.collect() == [(1,)]
    assert meta["dropped_columns"] == []


def test_apply_schema_columns_with_structured_schema_orders_and_drops_extra():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame([(1, "A", 3.14)], ["id", "name", "value"])
    logger = MagicMock()
    mock_spark = MagicMock()

    table = {
        "name": "t1",
        "schema": [
            {"column": "name", "type": "STRING"},
            {"column": "id", "type": "INT"},
        ],
    }

    schema_defs, source = resolve_schema(mock_spark, table, logger)
    assert source == SchemaSource.SCHEMA_STRUCTURED

    df2, meta = apply_schema_columns(df, schema_defs, logger)

    assert df2.columns == ["name", "id"]  # order comes from schema
    assert df2.collect() == [("A", 1)]  # extra column dropped
    assert meta["dropped_columns"] == ["value"]


def test_apply_schema_columns_converts_json_string_to_array():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    logger = MagicMock()

    # input column is a JSON string representation of an array
    df = spark.createDataFrame([('["a","b"]',)], ["tags"])

    schema_defs = parse_schema_sql("tags ARRAY<STRING>", logger)

    df2, meta = apply_schema_columns(df, schema_defs, logger)

    row = df2.collect()[0]
    assert row["tags"] == ["a", "b"]
    assert meta["dropped_columns"] == []


def test_apply_schema_columns_converts_json_string_to_nested_array():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    logger = MagicMock()

    df = spark.createDataFrame([("[[1,2],[3,4]]",)], ["matrix"])

    schema_defs = parse_schema_sql("matrix ARRAY<ARRAY<INT>>", logger)

    df2, _ = apply_schema_columns(df, schema_defs, logger)

    assert df2.collect()[0]["matrix"] == [[1, 2], [3, 4]]


# ----------------------------------------------------------------------
# parse_schema_structured tests
# ----------------------------------------------------------------------


def test_parse_schema_structured_parses_correctly_column_key():
    logger = MagicMock()
    schema = [
        {"column": "id", "type": "INT"},
        {"column": "name", "type": "STRING"},
        {"column": "score", "type": "DOUBLE"},
    ]

    parsed = parse_schema_structured(schema, logger)

    assert parsed[0][0] == "id"
    assert isinstance(parsed[0][1], IntegerType)

    assert parsed[1][0] == "name"
    assert isinstance(parsed[1][1], StringType)

    assert parsed[2][0] == "score"
    assert isinstance(parsed[2][1], DoubleType)


def test_parse_schema_structured_accepts_name_alias():
    logger = MagicMock()
    schema = [{"name": "id", "type": "INT"}]

    parsed = parse_schema_structured(schema, logger)
    assert parsed[0][0] == "id"
    assert isinstance(parsed[0][1], IntegerType)


def test_parse_schema_structured_supports_array_types():
    logger = MagicMock()
    schema = [{"column": "tags", "type": "ARRAY<STRING>"}]

    parsed = parse_schema_structured(schema, logger)

    assert parsed[0][0] == "tags"
    assert isinstance(parsed[0][1], ArrayType)
    assert isinstance(parsed[0][1].elementType, StringType)


def test_parse_schema_structured_raises_if_schema_not_list():
    logger = MagicMock()
    with pytest.raises(ValueError, match="Structured schema must be a list"):
        parse_schema_structured({"column": "id", "type": "INT"}, logger)  # not a list


def test_parse_schema_structured_raises_if_entry_not_dict():
    logger = MagicMock()
    with pytest.raises(ValueError, match="Invalid schema entry at index 0"):
        parse_schema_structured(["id INT"], logger)


def test_parse_schema_structured_raises_if_missing_column_and_name():
    logger = MagicMock()
    with pytest.raises(ValueError, match=r"missing 'column'/'name'"):
        parse_schema_structured([{"type": "INT"}], logger)


def test_parse_schema_structured_raises_if_missing_type():
    logger = MagicMock()
    with pytest.raises(ValueError, match=r"missing 'type'"):
        parse_schema_structured([{"column": "id"}], logger)


def test_parse_schema_structured_raises_on_unsupported_type():
    logger = MagicMock()
    with pytest.raises(ValueError, match=r"Unsupported data type 'SUPERSTRING'"):
        parse_schema_structured([{"column": "id", "type": "SUPERSTRING"}], logger)


def test_resolve_schema_returns_structured_schema_precedence():
    table = {
        "name": "t1",
        "schema": [{"column": "id", "type": "INT"}],
        "schema_sql": "id STRING",
    }
    mock_spark = MagicMock()
    mock_logger = MagicMock()

    schema_defs, source = resolve_schema(mock_spark, table, mock_logger)

    assert source == SchemaSource.SCHEMA_STRUCTURED
    assert schema_defs[0][0] == "id"
    assert isinstance(schema_defs[0][1], IntegerType)
