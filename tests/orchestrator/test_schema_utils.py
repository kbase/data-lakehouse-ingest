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

    resolved = resolve_schema(mock_spark, table, mock_logger)

    assert resolved.schema_source == SchemaSource.SCHEMA_SQL
    assert resolved.comment_metadata is None
    schema_defs = resolved.schema_defs
    assert schema_defs is not None

    assert schema_defs[0][0] == "id"
    assert isinstance(schema_defs[0][1], IntegerType)

    assert schema_defs[1][0] == "name"
    assert isinstance(schema_defs[1][1], StringType)


def test_resolve_schema_returns_inferred_when_none_present():
    table = {"name": "t1"}

    mock_spark = MagicMock()
    mock_logger = MagicMock()

    resolved = resolve_schema(mock_spark, table, mock_logger)

    assert resolved.schema_defs is None
    assert resolved.schema_source == SchemaSource.INFERRED
    assert resolved.comment_metadata is None


def test_resolve_schema_raises_when_schema_not_list():
    table = {"name": "t1", "schema": {"column": "id", "type": "INT"}}
    mock_spark = MagicMock()
    mock_logger = MagicMock()

    with pytest.raises(ValueError, match=r"'schema' must be a list of column definitions"):
        resolve_schema(mock_spark, table, mock_logger)


def test_resolve_schema_raises_when_schema_sql_not_string():
    table = {"name": "t1", "schema_sql": ["id INT"]}  # not a string
    mock_spark = MagicMock()
    mock_logger = MagicMock()

    with pytest.raises(ValueError, match=r"'schema_sql' must be a string"):
        resolve_schema(mock_spark, table, mock_logger)


def test_resolve_schema_empty_structured_schema_falls_back_to_schema_sql():
    table = {"name": "t1", "schema": [], "schema_sql": "id INT"}
    mock_spark = MagicMock()
    mock_logger = MagicMock()

    resolved = resolve_schema(mock_spark, table, mock_logger)

    assert resolved.schema_source == SchemaSource.SCHEMA_SQL
    assert resolved.comment_metadata is None
    schema_defs = resolved.schema_defs
    assert schema_defs is not None
    assert schema_defs[0][0] == "id"
    assert isinstance(schema_defs[0][1], IntegerType)

    mock_logger.info.assert_any_call(
        "'schema' provided but empty for table t1; falling back to schema_sql."
    )


def test_resolve_schema_returns_structured_schema_precedence():
    table = {
        "name": "t1",
        "schema": [{"column": "id", "type": "INT", "comment": "pk"}],
        "schema_sql": "id STRING",
    }
    mock_spark = MagicMock()
    mock_logger = MagicMock()

    resolved = resolve_schema(mock_spark, table, mock_logger)

    assert resolved.schema_source == SchemaSource.SCHEMA_STRUCTURED
    assert resolved.comment_metadata == [{"column": "id", "comment": "pk"}]
    schema_defs = resolved.schema_defs
    assert schema_defs is not None
    assert schema_defs[0][0] == "id"
    assert isinstance(schema_defs[0][1], IntegerType)

    mock_logger.info.assert_any_call(
        "Both 'schema' (structured) and 'schema_sql' provided for table t1; "
        "using structured schema and ignoring schema_sql."
    )


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


def test_parse_schema_sql_raises_on_unmatched_close_paren():
    logger = MagicMock()
    with pytest.raises(ValueError, match=r"unmatched '\)' at position"):
        parse_schema_sql("id INT)", logger)


def test_parse_schema_sql_raises_on_unmatched_close_angle():
    logger = MagicMock()
    with pytest.raises(ValueError, match=r"unmatched '>' at position"):
        parse_schema_sql("tags ARRAY<STRING>>", logger)


def test_parse_schema_sql_raises_on_unclosed_paren():
    logger = MagicMock()
    with pytest.raises(ValueError, match=r"unclosed '\('"):
        parse_schema_sql("amount DECIMAL(10,2", logger)


def test_parse_schema_sql_raises_on_unclosed_angle():
    logger = MagicMock()
    with pytest.raises(ValueError, match=r"unclosed '<'"):
        parse_schema_sql("tags ARRAY<STRING", logger)


def test_parse_schema_sql_raises_on_double_comma_empty_segment():
    logger = MagicMock()
    with pytest.raises(ValueError, match="Empty column definition in schema_sql"):
        parse_schema_sql("id INT,, name STRING", logger)


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

    resolved = resolve_schema(mock_spark, table, logger)

    assert resolved.schema_source == SchemaSource.SCHEMA_STRUCTURED
    schema_defs = resolved.schema_defs
    assert schema_defs is not None

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


def test_parse_schema_structured_raises_on_name_key_not_allowed():
    logger = MagicMock()
    schema = [{"name": "id", "type": "INT"}]

    with pytest.raises(ValueError, match=r"unsupported keys.*name"):
        parse_schema_structured(schema, logger)


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


def test_parse_schema_structured_raises_if_missing_column():
    logger = MagicMock()
    with pytest.raises(ValueError, match=r"missing required key 'column'"):
        parse_schema_structured([{"type": "INT"}], logger)


def test_parse_schema_structured_raises_if_missing_type():
    logger = MagicMock()
    with pytest.raises(ValueError, match=r"missing required key 'type'"):
        parse_schema_structured([{"column": "id"}], logger)


def test_parse_schema_structured_raises_on_unsupported_type():
    logger = MagicMock()
    with pytest.raises(ValueError, match=r"Unsupported data type 'SUPERSTRING'"):
        parse_schema_structured([{"column": "id", "type": "SUPERSTRING"}], logger)


def test_parse_schema_structured_raises_on_unsupported_keys():
    logger = MagicMock()
    schema = [{"column": "id", "type": "INT", "extra": "nope"}]

    with pytest.raises(ValueError, match=r"unsupported keys.*extra"):
        parse_schema_structured(schema, logger)


def test_parse_schema_structured_allows_nullable_and_comment():
    logger = MagicMock()
    schema = [{"column": "id", "type": "INT", "nullable": False, "comment": "pk"}]

    parsed = parse_schema_structured(schema, logger)
    assert parsed[0][0] == "id"
    assert isinstance(parsed[0][1], IntegerType)


def test_parse_schema_structured_raises_on_empty_column_value():
    logger = MagicMock()
    schema = [{"column": "   ", "type": "INT"}]

    with pytest.raises(ValueError, match=r"empty 'column' value"):
        parse_schema_structured(schema, logger)


def test_parse_schema_structured_raises_on_empty_type_value():
    logger = MagicMock()
    schema = [{"column": "id", "type": "   "}]

    with pytest.raises(ValueError, match=r"empty 'type' value"):
        parse_schema_structured(schema, logger)


def test_parse_schema_structured_raises_on_nullable_not_bool():
    logger = MagicMock()
    schema = [{"column": "id", "type": "INT", "nullable": "false"}]  # not bool

    with pytest.raises(ValueError, match=r"invalid 'nullable'"):
        parse_schema_structured(schema, logger)


def test_parse_schema_structured_raises_on_comment_not_string_or_none():
    logger = MagicMock()
    schema = [{"column": "id", "type": "INT", "comment": 123}]  # invalid

    with pytest.raises(ValueError, match=r"invalid 'comment'"):
        parse_schema_structured(schema, logger)


def test_parse_schema_structured_raises_when_column_not_string():
    logger = MagicMock()
    schema = [{"column": 123, "type": "INT"}]

    with pytest.raises(ValueError, match=r"invalid 'column'.*expected string"):
        parse_schema_structured(schema, logger)


def test_parse_schema_structured_raises_when_type_not_string():
    logger = MagicMock()
    schema = [{"column": "id", "type": 123}]

    with pytest.raises(ValueError, match=r"invalid 'type'.*expected string"):
        parse_schema_structured(schema, logger)
