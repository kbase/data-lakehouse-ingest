import pytest
from unittest.mock import MagicMock
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    IntegerType,
    StringType,
    DoubleType,
    DecimalType,
    ArrayType,
    MapType,
    StructType,
    StructField,
)
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


def test_resolve_schema_raises_when_schema_and_schema_sql_both_defined_even_if_schema_empty():
    table = {"name": "t1", "schema": [], "schema_sql": "id INT"}
    mock_spark = MagicMock()
    mock_logger = MagicMock()

    with pytest.raises(ValueError, match=r"both 'schema' and 'schema_sql' are defined"):
        resolve_schema(mock_spark, table, mock_logger)


def test_resolve_schema_raises_when_schema_and_schema_sql_both_defined():
    table = {
        "name": "t1",
        "schema": [{"column": "id", "type": "INT", "comment": "pk"}],
        "schema_sql": "id STRING",
    }
    mock_spark = MagicMock()
    mock_logger = MagicMock()

    with pytest.raises(ValueError, match=r"both 'schema' and 'schema_sql' are defined"):
        resolve_schema(mock_spark, table, mock_logger)


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
    assert meta == {"dropped_columns": []}


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
    assert meta == {"dropped_columns": ["extra"]}


def test_apply_schema_columns_returns_unchanged_when_no_schema_defs():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame([(1,)], ["id"])
    logger = MagicMock()

    df2, meta = apply_schema_columns(df, None, logger)

    assert df2.columns == ["id"]
    assert df2.collect() == [(1,)]
    assert meta == {"dropped_columns": []}


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
    assert meta == {"dropped_columns": ["value"]}


def test_apply_schema_columns_converts_json_string_to_array():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    logger = MagicMock()

    # input column is a JSON string representation of an array
    df = spark.createDataFrame([('["a","b"]',)], ["tags"])

    schema_defs = parse_schema_sql("tags ARRAY<STRING>", logger)

    df2, meta = apply_schema_columns(df, schema_defs, logger)

    row = df2.collect()[0]
    assert row["tags"] == ["a", "b"]
    assert meta == {"dropped_columns": []}


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


# ----------------------------------------------------------------------
# MAP type tests
# ----------------------------------------------------------------------


def test_parse_schema_sql_map_string_string():
    """Parses MAP<STRING,STRING> into a Spark MapType."""
    logger = MagicMock()

    result = parse_schema_sql("attrs MAP<STRING,STRING>", logger)

    assert result[0][0] == "attrs"
    assert isinstance(result[0][1], MapType)
    assert isinstance(result[0][1].keyType, StringType)
    assert isinstance(result[0][1].valueType, StringType)


def test_parse_schema_sql_map_string_int():
    """Parses MAP<STRING,INT> into a Spark MapType with integer values."""
    logger = MagicMock()

    result = parse_schema_sql("attrs MAP<STRING,INT>", logger)

    assert result[0][0] == "attrs"
    assert isinstance(result[0][1], MapType)
    assert isinstance(result[0][1].keyType, StringType)
    assert isinstance(result[0][1].valueType, IntegerType)


def test_parse_schema_sql_map_string_array_double():
    """Parses MAP<STRING,ARRAY<DOUBLE>> into a nested Spark MapType."""
    logger = MagicMock()

    result = parse_schema_sql("attrs MAP<STRING,ARRAY<DOUBLE>>", logger)

    assert result[0][0] == "attrs"
    assert isinstance(result[0][1], MapType)
    assert isinstance(result[0][1].keyType, StringType)
    assert isinstance(result[0][1].valueType, ArrayType)
    assert isinstance(result[0][1].valueType.elementType, DoubleType)


def test_parse_schema_sql_array_of_map():
    """Parses ARRAY<MAP<STRING,INT>> into a nested Spark ArrayType."""
    logger = MagicMock()

    result = parse_schema_sql("attrs ARRAY<MAP<STRING,INT>>", logger)

    assert result[0][0] == "attrs"
    assert isinstance(result[0][1], ArrayType)
    assert isinstance(result[0][1].elementType, MapType)
    assert isinstance(result[0][1].elementType.keyType, StringType)
    assert isinstance(result[0][1].elementType.valueType, IntegerType)


def test_parse_schema_sql_raises_on_invalid_map_definition_missing_value_type():
    """Raises ValueError for a MAP definition missing the value type."""
    logger = MagicMock()

    with pytest.raises(ValueError, match=r"Invalid MAP definition"):
        parse_schema_sql("attrs MAP<STRING>", logger)

    logger.error.assert_called()


def test_parse_schema_sql_raises_on_invalid_map_definition_empty_value_type():
    """Raises ValueError for a MAP definition with an empty value type."""
    logger = MagicMock()

    with pytest.raises(ValueError, match=r"Invalid MAP definition"):
        parse_schema_sql("attrs MAP<STRING,>", logger)

    logger.error.assert_called()


def test_parse_schema_structured_supports_map_type():
    """Parses structured schema entries containing MAP<STRING,STRING>."""
    logger = MagicMock()
    schema = [{"column": "attrs", "type": "MAP<STRING,STRING>"}]

    parsed = parse_schema_structured(schema, logger)

    assert parsed[0][0] == "attrs"
    assert isinstance(parsed[0][1], MapType)
    assert isinstance(parsed[0][1].keyType, StringType)
    assert isinstance(parsed[0][1].valueType, StringType)


def test_parse_schema_structured_supports_nested_map_type():
    """Parses structured schema entries containing nested MAP types."""
    logger = MagicMock()
    schema = [{"column": "attrs", "type": "MAP<STRING,ARRAY<INT>>"}]

    parsed = parse_schema_structured(schema, logger)

    assert parsed[0][0] == "attrs"
    assert isinstance(parsed[0][1], MapType)
    assert isinstance(parsed[0][1].keyType, StringType)
    assert isinstance(parsed[0][1].valueType, ArrayType)
    assert isinstance(parsed[0][1].valueType.elementType, IntegerType)


def test_resolve_schema_returns_structured_schema_with_map_comment_metadata():
    """Resolves structured MAP schema and preserves column comment metadata."""
    table = {
        "name": "t1",
        "schema": [
            {"column": "attrs", "type": "MAP<STRING,STRING>", "comment": "metadata map"},
        ],
    }

    mock_spark = MagicMock()
    mock_logger = MagicMock()

    resolved = resolve_schema(mock_spark, table, mock_logger)

    assert resolved.schema_source == SchemaSource.SCHEMA_STRUCTURED
    assert resolved.comment_metadata == [{"column": "attrs", "comment": "metadata map"}]
    assert resolved.schema_defs is not None
    assert resolved.schema_defs[0][0] == "attrs"
    assert isinstance(resolved.schema_defs[0][1], MapType)


def test_apply_schema_columns_converts_json_string_to_map():
    """Converts a JSON string column into a Spark MAP<STRING,STRING> column."""
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    logger = MagicMock()

    df = spark.createDataFrame([('{"a":"1","b":"2"}',)], ["attrs"])

    schema_defs = parse_schema_sql("attrs MAP<STRING,STRING>", logger)

    df2, meta = apply_schema_columns(df, schema_defs, logger)

    row = df2.collect()[0]
    assert row["attrs"] == {"a": "1", "b": "2"}
    assert meta == {"dropped_columns": []}


def test_apply_schema_columns_converts_json_string_to_map_with_array_values():
    """Converts a JSON string column into a Spark MAP with array values."""
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    logger = MagicMock()

    df = spark.createDataFrame([('{"x":[1,2],"y":[3,4]}',)], ["attrs"])

    schema_defs = parse_schema_sql("attrs MAP<STRING,ARRAY<INT>>", logger)

    df2, meta = apply_schema_columns(df, schema_defs, logger)

    row = df2.collect()[0]
    assert row["attrs"] == {"x": [1, 2], "y": [3, 4]}
    assert meta == {"dropped_columns": []}


def test_apply_schema_columns_converts_struct_to_map():
    """Converts a StructType column into a Spark MAP<STRING,STRING> column."""
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    logger = MagicMock()

    schema = StructType(
        [
            StructField(
                "attrs",
                StructType(
                    [
                        StructField("a", StringType(), True),
                        StructField("b", StringType(), True),
                    ]
                ),
                True,
            )
        ]
    )

    df = spark.createDataFrame(
        [({"a": "one", "b": "two"},)],
        schema=schema,
    )

    schema_defs = parse_schema_sql("attrs MAP<STRING,STRING>", logger)

    df2, meta = apply_schema_columns(df, schema_defs, logger)

    row = df2.collect()[0]
    assert row["attrs"] == {"a": "one", "b": "two"}
    assert meta == {"dropped_columns": []}


def test_parse_schema_structured_raises_on_invalid_map_definition():
    """Raises ValueError for malformed nested MAP syntax in structured schema."""
    logger = MagicMock()
    schema = [{"column": "attrs", "type": "MAP<STRING,ARRAY<INT>"}]

    with pytest.raises(ValueError, match=r"Invalid MAP definition|unclosed '<'"):
        parse_schema_structured(schema, logger)


def test_apply_schema_columns_casts_existing_array_column_via_complex_type_fallback():
    """Casts an already-typed array column through the complex-type fallback branch."""
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    logger = MagicMock()

    df = spark.createDataFrame([([1, 2, 3],)], ["tags"])

    schema_defs = parse_schema_sql("tags ARRAY<INT>", logger)

    df2, meta = apply_schema_columns(df, schema_defs, logger)

    assert df2.collect()[0]["tags"] == [1, 2, 3]
    assert meta == {"dropped_columns": []}


def test_parse_schema_sql_map_decimal_key_hits_paren_tracking():
    """Parses MAP with DECIMAL key to exercise parenthesis tracking in key parsing."""
    logger = MagicMock()

    result = parse_schema_sql("attrs MAP<DECIMAL(10,2),STRING>", logger)

    assert result[0][0] == "attrs"
    assert isinstance(result[0][1], MapType)
    assert isinstance(result[0][1].keyType, DecimalType)
    assert result[0][1].keyType.precision == 10
    assert result[0][1].keyType.scale == 2
    assert isinstance(result[0][1].valueType, StringType)


def test_parse_schema_sql_map_array_key_hits_angle_tracking():
    """Parses MAP with ARRAY key to exercise angle bracket tracking in key parsing."""
    logger = MagicMock()

    result = parse_schema_sql("attrs MAP<ARRAY<INT>,STRING>", logger)

    assert result[0][0] == "attrs"
    assert isinstance(result[0][1], MapType)
    assert isinstance(result[0][1].keyType, ArrayType)
    assert isinstance(result[0][1].keyType.elementType, IntegerType)
    assert isinstance(result[0][1].valueType, StringType)
