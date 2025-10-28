import pytest
import logging
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
from data_lakehouse_ingest.orchestrator.schema_utils import resolve_schema, apply_schema_columns

@patch("data_lakehouse_ingest.orchestrator.schema_utils.load_linkml_schema")
def test_resolve_schema_uses_linkml(mock_load):
    mock_load.return_value = {"id": "STRING", "name": "STRING"}
    table = {"name": "t1", "linkml_schema": "path/to/schema"}
    mock_spark = MagicMock()
    mock_logger = MagicMock()

    schema_sql, source = resolve_schema(mock_spark, table, mock_logger)
    assert "id STRING" in schema_sql
    assert source == "linkml"

def test_apply_schema_columns_renames_columns_correctly():
    spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
    df = spark.createDataFrame([(1, "A")], ["col1", "col2"])
    schema_sql = "id INT, name STRING"
    mock_logger = MagicMock()

    df2 = apply_schema_columns(df, schema_sql, mock_logger)
    assert df2.columns == ["id", "name"]
