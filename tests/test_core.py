# tests/test_core.py
import pytest
import json
from unittest.mock import MagicMock, patch
from datetime import datetime
from data_lakehouse_ingest.core import (
    load_json_data, load_xml_data, data_lakehouse_ingest_config
)

# ---------------------------------------------------------------------
# Helper: create a dummy SparkSession mock
# ---------------------------------------------------------------------
@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.read.option.return_value.json.return_value.count.return_value = 5
    spark.read.format.return_value.options.return_value.load.return_value.count.return_value = 3
    spark.read.options.return_value.format.return_value.load.return_value.count.return_value = 4
    spark.catalog.setCurrentDatabase.return_value = None
    spark.sql.return_value = None
    spark.read.format.return_value.load.return_value.count.return_value = 3
    return spark


@pytest.fixture
def mock_logger():
    logger = MagicMock()
    return logger


# ---------------------------------------------------------------------
# load_json_data tests
# ---------------------------------------------------------------------
def test_load_json_data_success(mock_spark, mock_logger):
    df = load_json_data(mock_spark, "s3a://bucket/sample.json", mock_logger)
    assert df is not None
    mock_logger.info.assert_any_call("📂 Reading JSON data from: s3a://bucket/sample.json")


def test_load_json_data_failure(mock_spark, mock_logger):
    mock_spark.read.option.side_effect = Exception("bad json")
    with pytest.raises(Exception):
        load_json_data(mock_spark, "bad.json", mock_logger)
    mock_logger.error.assert_called()


# ---------------------------------------------------------------------
# load_xml_data tests
# ---------------------------------------------------------------------
def test_load_xml_data_success(mock_spark, mock_logger):
    opts = {"rowTag": "record"}
    mock_spark.read.format.return_value.options.return_value.load.return_value.count.return_value = 2
    df = load_xml_data(mock_spark, "s3a://bucket/data.xml", opts, mock_logger)
    assert df is not None
    mock_logger.info.assert_any_call("📂 Reading XML data from: s3a://bucket/data.xml")


def test_load_xml_data_missing_rowtag(mock_spark, mock_logger):
    with pytest.raises(ValueError):
        load_xml_data(mock_spark, "data.xml", {}, mock_logger)
    mock_logger.error.assert_called_once()


def test_load_xml_data_failure(mock_spark, mock_logger):
    opts = {"rowTag": "entry"}
    mock_spark.read.format.return_value.options.side_effect = Exception("parse fail")
    with pytest.raises(Exception):
        load_xml_data(mock_spark, "bad.xml", opts, mock_logger)
    mock_logger.error.assert_called()


# ---------------------------------------------------------------------
# data_lakehouse_ingest_config tests
# ---------------------------------------------------------------------
def test_ingest_config_missing_spark(mock_logger):
    result = data_lakehouse_ingest_config(config={}, spark=None, logger=mock_logger)
    assert result["success"] is False
    assert "spark_initialization" in json.dumps(result)


@patch("data_lakehouse_ingest.core.ConfigLoader")
def test_ingest_config_configloader_failure(mock_loader, mock_spark, mock_logger):
    mock_loader.side_effect = Exception("invalid config")
    result = data_lakehouse_ingest_config(config={}, spark=mock_spark, logger=mock_logger)
    assert result["success"] is False
    assert "config_validation" in json.dumps(result)


@patch("data_lakehouse_ingest.core.load_linkml_schema")
@patch("data_lakehouse_ingest.core.ConfigLoader")
def test_ingest_config_valid_json(mock_loader, mock_linkml, mock_spark, mock_logger):
    # Mock ConfigLoader return values
    loader = mock_loader.return_value
    loader.get_full_config.return_value = {}
    loader.get_tenant.return_value = "tenant_demo"
    loader.get_all_defaults.return_value = {"json": {"header": True}}
    loader.get_tables.return_value = [{
        "name": "table1",
        "format": "json",
        "schema_sql": "id STRING, name STRING",
        "bronze_path": "s3a://bucket/file.json",
        "linkml_schema": None,
        "silver_path": "s3a://bucket/silver/table1"
    }]
    loader.get_bronze_path.return_value = "s3a://bucket/file.json"
    loader.get_silver_path.return_value = "s3a://bucket/silver/table1"

    result = data_lakehouse_ingest_config(config={}, spark=mock_spark, logger=mock_logger)
    assert result["success"] is True
    assert len(result["tables"]) == 1
    assert result["tables"][0]["status"] == "success"


@patch("data_lakehouse_ingest.core.load_linkml_schema")
@patch("data_lakehouse_ingest.core.ConfigLoader")
def test_ingest_config_linkml_failure_fallback(mock_loader, mock_linkml, mock_spark, mock_logger):
    mock_linkml.side_effect = Exception("bad schema")
    loader = mock_loader.return_value
    loader.get_full_config.return_value = {}
    loader.get_tenant.return_value = "tenant_demo"
    loader.get_all_defaults.return_value = {}
    loader.get_tables.return_value = [{
        "name": "table1",
        "format": "json",
        "schema_sql": "id STRING",
        "linkml_schema": "s3a://bucket/schema.yml",
        "bronze_path": "s3a://bucket/file.json",
        "silver_path": "s3a://bucket/silver/table1"
    }]
    loader.get_bronze_path.return_value = "s3a://bucket/file.json"
    loader.get_silver_path.return_value = "s3a://bucket/silver/table1"

    result = data_lakehouse_ingest_config(config={}, spark=mock_spark, logger=mock_logger)
    assert result["success"] is True
    assert result["tables"][0]["schema_source"] in ("schema_linkml_path", "inferred")
    mock_logger.warning.assert_any_call(
        "⚠️ Falling back to inline schema_sql for table1."
    )
