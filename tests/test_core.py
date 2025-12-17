import pytest
import json
from unittest.mock import MagicMock, patch
from data_lakehouse_ingest.core import ingest


# ---------------------------------------------------------------------
# Helper: create a dummy SparkSession mock
# ---------------------------------------------------------------------
@pytest.fixture
def mock_spark():
    spark = MagicMock()

    # --- Fake DataFrame returned by read.json ---
    mock_df = MagicMock()
    mock_df.columns = ["id", "name"]
    mock_df.select.return_value = mock_df
    mock_df.count.return_value = 5
    spark.read.option.return_value.options.return_value.json.return_value = mock_df

    # --- Mock DESCRIBE NAMESPACE EXTENDED ---
    fake_row = MagicMock()
    fake_row.info_name = "location"
    fake_row.info_value = "s3a://bucket/silver/"  # MUST NOT be None

    fake_result_df = MagicMock()
    fake_result_df.collect.return_value = [fake_row]

    spark.sql.return_value = fake_result_df

    # --- Prevent failures in setCurrentDatabase ---
    spark.catalog.setCurrentDatabase.return_value = None

    return spark


@pytest.fixture
def mock_logger():
    return MagicMock()


# ---------------------------------------------------------------------
# data_lakehouse_ingest_config tests
# ---------------------------------------------------------------------
def test_ingest_config_missing_spark(mock_logger):
    result = ingest(config={}, spark=None, logger=mock_logger)
    assert result["success"] is False
    assert "spark_initialization" in json.dumps(result)


@patch("data_lakehouse_ingest.core.get_minio_client")
@patch("data_lakehouse_ingest.core.ConfigLoader")
def test_ingest_config_configloader_failure(mock_loader, mock_spark, mock_logger):
    mock_loader.side_effect = Exception("invalid config")
    result = ingest(config={}, spark=mock_spark, logger=mock_logger)
    assert result["success"] is False
    assert "config_validation" in json.dumps(result)


@patch("data_lakehouse_ingest.orchestrator.init_utils.create_namespace_if_not_exists")
@patch("data_lakehouse_ingest.core.get_minio_client")
@patch("data_lakehouse_ingest.core.process_tables")
@patch("data_lakehouse_ingest.core.ConfigLoader")
def test_ingest_config_valid_json(
    mock_configloader,
    mock_process,
    mock_minio,
    mock_spark,
    mock_logger,
):
    mock_minio.return_value = MagicMock()

    mock_process.return_value = ([{"name": "table1", "status": "success"}], [])

    loader = mock_configloader.return_value

    loader.config = {"tenant": "tenant_demo", "dataset": "demo_dataset"}

    loader.get_full_config.return_value = {}
    loader.get_tenant.return_value = "tenant_demo"

    loader.get_namespace.return_value = "tenant_demo__dataset"
    loader.get_namespace_base_path.return_value = "s3a://bucket/silver/"

    loader.get_all_defaults.return_value = {"json": {"header": True}}
    loader.get_tables.return_value = [
        {
            "name": "table1",
            "format": "json",
            "schema_sql": "id STRING, name STRING",
            "bronze_path": "s3a://bucket/file.json",
            "linkml_schema": None,
            "silver_path": "s3a://bucket/silver/table1",
        }
    ]

    loader.get_bronze_path.return_value = "s3a://bucket/file.json"
    loader.get_silver_path.return_value = "s3a://bucket/silver/table1"

    result = ingest(config={}, spark=mock_spark, logger=mock_logger)

    assert result["success"] is True
    assert len(result["tables"]) == 1
    assert result["tables"][0]["status"] == "success"
