import pytest
import logging
from unittest.mock import MagicMock, patch
from data_lakehouse_ingest.orchestrator.table_processor import process_table


@pytest.fixture
def mock_spark():
    spark = MagicMock()
    df = MagicMock()
    df.count.return_value = 10
    spark.read.format.return_value.load.return_value = df
    return spark


@pytest.fixture
def mock_logger():
    return MagicMock(spec=logging.Logger)


@pytest.fixture
def mock_loader():
    loader = MagicMock()
    loader.get_bronze_path.return_value = "s3a://bronze/test_table/"
    loader.get_silver_path.return_value = "s3a://silver/test_table/"
    loader.get_defaults_for.return_value = {"header": True, "delimiter": ",", "inferSchema": False}
    return loader


@pytest.fixture
def table_config():
    return {"name": "test_table", "format": "csv", "mode": "overwrite"}


# ---------------------------------------------------------------------
# Regular ingestion path
# ---------------------------------------------------------------------
@patch("data_lakehouse_ingest.orchestrator.table_processor.resolve_schema", return_value=("CREATE TABLE ...", "linkml"))
@patch("data_lakehouse_ingest.orchestrator.table_processor.detect_format", return_value="csv")
@patch("data_lakehouse_ingest.orchestrator.table_processor.load_table_data", return_value=(MagicMock(), 100))
@patch("data_lakehouse_ingest.orchestrator.table_processor.apply_schema_columns", side_effect=lambda df, **_: df)
@patch("data_lakehouse_ingest.orchestrator.table_processor.write_to_delta", return_value=95)
def test_process_table_success(
    mock_write_to_delta,
    mock_apply_schema,
    mock_load_data,
    mock_detect_format,
    mock_resolve_schema,
    mock_spark,
    mock_logger,
    mock_loader,
    table_config,
):
    ctx = {"tenant": "tenant_alpha", "namespace": "tenant_alpha__dataset", "namespace_base_path": "s3a://silver/"}
    result = process_table(
        spark=mock_spark,
        logger=mock_logger,
        loader=mock_loader,
        ctx=ctx,
        table=table_config,
        run_started_at_iso="2025-10-31T12:00:00Z",
    )


    # Assertions
    assert result["status"] == "success"
    assert result["rows_in"] == 100
    assert result["rows_written"] == 95
    assert "elapsed_sec" in result
    mock_detect_format.assert_called_once()
    mock_resolve_schema.assert_called_once()
    mock_load_data.assert_called_once()
    mock_apply_schema.assert_called_once()
    mock_write_to_delta.assert_called_once()
    mock_logger.info.assert_any_call("Processing table: test_table")


# ---------------------------------------------------------------------
# Data load failure path
# ---------------------------------------------------------------------
@patch("data_lakehouse_ingest.orchestrator.table_processor.detect_format", return_value="csv")
@patch("data_lakehouse_ingest.orchestrator.table_processor.load_table_data", side_effect=Exception("Simulated load failure"))
def test_process_table_data_load_failure(mock_load, mock_detect_format, mock_spark, mock_logger, mock_loader, table_config):
    ctx = {"tenant": "tenant_alpha", "namespace": "tenant_alpha__dataset", "namespace_base_path": "s3a://silver/"}
    result = process_table(
        spark=mock_spark,
        logger=mock_logger,
        loader=mock_loader,
        ctx=ctx,
        table=table_config,
        run_started_at_iso="2025-10-31T12:00:00Z",
    )

    assert result["status"] == "failed"
    assert result["phase"] == "data_loading"
    assert "Simulated load failure" in result["error"]
    mock_logger.error.assert_called_once()

