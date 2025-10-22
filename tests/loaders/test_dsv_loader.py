import pytest
from unittest.mock import MagicMock
from data_lakehouse_ingest.loaders.dsv_loader import load_csv_data, load_tsv_data

@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.read.options.return_value.format.return_value.load.return_value.count.return_value = 3
    return spark

@pytest.fixture
def mock_logger():
    return MagicMock()

def test_load_csv_data_success(mock_spark, mock_logger):
    df = load_csv_data(mock_spark, "s3a://bucket/data.csv", {"header": "true"}, mock_logger)
    assert df is not None
    mock_logger.info.assert_any_call("📂 Reading CSV data from: s3a://bucket/data.csv")

def test_load_tsv_data_success(mock_spark, mock_logger):
    df = load_tsv_data(mock_spark, "s3a://bucket/data.tsv", {"header": "true"}, mock_logger)
    assert df is not None
    mock_logger.info.assert_any_call("📂 Reading TSV data from: s3a://bucket/data.tsv")
