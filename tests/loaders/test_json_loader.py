import pytest
from unittest.mock import MagicMock
from data_lakehouse_ingest.loaders.json_loader import load_json_data

@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.read.option.return_value.options.return_value.json.return_value.count.return_value = 5
    return spark

@pytest.fixture
def mock_logger():
    return MagicMock()

def test_load_json_data_success(mock_spark, mock_logger):
    df = load_json_data(mock_spark, "s3a://bucket/sample.json", {}, mock_logger)
    assert df is not None
    mock_logger.info.assert_any_call("📂 Reading JSON data from: s3a://bucket/sample.json")

def test_load_json_data_failure(mock_spark, mock_logger):
    mock_spark.read.option.side_effect = Exception("bad json")
    with pytest.raises(Exception):
        load_json_data(mock_spark, "bad.json", {}, mock_logger)
    mock_logger.error.assert_called()
