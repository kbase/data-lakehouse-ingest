from unittest.mock import MagicMock, patch
from data_lakehouse_ingest.orchestrator.io_utils import detect_format, load_table_data, write_to_delta

def test_detect_format_by_extension():
    assert detect_format("file.json", None) == "json"
    assert detect_format("file.tsv", None) == "tsv"
    assert detect_format("file.csv", None) == "csv"

@patch("data_lakehouse_ingest.orchestrator.io_utils.load_json_data")
def test_load_table_data_uses_correct_loader(mock_loader):
    mock_loader.return_value.count.return_value = 10
    mock_logger = MagicMock()
    mock_spark = MagicMock()
    df, rows = load_table_data(mock_spark, "s3://bucket/data.json", "json", {}, mock_logger)
    mock_loader.assert_called_once()

@patch("data_lakehouse_ingest.orchestrator.io_utils.SparkSession")
def test_write_to_delta_creates_table(mock_spark):
    mock_logger = MagicMock()
    mock_df = MagicMock()
    mock_df.write.format.return_value.mode.return_value = mock_df.write
    rows = write_to_delta(mock_df, mock_spark, "tenant", "t1", "personal_test_table", "/tmp/silver", None, "overwrite", mock_logger)

    # Verify info was logged with the expected keyword
    info_calls = [str(call) for call in mock_logger.info.call_args_list]
    assert any("Wrote" in call for call in info_calls)