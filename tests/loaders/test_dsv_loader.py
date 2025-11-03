from data_lakehouse_ingest.loaders.dsv_loader import load_csv_data, load_tsv_data

# TODO: Add more comprehensive tests to cover valid and invalid DSV data cases.
# For example:
#   - Test reading an actual CSV/TSV file with sample data.
#   - Test handling malformed files or missing columns.
#   - Test Spark read failures and exception handling.

def test_load_csv_data_success(mock_spark, mock_logger):
    df = load_csv_data(mock_spark, "s3a://bucket/data.csv", {"header": "true"}, mock_logger)
    assert df is not None
    mock_logger.info.assert_any_call("📂 Reading CSV data from: s3a://bucket/data.csv")

def test_load_tsv_data_success(mock_spark, mock_logger):
    df = load_tsv_data(mock_spark, "s3a://bucket/data.tsv", {"header": "true"}, mock_logger)
    assert df is not None
    mock_logger.info.assert_any_call("📂 Reading TSV data from: s3a://bucket/data.tsv")
