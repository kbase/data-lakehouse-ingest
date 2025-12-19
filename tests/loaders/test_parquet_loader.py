import pytest
from unittest.mock import MagicMock

from data_lakehouse_ingest.loaders.parquet_loader import (
    load_parquet_data,
    load_parquet,
)


def test_load_parquet_data_success(mock_spark, mock_logger):
    mock_df = MagicMock()
    mock_df.count.return_value = 3

    mock_spark.read.options.return_value.parquet.return_value = mock_df

    path = "s3a://bucket/data.parquet"
    opts = {"mergeSchema": "true"}

    df = load_parquet_data(mock_spark, path, opts, mock_logger)

    assert df is mock_df

    mock_logger.info.assert_any_call(f"📂 Reading PARQUET data from: {path}")
    mock_logger.info.assert_any_call(f"✅ Loaded 3 PARQUET records from {path}")

    mock_spark.read.options.assert_called_once_with(**opts)
    mock_spark.read.options.return_value.parquet.assert_called_once_with(path)


def test_load_parquet_data_success_with_no_opts(mock_spark, mock_logger):
    mock_df = MagicMock()
    mock_df.count.return_value = 1

    mock_spark.read.options.return_value.parquet.return_value = mock_df

    path = "s3a://bucket/data.parquet"

    df = load_parquet_data(mock_spark, path, None, mock_logger)

    assert df is mock_df
    mock_spark.read.options.assert_called_once_with()
    mock_logger.info.assert_any_call(f"📂 Reading PARQUET data from: {path}")


def test_load_parquet_data_failure(mock_spark, mock_logger):
    mock_spark.read.options.return_value.parquet.side_effect = Exception("parquet read failed")

    path = "s3a://bucket/bad.parquet"

    with pytest.raises(Exception):
        load_parquet_data(mock_spark, path, {}, mock_logger)

    mock_logger.error.assert_called()
    assert "❌ Failed to load PARQUET" in mock_logger.error.call_args[0][0]


def test_load_parquet_wrapper_calls_load_parquet_data(mock_spark, mock_logger):
    mock_df = MagicMock()
    mock_df.count.return_value = 2
    mock_spark.read.options.return_value.parquet.return_value = mock_df

    path = "s3a://bucket/data.parquet"

    df = load_parquet(mock_spark, path, {}, mock_logger)

    assert df is mock_df
