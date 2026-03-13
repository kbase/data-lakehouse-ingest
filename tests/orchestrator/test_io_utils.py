import pytest
from unittest.mock import MagicMock, patch
from data_lakehouse_ingest.orchestrator.io_utils import (
    detect_format,
    load_table_data,
    write_to_delta,
)


@pytest.mark.parametrize(
    "filename,format_hint,expected",
    [
        ("file.json", None, "json"),
        ("file.tsv", None, "tsv"),
        ("file.csv", None, "csv"),
        ("file.xml", None, "xml"),
        ("file.unknown", None, "csv"),  # default fallback from detect_format()
        ("file", None, "csv"),  # no extension → fallback to csv
    ],
)
def test_detect_format_by_extension(filename, format_hint, expected):
    assert detect_format(filename, format_hint) == expected


def test_detect_format_explicit_override_lowercases():
    assert detect_format("file.csv", "JSON") == "json"


@patch("data_lakehouse_ingest.orchestrator.io_utils.load_json_data")
def test_load_table_data_uses_correct_loader(mock_loader):
    mock_df = MagicMock()
    mock_df.count.return_value = 10
    mock_loader.return_value = mock_df

    mock_logger = MagicMock()
    mock_spark = MagicMock()

    df, rows_in = load_table_data(
        mock_spark,
        "s3://bucket/data.json",
        "json",
        {},
        mock_logger,
    )

    mock_loader.assert_called_once_with(mock_spark, "s3://bucket/data.json", {}, mock_logger)
    assert df == mock_df
    assert rows_in == 10


def test_load_table_data_raises_for_unsupported_format():
    mock_logger = MagicMock()
    mock_spark = MagicMock()

    with pytest.raises(ValueError, match="Unsupported file format 'xlsx'"):
        load_table_data(
            mock_spark,
            "s3://bucket/data.xlsx",
            "xlsx",
            {},
            mock_logger,
        )


def test_write_to_delta_creates_table_overwrite():
    mock_logger = MagicMock()
    mock_df = MagicMock()
    mock_spark = MagicMock()

    mock_df.count.return_value = 25

    mock_writer = MagicMock()
    mock_df.write.format.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer
    mock_writer.option.return_value = mock_writer

    rows_written = write_to_delta(
        df=mock_df,
        spark=mock_spark,
        namespace="tenant",
        namespace_base_path="t1",
        name="personal_test_table",
        silver_path="/tmp/silver",
        partition_by=None,
        mode="overwrite",
        logger=mock_logger,
    )

    table_path = "t1/personal_test_table"

    mock_df.count.assert_called_once()
    mock_df.write.format.assert_called_once_with("delta")
    mock_writer.mode.assert_called_once_with("overwrite")
    mock_writer.option.assert_called_once_with("overwriteSchema", "true")
    mock_writer.save.assert_called_once_with(table_path)
    mock_spark.sql.assert_called_once()

    sql_arg = mock_spark.sql.call_args[0][0]
    assert "CREATE TABLE IF NOT EXISTS `tenant`.`personal_test_table`" in sql_arg
    assert f"LOCATION '{table_path}'" in sql_arg

    assert rows_written == 25


def test_write_to_delta_partitioned_append():
    mock_logger = MagicMock()
    mock_df = MagicMock()
    mock_spark = MagicMock()

    mock_df.count.return_value = 100

    mock_writer = MagicMock()
    mock_df.write.format.return_value = mock_writer
    mock_writer.mode.return_value = mock_writer
    mock_writer.partitionBy.return_value = mock_writer

    rows_written = write_to_delta(
        df=mock_df,
        spark=mock_spark,
        namespace="tenant",
        namespace_base_path="base_path",
        name="events",
        silver_path="/tmp/silver",
        partition_by=["load_date"],
        mode="append",
        logger=mock_logger,
    )

    table_path = "base_path/events"

    mock_df.write.format.assert_called_once_with("delta")
    mock_writer.mode.assert_called_once_with("append")
    mock_writer.partitionBy.assert_called_once_with(["load_date"])
    mock_writer.save.assert_called_once_with(table_path)

    mock_writer.option.assert_not_called()
    assert rows_written == 100


@patch("data_lakehouse_ingest.orchestrator.io_utils.SparkSession")
def test_write_to_delta_creates_table(mock_spark):
    mock_logger = MagicMock()
    mock_df = MagicMock()
    mock_df.write.format.return_value.mode.return_value = mock_df.write
    write_to_delta(
        mock_df,
        mock_spark,
        "tenant",
        "t1",
        "personal_test_table",
        "/tmp/silver",
        None,
        "overwrite",
        mock_logger,
    )

    # Verify info was logged with the expected keyword
    info_calls = [str(call) for call in mock_logger.info.call_args_list]
    assert any("Wrote" in call for call in info_calls)
