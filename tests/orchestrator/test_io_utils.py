import pytest
from unittest.mock import MagicMock, patch
from data_lakehouse_ingest.orchestrator.io_utils import (
    detect_format,
    load_table_data,
    write_table,
)


@pytest.mark.parametrize(
    "filename,format_hint,expected",
    [
        ("file.json", None, "json"),
        ("file.tsv", None, "tsv"),
        ("file.csv", None, "csv"),
        ("file.xml", None, "xml"),
        ("file.parquet", None, "parquet"),
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


def test_write_table_creates_or_replaces_table_overwrite():
    mock_logger = MagicMock()
    mock_df = MagicMock()
    mock_spark = MagicMock()

    mock_spark.table.side_effect = Exception("table does not exist")

    mock_writer = MagicMock()
    mock_df.writeTo.return_value = mock_writer

    rows_written = write_table(
        df=mock_df,
        spark=mock_spark,
        namespace="my.dataset",
        name="events",
        partition_by=None,
        mode="overwrite",
        rows_in=25,
        logger=mock_logger,
    )

    mock_df.writeTo.assert_called_once_with("my.dataset.events")
    mock_writer.createOrReplace.assert_called_once_with()
    mock_writer.append.assert_not_called()
    assert rows_written == 25


def test_write_table_partitioned_append_existing_table():
    mock_logger = MagicMock()
    mock_df = MagicMock()
    mock_spark = MagicMock()

    mock_spark.table.return_value.limit.return_value.count.return_value = 1

    mock_writer = MagicMock()
    mock_df.writeTo.return_value = mock_writer
    mock_writer.partitionedBy.return_value = mock_writer

    rows_written = write_table(
        df=mock_df,
        spark=mock_spark,
        namespace="my.dataset",
        name="events",
        partition_by=["load_date"],
        mode="append",
        rows_in=100,
        logger=mock_logger,
    )

    mock_df.writeTo.assert_called_once_with("my.dataset.events")
    mock_writer.partitionedBy.assert_called_once_with("load_date")
    mock_writer.append.assert_called_once_with()
    mock_writer.createOrReplace.assert_not_called()
    assert rows_written == 100


def test_write_table_append_missing_table_raises():
    mock_logger = MagicMock()
    mock_df = MagicMock()
    mock_spark = MagicMock()

    mock_spark.table.side_effect = Exception("table does not exist")

    with pytest.raises(ValueError, match="Cannot append to my.dataset.events"):
        write_table(
            df=mock_df,
            spark=mock_spark,
            namespace="my.dataset",
            name="events",
            partition_by=None,
            mode="append",
            rows_in=10,
            logger=mock_logger,
        )

    mock_df.writeTo.assert_not_called()


def test_write_table_invalid_mode_raises():
    with pytest.raises(ValueError, match="Unsupported write mode 'merge'"):
        write_table(
            df=MagicMock(),
            spark=MagicMock(),
            namespace="my.dataset",
            name="events",
            partition_by=None,
            mode="merge",
            rows_in=10,
            logger=MagicMock(),
        )