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


def test_write_table_creates_or_replaces():
    mock_logger = MagicMock()
    mock_df = MagicMock()
    mock_spark = MagicMock()
    mock_writer = MagicMock()
    mock_df.writeTo.return_value = mock_writer
    mock_spark.table.return_value.count.return_value = 42

    rows = write_table(
        df=mock_df,
        spark=mock_spark,
        namespace="kbase.ke_pangenome",
        name="genome",
        partition_by=None,
        mode="overwrite",
        logger=mock_logger,
    )

    mock_df.writeTo.assert_called_once_with("kbase.ke_pangenome.genome")
    mock_writer.createOrReplace.assert_called_once()
    mock_writer.append.assert_not_called()
    assert rows == 42

    info_calls = [str(call) for call in mock_logger.info.call_args_list]
    assert any("Wrote" in call for call in info_calls)


def test_write_table_append_mode():
    mock_logger = MagicMock()
    mock_df = MagicMock()
    mock_spark = MagicMock()
    mock_writer = MagicMock()
    mock_df.writeTo.return_value = mock_writer
    mock_spark.table.return_value.count.return_value = 10

    rows = write_table(
        df=mock_df,
        spark=mock_spark,
        namespace="my.dataset",
        name="events",
        partition_by=None,
        mode="append",
        logger=mock_logger,
    )

    mock_writer.append.assert_called_once()
    mock_writer.createOrReplace.assert_not_called()
    assert rows == 10


def test_write_table_with_partition_by():
    mock_logger = MagicMock()
    mock_df = MagicMock()
    mock_spark = MagicMock()
    mock_writer = MagicMock()
    mock_partitioned_writer = MagicMock()
    mock_df.writeTo.return_value = mock_writer
    mock_writer.partitionedBy.return_value = mock_partitioned_writer
    mock_spark.table.return_value.count.return_value = 5

    write_table(
        df=mock_df,
        spark=mock_spark,
        namespace="kbase.dataset",
        name="table1",
        partition_by="year",
        mode="overwrite",
        logger=mock_logger,
    )

    mock_writer.partitionedBy.assert_called_once_with("year")
    mock_partitioned_writer.createOrReplace.assert_called_once()


def test_write_table_with_partition_by_list():
    mock_logger = MagicMock()
    mock_df = MagicMock()
    mock_spark = MagicMock()
    mock_writer = MagicMock()
    mock_partitioned_writer = MagicMock()
    mock_df.writeTo.return_value = mock_writer
    mock_writer.partitionedBy.return_value = mock_partitioned_writer
    mock_spark.table.return_value.count.return_value = 5

    write_table(
        df=mock_df,
        spark=mock_spark,
        namespace="kbase.dataset",
        name="table1",
        partition_by=["year", "month"],
        mode="overwrite",
        logger=mock_logger,
    )

    mock_writer.partitionedBy.assert_called_once_with("year", "month")
    mock_partitioned_writer.createOrReplace.assert_called_once()
