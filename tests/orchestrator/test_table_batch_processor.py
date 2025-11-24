import pytest
from unittest.mock import MagicMock, patch

from data_lakehouse_ingest.orchestrator.table_batch_processor import process_tables


def test_process_tables_success():
    """
    Test that process_tables correctly iterates over tables,
    delegates to process_table(), and aggregates reports.
    """

    # --- Mocks ---
    spark = MagicMock()
    logger = MagicMock()
    loader = MagicMock()
    minio_client = MagicMock()

    ctx = {
        "tables": [
            {"name": "table1"},
            {"name": "table2"}
        ]
    }

    started_at = "2025-01-01T00:00:00Z"

    # Mock process_table to return different rows for each call
    mock_report_1 = {"status": "success", "table": "table1"}
    mock_report_2 = {"status": "success", "table": "table2"}

    with patch(
        "data_lakehouse_ingest.orchestrator.table_batch_processor.process_table",
        side_effect=[mock_report_1, mock_report_2]
    ) as mock_process_table:

        table_reports, error_list = process_tables(
            spark=spark,
            logger=logger,
            loader=loader,
            ctx=ctx,
            started_at=started_at,
            minio_client=minio_client,
        )

    # --- Assertions ---
    assert table_reports == [mock_report_1, mock_report_2]
    assert error_list == []

    # Ensure process_table was called twice (once per table)
    assert mock_process_table.call_count == 2

    # Ensure logger context was set per table if logger.context_filter exists
    if hasattr(logger, "context_filter"):
        logger.context_filter.set_table.assert_any_call("table1")
        logger.context_filter.set_table.assert_any_call("table2")

    # Ensure logger was called with the table processing log
    logger.info.assert_any_call("Processing table: table1")
    logger.info.assert_any_call("Processing table: table2")


def test_process_tables_with_exception():
    """
    Test that when process_table raises an exception, the error entry is created
    and appended to both table_reports and error_list.
    """

    spark = MagicMock()
    logger = MagicMock()
    loader = MagicMock()
    minio_client = MagicMock()

    ctx = {"tables": [{"name": "boom_table"}]}
    started_at = "2025-01-01T00:00:00Z"

    # Mock process_table to raise an exception
    with patch(
        "data_lakehouse_ingest.orchestrator.table_batch_processor.process_table",
        side_effect=Exception("Test error")
    ):
        with patch(
            "data_lakehouse_ingest.orchestrator.table_batch_processor.error_entry_for_exception",
            return_value={"status": "error", "table": "boom_table", "error": "Test error"}
        ) as mock_error_entry:

            table_reports, error_list = process_tables(
                spark=spark,
                logger=logger,
                loader=loader,
                ctx=ctx,
                started_at=started_at,
                minio_client=minio_client,
            )

    # --- Assertions ---
    assert len(table_reports) == 1
    assert len(error_list) == 1

    assert table_reports[0] == {
        "status": "error",
        "table": "boom_table",
        "error": "Test error"
    }

    assert error_list[0] == table_reports[0]

    # Ensure error_entry_for_exception was used
    mock_error_entry.assert_called_once()

    # Verify that ProcessTable attempted to log
    logger.info.assert_any_call("Processing table: boom_table")
